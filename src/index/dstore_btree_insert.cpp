/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 *
 * ---------------------------------------------------------------------------------------
 *
 * dstore_btree_insert.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/index/dstore_btree_insert.cpp
 *
 * ---------------------------------------------------------------------------------------*/

#include "index/dstore_btree_insert.h"
#include "catalog/dstore_function.h"
#include "index/dstore_btree_prune.h"
#include "index/dstore_btree_wal.h"
#include "index/dstore_btree_perf_unit.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_mgr.h"
#include "catalog/dstore_typecache.h"
#include "fault_injection/fault_injection.h"

namespace DSTORE {

BtreeInsert::BtreeInsert(StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scanKey, bool needDefferCheck)
    : BtreeSplit(indexRel, indexInfo, scanKey, true), m_needDefferCheck(needDefferCheck)
{
    Init();
    /* Do not support deffer constraint checking for now. Remove when supported */
    StorageAssert(!m_needDefferCheck);
}

BtreeInsert::~BtreeInsert()
{
    m_insertPageBuf = INVALID_BUFFER_DESC;
    m_insertTuple = nullptr;
}

void BtreeInsert::Clear()
{
    ClearStack();
    if (m_insertPageBuf != INVALID_BUFFER_DESC) {
        m_bufMgr->UnlockAndRelease(m_insertPageBuf);
        m_insertPageBuf = INVALID_BUFFER_DESC;
    }
    if (m_insertTuple != nullptr) {
        DstorePfree(m_insertTuple);
        m_insertTuple = nullptr;
    }
    DstorePfreeExt(m_duplicateTuple);
}

RetStatus BtreeInsert::InsertTuple(Datum *values, bool *isnull, ItemPointer heapCtid, bool *satisfiesUnique,
                                   CommandId cid)
{
    RetStatus ret = DSTORE_FAIL;
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"ActiveTransaction\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(transaction->AllocTransactionSlot())) {
        return ret;
    }
    m_cid = (cid == INVALID_CID) ? transaction->GetCurCid() : cid;

    StoragePdb *pdb = g_storageInstance->GetPdb(this->GetPdbId());
    if (STORAGE_VAR_NULL(pdb)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"StoragePdb\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    TransactionMgr *transactionMgr = pdb->GetTransactionMgr();
    if (STORAGE_VAR_NULL(transactionMgr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"TransactionMgr\".", __FUNCTION__));
        return DSTORE_FAIL;
    }

    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreeInsertTupleLatency);
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    /* Step 1. Form an Index Tuple */
    if (likely(m_insertTuple == nullptr) &&
        STORAGE_FUNC_FAIL(FormIndexTuple(values, isnull, heapCtid, &m_insertTuple))) {
        return ret;
    }

    /* Step 2. Init Scankeys for searching Insert location */
    StorageClearError();
    InitSearchingKeys();

    /* Step 3. Do searching and checking for Inserting page in a loop.
     *         We may need to retry this step for several times if concurrent transaction is in progress */
    m_retryCount = 0;
RETRY_INSERTION:
    if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Index insert of heapCtid(%hu, %u, %hu) is canceled by user.",
                   heapCtid->GetFileId(), heapCtid->GetBlockNum(), heapCtid->GetOffset()));
        goto END;
    }
    m_isBoundValid = false;
    if (m_needCheckUnique) {
        /* Clear heapCtid to find the first key with the same key values */
        m_scanKeyValues.heapCtid = INVALID_ITEM_POINTER;
    }
    /* Search the Btree from the root and get the inserting leaf */
    if (STORAGE_FUNC_FAIL(SearchBtreeForInsert()) || m_insertPageBuf == INVALID_BUFFER_DESC) {
        goto END;
    }
    UpdateCachedInsertionPageIdIfNeeded(m_insertPageBuf->GetPageId());
    m_needRetrySearchBtree = false;

    /* Check if there's any conflict for this insertion */
    if (STORAGE_FUNC_FAIL(ret = CheckConflict())) {
        goto END;
    }
    if (m_needRetrySearchBtree) {
        StorageAssert(m_insertPageBuf == INVALID_BUFFER_DESC);
        goto RETRY_INSERTION;
    }
    if (satisfiesUnique != nullptr) {
        *satisfiesUnique = m_satisfiedUnique;
    }

    /* Step 4. Find the inserting offset on the page */
    if (unlikely(!m_readyForInsert) || STORAGE_FUNC_FAIL(FindInsertLoc())) {
        /* Error code has been set along with m_readyForInsert flag setting, just return here. */
        ret = DSTORE_FAIL;
        goto END;
    }

    /* Step 5. Write the inserting Index Tuple onto the page */
    if (STORAGE_FUNC_FAIL(ret = BtreeInsert::AddTupleToLeaf())) {
        FAULT_INJECTION_ACTION(DstoreIndexFI::STOP_RETRY_SEARCH_BTREE, m_needRetrySearchBtree = false);
        if (m_needRetrySearchBtree) {
            goto RETRY_INSERTION;
        }
    }

END:
    Clear();
    return ret;
}

void BtreeInsert::InitSearchingKeys()
{
    /* Update scankey values using insertion index tuple to help search for insertion location. */
    bool hasNull = UpdateScanKeyWithValues(m_insertTuple);
    /* Step 2. Check unique if requested */
    if (m_needCheckUnique) {
        if (hasNull) {
            /* NULL unequal to any value, including NULL. */
            m_needCheckUnique = false;
            m_satisfiedUnique = true;
        }
    }
}

RetStatus BtreeInsert::CheckConflict()
{
    if (!m_needCheckUnique) {
        m_readyForInsert = true;
        return DSTORE_SUCC;
    }

    m_readyForInsert = false;
    Xid waitXid = INVALID_XID;
    if (STORAGE_FUNC_FAIL(BtreeInsert::CheckUnique(&waitXid))) {
        /* Something wrong when checking... */
        return DSTORE_FAIL;
    }
    if (unlikely(waitXid != INVALID_XID)) {
        /* Have to unlock curren page to wait for other sessions then recheck page */
        PageId pageId = m_insertPageBuf->GetPageId();
        m_bufMgr->UnlockAndRelease(m_insertPageBuf);
        m_insertPageBuf = INVALID_BUFFER_DESC;
        if (STORAGE_FUNC_FAIL(WaitForTxnEndIfNeeded(pageId, waitXid))) {
            return DSTORE_FAIL;
        }
        m_needRetrySearchBtree = true;
        return DSTORE_SUCC;
    }

    m_readyForInsert = m_satisfiedUnique;
    if (unlikely(!m_satisfiedUnique)) {
        if (m_needDefferCheck) {
            m_readyForInsert = true;
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                ErrMsg("Insert a potential duplicate and will check it when commit, heapCtid(%hu, %u, %hu)",
                m_insertTuple->GetHeapCtid().GetFileId(), m_insertTuple->GetHeapCtid().GetBlockNum(),
                m_insertTuple->GetHeapCtid().GetOffset()));
        } else {
            ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("Duplicate found when unique check, heapCtid(%hu, %u, %hu)",
                m_insertTuple->GetHeapCtid().GetFileId(), m_insertTuple->GetHeapCtid().GetBlockNum(),
                m_insertTuple->GetHeapCtid().GetOffset()));
        }
    }

    m_scanKeyValues.heapCtid = m_insertTuple->GetHeapCtid();
    return DSTORE_SUCC;
}

RetStatus BtreeInsert::SearchBtreeForInsert()
{
    StorageAssert(m_insertPageBuf == INVALID_BUFFER_DESC);
    StorageAssert(!m_isBoundValid);

    /* Check if cached inserting page of last insertion is fit for current insertion */
    PageId lastInsertPageId = GetBtreeSmgr()->GetLastInsertionPageId();
    if (m_useInsertPageCache && lastInsertPageId.IsValid()) {
        LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreeSearchHitCacheForInsertLatency);
        /* last insert page may be reused as BTR_QUEUE_PAGE_TYPE, cannot to check page type here */
        m_insertPageBuf = Btree::ReadAndCheckBtrPage(lastInsertPageId, LW_EXCLUSIVE, m_bufMgr, this->GetPdbId(), false);
        if (likely(m_insertPageBuf != nullptr)) {
            BtrPage *lastInsertPage = static_cast<BtrPage *>(m_insertPageBuf->GetPage());
            if (likely(lastInsertPage->TestType(PageType::INDEX_PAGE_TYPE))) {
                if (unlikely(lastInsertPage->IsDamaged())) {
                    storage_set_error(INDEX_ERROR_FAIL_SEARCH_INSERT_FOR_PAGE_DAMAGED, lastInsertPageId.m_fileId,
                        lastInsertPageId.m_blockId);
                    ErrLog(DSTORE_ERROR, MODULE_INDEX,
                        ErrMsg("btrPage(%d, %u) is damaged in(%s), segment(%hu, %u) " BTR_PAGE_HEADER_FMT,
                        lastInsertPageId.m_fileId, lastInsertPageId.m_blockId, m_indexInfo->indexRelName,
                        GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                        BTR_PAGE_HEADER_VAL(lastInsertPage)));
                    m_bufMgr->UnlockAndRelease(m_insertPageBuf, BufferPoolUnlockContentFlag ::DontCheckCrc());
                    m_insertPageBuf = INVALID_BUFFER_DESC;
                    return DSTORE_FAIL;
                }
                StorageAssert(lastInsertPage->CheckSanity());
                BtrPageLinkAndStatus *lastInsertLinkStat = lastInsertPage->GetLinkAndStatus();
                bool isInternalPage = lastInsertLinkStat->TestType(BtrPageType::INTERNAL_PAGE);
                bool isMached =
                    !lastInsertLinkStat->IsUnlinked() && lastInsertLinkStat->TestType(BtrPageType::LEAF_PAGE) &&
                    lastInsertLinkStat->IsRightmost() && lastInsertPage->GetMaxOffset() >= BTREE_PAGE_HIKEY &&
                    lastInsertLinkStat->IsSplitComplete() &&
                    lastInsertPage->GetFreeSpaceForInsert() > (m_insertTuple->GetSize() + sizeof(TD)) &&
                    CompareKeyToTuple(lastInsertPage, lastInsertLinkStat, lastInsertLinkStat->GetFirstDataOffset(),
                                    isInternalPage) > 0;
                if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
                    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                        ErrMsg("Failed to compare tuple since compare function returns null."));
                    return DSTORE_FAIL;
                }
                if (isMached) {
                    /* Split is not supported for fastpath searching since m_leafStack is empty. */
                    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                        ErrMsg("Found cached page {%d, %u} of last insertion", lastInsertPageId.m_fileId,
                        lastInsertPageId.m_blockId));
                    return DSTORE_SUCC;
                }
            }
            /* Cached inserting page is unsuitable for current insertion. */
            m_bufMgr->UnlockAndRelease(m_insertPageBuf, BufferPoolUnlockContentFlag ::DontCheckCrc());
            m_insertPageBuf = INVALID_BUFFER_DESC;
            /* Also clear cache */
            GetBtreeSmgr()->SetLastInsertionPageId(INVALID_PAGE_ID);
        }
    }

    return BtreeSplit::SearchBtree(&m_insertPageBuf, false);
}

RetStatus BtreeInsert::FindInsertLoc()
{
    StorageAssert(m_insertPageBuf != INVALID_BUFFER_DESC);

    /* Step 1. Check index tuple size */
    BtrPage *insertPage = static_cast<BtrPage *>(m_insertPageBuf->GetPage());

    /* Step 1. Some extra searching process for CheckUnique case */
    /* For CheckUnique case, we'd deleted heapCtid for SearchBtree in order to find the first tuple having the same key
     * with insertion tuple. Now we need to re-search using a scankey containing the heapCtid to find the exactly
     * insert location. */
    if (m_needCheckUnique) {
        BufferDesc *rightBuf = INVALID_BUFFER_DESC;
        BufferDesc *insertBuf = INVALID_BUFFER_DESC;
        for (;;) {
            if (STORAGE_FUNC_FAIL(StepRightWhenCheckUnique(&rightBuf, LW_EXCLUSIVE))) {
                return DSTORE_FAIL;
            }
            if (rightBuf == INVALID_BUFFER_DESC) {
                break;
            }
            insertBuf = rightBuf;
        }
        /* Must unlock and release write-lock on current page after get write-lock of right target, otherwise
         * someone else's CheckUnique scan could fail to see our insertion. */
        if (insertBuf != INVALID_BUFFER_DESC) {
            m_bufMgr->UnlockAndRelease(m_insertPageBuf);
            m_insertPageBuf = insertBuf;
            insertPage = static_cast<BtrPage *>(m_insertPageBuf->GetPage());
            m_isBoundValid = false;
        }
    }
    /* check page */
    StorageAssert(insertPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
    StorageAssert(insertPage->GetLinkAndStatus()->IsSplitComplete());
    StorageAssert(insertPage->IsRightmost() ||
                  CompareKeyToTuple(insertPage, insertPage->GetLinkAndStatus(), BTREE_PAGE_HIKEY,
                                    insertPage->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE)) <= 0);

    uint32 freeSpaceOnPage = insertPage->GetFreeSpaceForInsert();
    /* Step 2. Prune page if free space is insufficient or page has dead tuples and is getting full */
    /* Never prune in-building ccindex */
    bool needPrunePage = (m_indexInfo->btrIdxStatus == BtrCcidxStatus::NOT_CCINDEX);
    bool spaceInsufficient = (freeSpaceOnPage < m_insertTuple->GetSize() + sizeof(ItemId));
    bool proactivePrune = (insertPage->HasPrunableTuple() && freeSpaceOnPage < BLCKSZ / 4);
    if (needPrunePage && (spaceInsufficient || proactivePrune)) {
        LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreePagePruneLatency, false);
        timer.Start();
        /* Try prune page to get more space and to avoid splitting */
        BtreePagePrune prunePage(m_indexRel, m_indexInfo, m_scanKeyValues.scankeys, m_insertPageBuf);
        if (STORAGE_FUNC_FAIL(prunePage.Prune())) {
            return DSTORE_FAIL;
        }
        if (prunePage.IsPagePrunable()) {
            /* The arrange of keys may be changed after pruning. We need to set insertstate->bounds_valid to false
             * in order to renew the upper and lower boundary of binary search and to trigger a round of new search
             * among the entire page */
            m_isBoundValid = false;
            m_prunedPage = true;
        }
        timer.End();
    }

    m_insertOff = BinarySearchOnLeaf(insertPage);
    if (StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("Failed to binary search btrPage(%hu, %u) since compare function returns null.",
                      m_insertPageBuf->GetPageId().m_fileId, m_insertPageBuf->GetPageId().m_blockId));
        /* No need to release m_insertPageBuf. Clear() would do. */
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Found insert off %d on page {%d, %u} for tuple with heap ctid {%d, %u} %d", m_insertOff,
                  m_insertPageBuf->GetPageId().m_fileId, m_insertPageBuf->GetPageId().m_blockId,
                  m_insertTuple->GetHeapCtid().GetFileId(), m_insertTuple->GetHeapCtid().GetBlockNum(),
                  m_insertTuple->GetHeapCtid().GetOffset()));

    return DSTORE_SUCC;
}

OffsetNumber BtreeInsert::BinarySearchOnLeaf(BtrPage *btrPage)
{
    BtrPageLinkAndStatus *linkAndStatus = btrPage->GetLinkAndStatus();
    StorageAssert(linkAndStatus->TestType(BtrPageType::LEAF_PAGE));

    uint16 low = linkAndStatus->GetFirstDataOffset();
    uint16 high = btrPage->GetMaxOffset();
    uint16 maxInsertOff = OffsetNumberNext(high);
    if (unlikely(high < low)) {
        /* No keys on the page since the page is really empty, just return the first available slot. */
        return low;
    }

    if (m_isBoundValid) {
        /* We've searched for a rough range for inserting location during unique check.
         * Continue to search within the range. */
        low = m_boundLow;
        high = m_boundStrictHigh;
    } else {
        /* Need a strictly higher bound for binary search. */
        high += 1;
        /* Need to initialize bound values */
        m_boundLow = low;
        m_boundStrictHigh = high;
    }

    /* Binary search to find the first key >= scan key */
    /* Search for tuples with key values >= scan key values */
    int goRightVal = 1;
    int32 result;
    while (high > low) {
        uint16 mid = low + (static_cast<uint16>(high - low) >> 1);
        result = CompareKeyToTuple(btrPage, linkAndStatus, mid, false);
        if (result >= goRightVal) {
            low = mid + 1;
        } else {
            high = mid;
            /* Update cached strictly higher bound */
            m_boundStrictHigh = (result != 0) ? high : m_boundStrictHigh;
        }
    }

    /* Check correctness for searching result */
    StorageAssert(low <= maxInsertOff);
    StorageAssert(low == maxInsertOff || CompareKeyToTuple(btrPage, linkAndStatus, low, false) <= goRightVal);
    UNUSED_VARIABLE(maxInsertOff);
    /* Update cached bound */
    m_isBoundValid = true;
    m_boundLow = low;
    /* We're searching on a leaf page, so always returns the first key >= scan key. */
    return low;
}

/* targetBuf will be unlocked and released after calling, no matter AddTupleToLeaf succeeds or not */
RetStatus BtreeInsert::AddTupleToLeaf()
{
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    if (STORAGE_VAR_NULL(walContext)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Wal writer context is null in current context."));
        return DSTORE_FAIL;
    }
    UndoRecord *undoRec = nullptr;
    WalRecord *walRec = nullptr;
    uint8 tdId = INVALID_TD_SLOT;
    RetStatus ret = DSTORE_FAIL;

    PageId insPageId = m_insertPageBuf->GetPageId();
    BtrPage *insPage = static_cast<BtrPage *>(m_insertPageBuf->GetPage());
    StorageAssert(insPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));

    /* Step 1. Split page if full */
    if (m_needRecordUndo) {
        tdId = AllocAndSetTd(insPage, m_insertTuple);
        FAULT_INJECTION_ACTION(DstoreTransactionFI::ALLOC_TD_FAIL, tdId = INVALID_TD_SLOT);
    }
    bool needSplit = (m_needRecordUndo && tdId == INVALID_TD_SLOT) ||
                     insPage->GetFreeSpaceForInsert() < m_insertTuple->GetSize();
    FAULT_INJECTION_ACTION(DstoreIndexFI::FORCE_SPLIT, needSplit = true);
    if (needSplit && !m_prunedPage &&
        m_indexInfo->btrIdxStatus == BtrCcidxStatus::NOT_CCINDEX &&
        insPage->HasPrunableTuple()) {
        /* Last resort before split: page has dead tuples that weren't cleaned yet (e.g. prune wasn't triggered
         * in FindInsertLoc because space seemed sufficient, but TD alloc failed). Try prune now. */
        BtreePagePrune prunePage(m_indexRel, m_indexInfo, m_scanKeyValues.scankeys, m_insertPageBuf);
        if (STORAGE_FUNC_SUCC(prunePage.Prune()) && prunePage.IsPagePrunable()) {
            m_prunedPage = true;
            m_isBoundValid = false;
            m_insertOff = BinarySearchOnLeaf(insPage);
            /* Re-evaluate need for split after prune */
            if (m_needRecordUndo && tdId == INVALID_TD_SLOT) {
                tdId = AllocAndSetTd(insPage, m_insertTuple);
            }
            needSplit = (m_needRecordUndo && tdId == INVALID_TD_SLOT) ||
                        insPage->GetFreeSpaceForInsert() < m_insertTuple->GetSize();
        }
    }
    if (needSplit) {
        InitSplittingTarget(m_insertPageBuf, !m_prunedPage);
        /* Split page and add new indexTuple on either target page or new right page. */
        ret = BtreeSplit::SplitAndAddDownlink(m_insertTuple, m_insertOff, m_leafStack);
        /* In any case, m_insertPageBuf should have been unlocked and released after AddTupleToLeaf. */
        m_insertPageBuf = INVALID_BUFFER_DESC;
        return ret;
    }

    /* Step 2 Add indexTuple to current page */
    bool allocTdDirty = m_tdContext.allocTd.isDirty;
    OffsetNumber offset = insPage->AddTuple(m_insertTuple, m_insertOff, tdId);
    if (unlikely(offset != m_insertOff)) {
        if (likely(m_insertOff <= insPage->GetMaxOffset())) {
            storage_set_error(INDEX_ERROR_ADD_ITEM_FAIL);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to add tuple to btrPage(%hu, %u), insertoff:%hu, "
                "retoff:%hu, relid:%u, tdId:%hhu " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, insPageId.m_fileId,
                insPageId.m_blockId, m_insertOff, offset, m_indexInfo->indexRelId, tdId, BTR_PAGE_HEADER_VAL(insPage),
                BTR_PAGE_LINK_AND_STATUS_VAL(insPage->GetLinkAndStatus())));
        } else {
            storage_set_error(INDEX_ERROR_LOW_IS_GREATER_THAN_MAX_OFFSET, insPageId.m_fileId, insPageId.m_blockId);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to add tuple to btrPage{%hu, %u}, insertoff:%hu, "
                "retoff:%hu, relid:%u, tdId:%hhu, boundValid:%hhu, boundlow:%hu, boundhigh:%hu " BTR_PAGE_HEADER_FMT
                BTR_PAGE_LINK_AND_STATUS_FMT, insPageId.m_fileId, insPageId.m_blockId, m_insertOff, offset,
                m_indexInfo->indexRelId, tdId, m_isBoundValid, m_boundLow, m_boundStrictHigh,
                BTR_PAGE_HEADER_VAL(insPage), BTR_PAGE_LINK_AND_STATUS_VAL(insPage->GetLinkAndStatus())));
        }
        return DSTORE_FAIL;
    }

    /* The target page is changing now. Must restore page before return if hits any failure later. */

    /* Step 3. Generate Undo record and Wal record */
    /* Step 3.1. Generate Wal record (but do not insert now) */
    if (likely(NeedWal()) && unlikely((walRec = GenerateLeafInsertWal()) == nullptr)) {
        goto RELEASE_AND_RETURN_FAIL;
    }
    /* Step 3.2. Generate Undo record (but do not insert now) */
    if (m_needRecordUndo &&
        STORAGE_FUNC_FAIL(ret = GenerateUndoRecordForInsert(m_insertTuple, m_insertOff, insPage, &undoRec))) {
        goto RELEASE_AND_RETURN_FAIL;
    }

    /* Step 4. Insert Undo record and Wal record */
    ret = m_bufMgr->MarkDirty(m_insertPageBuf);
    if (STORAGE_FUNC_FAIL(ret)) {
        /* Do not cover the error code */
        goto RELEASE_AND_RETURN_FAIL;
    }
    walContext->BeginAtomicWal(thrd->GetCurrentXid());
    if (m_needRecordUndo && STORAGE_FUNC_FAIL(ret = InsertUndoRecAndSetTd(tdId, m_insertOff, insPage, undoRec))) {
        walContext->ResetForAbort();
        /* Note that InsertUndoRecAndSetTd would only fail for buffer descriptor reading failure in this case.
         * It is garanteed that if InsertUndoRecAndSetTd fails, our undoRec would not be on Undo Page (neither disk
         * nor memory). Thus it's safe to rollback the insertion here because Transaction will not do so
         * when aborting (for no undo record inserted) */
        goto RELEASE_AND_RETURN_FAIL;
    }
    if (likely(NeedWal())) {
        /* Insert wal record */
        /* In step 3.1, we generated wal record according to the "OLD" page without UndoRecPtr (because we just
         * got the UndoRecPtr after InsertUndoRecAndSetTd succeeded).
         * Now we must update the wal record to fill UndoRecPtr in */
        UndoRecPtr undoRecPtr = m_needRecordUndo ? insPage->GetTd(tdId)->GetUndoRecPtr() : INVALID_UNDO_RECORD_PTR;
        static_cast<WalRecordBtreeInsertOnLeaf *>(walRec)->SetUndoRecPtr(undoRecPtr);
        walContext->RememberPageNeedWal(m_insertPageBuf);
        walContext->PutNewWalRecord(walRec);
    }
    UNUSED_VARIABLE(walContext->EndAtomicWal());

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("Finish insert indexTuple(%hu, %u, %hu) td(%hhu), heapCtid(%hu, %u, %hu), snapshot csn %lu",
        insPageId.m_fileId, insPageId.m_blockId, m_insertOff, tdId,
        m_insertTuple->GetHeapCtid().GetFileId(), m_insertTuple->GetHeapCtid().GetBlockNum(),
        m_insertTuple->GetHeapCtid().GetOffset(), thrd->GetSnapShotCsn()));

END:
    m_curPageId = m_insertPageBuf->GetPageId();
    delete undoRec;
    DstorePfreeExt(walRec);
    m_bufMgr->UnlockAndRelease(m_insertPageBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
    m_insertPageBuf = INVALID_BUFFER_DESC;
    return ret;

RELEASE_AND_RETURN_FAIL:
    HandleErrorAndRestorePage(undoRec);
    /* If insert index tuple fail, alloctd maybe has modified page we can't rollback, so we need record allcotd wal. */
    if (allocTdDirty) {
        /* isDirty flag may has been clear when call GenerateLeafInsertWal success */
        m_tdContext.allocTd.isDirty = true;
        (void)m_bufMgr->MarkDirty(m_insertPageBuf);
        GenerateAllocTdWal(m_insertPageBuf, m_tdContext);
    }
    if (unlikely(m_tdContext.allocTd.hasRollbackTd)) {
        (void)m_bufMgr->MarkDirty(m_insertPageBuf);
        GenerateRollbackTdWal(m_insertPageBuf, m_tdContext);
    }
    goto END;
}

RetStatus BtreeInsert::CheckGPIPartVisible(Oid checkTuplePartOid, bool *isVisible)
{
    if (STORAGE_VAR_NULL(isVisible)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr in \"isVisible\"", __FUNCTION__));
        return DSTORE_FAIL;
    }
    CacheHashManager *chManager = g_storageInstance->GetCacheHashMgr();
    if (STORAGE_VAR_NULL(chManager)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr in storage instance \"CacheHashManager\"", __FUNCTION__));
        return DSTORE_FAIL;
    }
    IndexCommonCb commonCb = chManager->GetIndexCommonCb();
    PGFunction func = chManager->GPICheckPartVisible();
    if (unlikely(func == nullptr || commonCb == nullptr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr in CacheHashManager \"GetIndexCommonCb\" or \"GPICheckPartVisible\"",
                      __FUNCTION__));
        return DSTORE_FAIL;
    }
    FunctionCallInfoData fcInfo;
    fcInfo.prealloc_arg[0] = ObjectIdGetDatum(checkTuplePartOid);
    Datum result = 0;

    if (STORAGE_FUNC_FAIL(commonCb(func, &fcInfo, &result))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] commonCb fail", __FUNCTION__));
        return DSTORE_FAIL;
    }

    *isVisible = DatumGetBool(result);
    return DSTORE_SUCC;
}

RetStatus BtreeInsert::CheckUnique(Xid *waitXid)
{
    StorageAssert(m_scanKeyValues.heapCtid == INVALID_ITEM_POINTER);
    StorageAssert(m_insertPageBuf != INVALID_BUFFER_DESC);
    StorageAssert(!m_isBoundValid);

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Start CheckUnique for insertion, on page {%d, %u} tuple heap "
        "ctid {%d, %u} %d", m_insertPageBuf->GetPageId().m_fileId, m_insertPageBuf->GetPageId().m_blockId,
        m_insertTuple->GetHeapCtid().GetFileId(), m_insertTuple->GetHeapCtid().GetBlockNum(),
        m_insertTuple->GetHeapCtid().GetOffset()));

    /* init assumption */
    m_satisfiedUnique = true;
    RetStatus ret = DSTORE_SUCC;

    /* Step 1. Search for the first key >= scan key on page */
    auto *btrPage = static_cast<BtrPage *>(m_insertPageBuf->GetPage());
    OffsetNumber maxOff = btrPage->GetMaxOffset();
    OffsetNumber checkOff = BinarySearchOnLeaf(btrPage);
    if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("Failed to binary search btrPage(%d, %u) since compare function returns null.",
                      m_insertPageBuf->GetPageId().m_fileId, m_insertPageBuf->GetPageId().m_blockId));
        return DSTORE_FAIL;
    }
    if (checkOff <= maxOff && checkOff == m_boundStrictHigh) {
        /* Nothing equal to scankey, just return */
        StorageAssert(m_isBoundValid);
        StorageAssert(CompareKeyToTuple(btrPage, btrPage->GetLinkAndStatus(), checkOff,
                                        btrPage->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE)) < 0);
        return DSTORE_SUCC;
    }

    /* Step 2. Check all equal tuple for uniqueness */
    BufferDesc *rightBuf = INVALID_BUFFER_DESC;
    while (m_insertPageBuf != INVALID_BUFFER_DESC) {
        /* Step 2.1. There may be more equal tuples on right page. Try step right to check them. */
        while (checkOff > maxOff) {
            if (STORAGE_FUNC_FAIL(StepRightWhenCheckUnique(&rightBuf, LW_SHARED))) {
                return DSTORE_FAIL;
            }
            if (rightBuf == INVALID_BUFFER_DESC) {
                /* No more tuple to compare */
                return DSTORE_SUCC;
            }
            /* Update current checking page info */
            btrPage = static_cast<BtrPage *>(rightBuf->GetPage());
            maxOff = btrPage->GetMaxOffset();
            checkOff = btrPage->GetLinkAndStatus()->GetFirstDataOffset();
        }

        /* Step 2.2. Check if current item is a duplicate */
        ItemId *currItemId = btrPage->GetItemIdPtr(checkOff);
        if (currItemId->IsNormal()) {
            IndexTuple *tupleOnPage = btrPage->GetIndexTuple(currItemId);
            if (m_indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX)) {
                Oid checkTuplePartOid = tupleOnPage->GetTableOid(m_indexInfo);
                bool isVisible = false;
                if (STORAGE_FUNC_FAIL(CheckGPIPartVisible(checkTuplePartOid, &isVisible))) {
                    ret = DSTORE_FAIL;
                    break;
                }
                if (!isVisible) {
                    checkOff++;
                    continue;
                }
            }
            if (CompareKeyToTuple(btrPage, btrPage->GetLinkAndStatus(), checkOff,
                                  btrPage->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE)) != 0) {
                /* All equal tuples have been checked. */
                break;
            }
            /* Comparing result is "equal", it's a duplicate */
            /* Step 2.3. Deal with m_needDefferCheck case (do not report error).
             * Insert the duplicate. Caller would recheck unique when commit */
            if (m_needDefferCheck) {
                m_satisfiedUnique = false;
                break;
            }

            /* Step 2.4. Check visibility of current item */
            bool visible = CheckTupleVisibility(btrPage, checkOff, waitXid);
            if (visible) {
                /* Fail if the duplicated tuple is visible */
                m_satisfiedUnique = false;
                m_insertOff = checkOff;
                m_duplicateTuple = tupleOnPage->Copy();
                if (STORAGE_VAR_NULL(m_duplicateTuple)) {
                    storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
                    ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to copy duplicate tuple."));
                    ret = DSTORE_FAIL;
                }
                break;
            }
            if (*waitXid != INVALID_XID) {
                /* *waitXid == INVALID_XID means invisible for already deleted. */
                /* *waitXid != INVALID_XID means invisible for open transaction. Need to wait and recheck. */
                m_satisfiedUnique = false;
                break;
            }
        }

        /* Current duplicate is invisible. Go check next tuple. */
        checkOff++;
    }

    if (rightBuf != INVALID_BUFFER_DESC) {
        m_bufMgr->UnlockAndRelease(rightBuf);
    }
    return ret;
}

bool BtreeInsert::CheckTupleVisibility(BtrPage *btrPage, OffsetNumber checkOff, Xid *waitXid)
{
    if (!BtreeSplit::CheckTupleVisibility(btrPage, checkOff, waitXid)) {
        StorageAssert(*waitXid != INVALID_XID);
        return false;
    }

    /* Duplicate is visible. Check if deleted */
    if (btrPage->GetIndexTuple(checkOff)->IsDeleted()) {
        /* The deletion of tuple is visible. Treat it as invisible tuple. */
        *waitXid = INVALID_XID;
        return false;
    }
    /* Duplicate is visible and not deleted */
    IndexTuple *checkTuple = btrPage->GetIndexTuple(checkOff);
    TupleTdStatus tdStatus = checkTuple->GetTdStatus();
    Xid checkTupleXid = INVALID_XID;
    TD *td = nullptr;
    CommitSeqNo csn = INVALID_CSN;
    TrxSlotStatus xsStatus = TXN_STATUS_UNKNOWN;
    ItemPointerData heapCtid = btrPage->GetIndexTuple(checkOff)->GetHeapCtid();
    if (checkTuple->TestTdStatus(ATTACH_TD_AS_NEW_OWNER)) {
        td = btrPage->GetTd(checkTuple->GetTdId());
        checkTupleXid = td->GetXid();
        XidStatus xs(checkTupleXid, thrd->GetActiveTransaction(), td);
        csn = xs.GetCsn();
        xsStatus = xs.GetStatus();
    } else if (checkTuple->TestTdStatus(ATTACH_TD_AS_HISTORY_OWNER)) {
        td = btrPage->GetTd(checkTuple->GetTdId());
        checkTupleXid = td->GetXid();
        if (checkTupleXid != INVALID_XID) {
            TransactionMgr *transactionMgr = g_storageInstance->GetPdb(this->GetPdbId())->GetTransactionMgr();
            StorageAssert(transactionMgr != nullptr);
            UndoRecord undoRecord;
            transactionMgr->FetchUndoRecordByMatchedCtid(checkTupleXid, &undoRecord, td->GetUndoRecPtr(), heapCtid,
                                                         &csn);
            xsStatus = transactionMgr->GetTxnSlotStatus(checkTupleXid);
        }
    }
    ItemPointerData insertHeapCtid = m_insertTuple->GetHeapCtid();
    storage_set_error(INDEX_ERROR_INSERT_UNIQUE_CHECK);
    ErrLog(m_duplicateErrLevel, MODULE_INDEX,
        ErrMsg("Duplicate indexTuple(%hu, %u, %hu) with heapCtid(%hu, %u, %hu) is visible, "
        "insertheapCtid(%hu, %u, %hu), xid(%d, %lu), status:%hhu, csn:%lu, tdstatus:%hhu, index(%s), segment(%hu, %u), "
        "currXid(%d, %lu), snapshotcsn:%lu "
        BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
        btrPage->GetSelfPageId().m_fileId, btrPage->GetSelfPageId().m_blockId, checkOff,
        heapCtid.GetFileId(), heapCtid.GetBlockNum(), heapCtid.GetOffset(),
        insertHeapCtid.GetFileId(), insertHeapCtid.GetBlockNum(), insertHeapCtid.GetOffset(),
        static_cast<int32>(checkTupleXid.m_zoneId), checkTupleXid.m_logicSlotId, xsStatus, csn, tdStatus,
        m_indexInfo->indexRelName, GetBtreeSmgr()->GetSegMetaPageId().m_fileId,
        GetBtreeSmgr()->GetSegMetaPageId().m_blockId, static_cast<int32>(thrd->GetCurrentXid().m_zoneId),
        thrd->GetCurrentXid().m_logicSlotId, thrd->GetSnapShotCsn(),
        BTR_PAGE_HEADER_VAL(btrPage), BTR_PAGE_LINK_AND_STATUS_VAL(btrPage->GetLinkAndStatus())));
    m_satisfiedUnique = false;
    return true;
}

RetStatus BtreeInsert::StepRightWhenCheckUnique(BufferDesc **rightBuf, LWLockMode access)
{
    StorageAssert(access == LW_EXCLUSIVE ||
                  m_indexInfo->btrIdxStatus == BtrCcidxStatus::WRITE_ONLY_INDEX ||
                  m_scanKeyValues.heapCtid == INVALID_ITEM_POINTER);

    BufferDesc *currBuf = (*rightBuf != INVALID_BUFFER_DESC) ? *rightBuf : m_insertPageBuf;
    BtrPage *currPage = static_cast<BtrPage *>(currBuf->GetPage());
    BtrPageLinkAndStatus *currLinkStat = currPage->GetLinkAndStatus();
    bool needFreeRight = (*rightBuf != INVALID_BUFFER_DESC) && (access == LW_SHARED);
    if (currLinkStat->IsRightmost()) {
        /* Finish if we've reached the end of leaf level */
        if (needFreeRight) {
            /* All check finished. Release page for read mode and we're done. */
            m_bufMgr->UnlockAndRelease(*rightBuf);
        }
        /* It's safe to set *rightBuf invalid because for write-lock mode, we've already saved it as insertBuf */
        *rightBuf = INVALID_BUFFER_DESC;
        return DSTORE_SUCC;
    }

    /* More pages to check. Determine wether to check next page by comparing with hikey on current page */
    int32 goRightVal = m_scanKeyValues.heapCtid == INVALID_ITEM_POINTER ? 0 : 1;
    int32 cmpRet =
        CompareKeyToTuple(currPage, currLinkStat, BTREE_PAGE_HIKEY, currLinkStat->TestType(BtrPageType::INTERNAL_PAGE));
    if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to compare tuple since compare function returns null."));
        if ((*rightBuf) != m_insertPageBuf) {
            /* m_insertPageBuf would be released by Clear(). */
            m_bufMgr->UnlockAndRelease(*rightBuf);
            *rightBuf = INVALID_BUFFER_DESC;
        }
        return DSTORE_FAIL;
    }
    StorageAssert(cmpRet <= goRightVal);
    if (cmpRet < goRightVal) {
        /* For unique check, also need to check next page if scan key == hikey */
        if (needFreeRight) {
            /* All check finished. Release page for read mode and we're done. */
            m_bufMgr->UnlockAndRelease(*rightBuf);
        }
        /* It's safe to set *rightBuf invalid because for write-lock case, we've already saved it as insertBuf */
        *rightBuf = INVALID_BUFFER_DESC;
        return DSTORE_SUCC;
    }

    PageId nextPageId = currLinkStat->GetRight();
    for (;;) {
        if (unlikely(!nextPageId.IsValid())) {
            storage_set_error(INDEX_ERROR_MOVE_END, m_indexInfo->indexRelName);
            Xid btrCreateXid = GetBtreeSmgr()->GetMetaCreateXid();
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Right page is invalid when StepRightWhenCheckUnique. "
                "pdb:%u, index(%s:%u), createXid(%d, %lu), segment(%hu, %u), currXid(%d, %lu)"
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                GetPdbId(), m_indexInfo->indexRelName, m_indexRel->relOid,
                static_cast<int>(btrCreateXid.m_zoneId), btrCreateXid.m_logicSlotId,
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                static_cast<int>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
                BTR_PAGE_HEADER_VAL(currPage), BTR_PAGE_LINK_AND_STATUS_VAL(currLinkStat)));
            m_bufMgr->UnlockAndRelease(*rightBuf);
            *rightBuf = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        *rightBuf = ReleaseOldGetNewBuf(*rightBuf, nextPageId, access);
        if (STORAGE_VAR_NULL(*rightBuf)) {
            return DSTORE_FAIL;
        }
        currPage = static_cast<BtrPage *>((*rightBuf)->GetPage());
        currLinkStat = currPage->GetLinkAndStatus();
        /* Finish splitting if incomplete */
        if (access == LW_EXCLUSIVE && unlikely(!currLinkStat->IsSplitComplete())) {
            if (STORAGE_FUNC_FAIL(CompleteSplit(*rightBuf, m_leafStack, LW_EXCLUSIVE))) {
                return DSTORE_FAIL;
            }
            /* buffer is released after split */
            *rightBuf = INVALID_BUFFER_DESC;
            continue;
        }
        if (!currLinkStat->IsUnlinked()) {
            if (currLinkStat->IsRightmost()) {
                break;
            }
            cmpRet = CompareKeyToTuple(currPage, currLinkStat, BTREE_PAGE_HIKEY,
                                       currLinkStat->TestType(BtrPageType::INTERNAL_PAGE));
            if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to compare tuple since compare function returns null."));
                if ((*rightBuf) != m_insertPageBuf) {
                    /* m_insertPageBuf would be released by Clear() */
                    m_bufMgr->UnlockAndRelease(*rightBuf);
                }
                return DSTORE_FAIL;
            }
            if (cmpRet <= goRightVal) {
                break;
            }
        }
        if (unlikely(currLinkStat->IsRightmost())) {
            m_bufMgr->UnlockAndRelease(*rightBuf);
            *rightBuf = INVALID_BUFFER_DESC;
            storage_set_error(INDEX_ERROR_MOVE_END, m_indexInfo->indexRelName);
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Reach end of of btree level when check unique in(%s), segment(%hu, %u) "
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, m_indexInfo->indexRelName,
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                BTR_PAGE_HEADER_VAL(currPage), BTR_PAGE_LINK_AND_STATUS_VAL(currLinkStat)));
            return DSTORE_FAIL;
        }
        nextPageId = currLinkStat->GetRight();
    }

    StorageAssert(currLinkStat->IsRightmost() ||
                  CompareKeyToTuple(currPage, currLinkStat, BTREE_PAGE_HIKEY,
                                    currLinkStat->TestType(BtrPageType::INTERNAL_PAGE)) <= goRightVal);
    return DSTORE_SUCC;
}

void BtreeInsert::UpdateCachedInsertionPageIdIfNeeded(const PageId insertPageId)
{
    if (!m_useInsertPageCache) {
        return;
    }
    uint32 rootLevel = GetBtreeSmgr()->GetRootLevelFromMetaCache();
    if (rootLevel >= BTREE_HIGHEST_LEVEL) {
        /* We've constructed a btree meta cache when GetRoot. But it may be staled and cleared by others.
         * Then we need to read the real meta page */
        BufferDesc *btrMetaBuf = INVALID_BUFFER_DESC;
        BtrMeta *btrMeta = GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
        /* This function is a performance optimization, so failure here does not affect the main process. */
        if (unlikely(btrMeta == nullptr)) {
            ErrLog(DSTORE_WARNING, MODULE_INDEX,
                   ErrMsg("Can not get btrMeta when UpdateCachedInsertionPageIdIfNeeded."));
            return;
        }
        rootLevel = btrMeta->GetRootLevel();
        m_bufMgr->UnlockAndRelease(btrMetaBuf);
    }

    if (rootLevel > 1U) {
        /* Only cache insertion page ID when the btree has more than one level */
        GetBtreeSmgr()->SetLastInsertionPageId(insertPageId);
    }
    return;
}

WalRecord *BtreeInsert::GenerateLeafInsertWal()
{
    StorageAssert(NeedWal());
    BtrPage *insPage = static_cast<BtrPage *>(m_insertPageBuf->GetPage());

    /* Generate WalRecordBtreeInsertOnLeaf + AllocTd */
    uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(m_tdContext);
    uint32 walDataSize = sizeof(WalRecordBtreeInsertOnLeaf) + m_insertTuple->GetSize() + allocTdSize;
    WalRecordBtreeInsertOnLeaf *walData = static_cast<WalRecordBtreeInsertOnLeaf *>(DstorePalloc(walDataSize));
    if (unlikely(walData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when GenerateLeafInsertWal."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return nullptr;
    }
    /* get file version */
    if (unlikely(m_insertPageBuf->fileVersion == INVALID_FILE_VERSION)) {
        m_insertPageBuf->SetFileVersion(WalUtils::GetFileVersion(m_insertPageBuf->GetPdbId(),
            m_insertPageBuf->GetPageId().m_fileId));
    }
    walData->SetHeader(insPage, walDataSize, insPage->GetWalId() != thrd->m_walWriterContext->GetWalId(), m_insertOff,
                       m_insertPageBuf->GetFileVersion());
    walData->SetTupleData(m_insertTuple);
    walData->SetAllocTd(m_tdContext);

    return walData;
}

/*
 * Restore insertion for Undo record writing failure.
 * When caller hit an undo generating failure that we don't have an undo record to rollback, call
 * RestorePageWithoutUndoRec to remove the tuple that we just inserted onto the page directly. Neither undo record
 * nor wal record would be written for this insertion any longer.
 * Note that we would only restore ItemId and IndexTuple on page. Any change within TD space would be ignore.
 */
void BtreeInsert::RestorePageWithoutUndoRec()
{
    BtrPage *insPage = static_cast<BtrPage *>(m_insertPageBuf->GetPage());
    IndexTuple *insertedTuple = insPage->GetIndexTuple(m_insertOff);
    StorageReleasePanic((memcmp(insertedTuple, m_insertTuple, m_insertTuple->GetSize()) != 0),
                        MODULE_INDEX, ErrMsg("insertedTuple has been changed"));
    StorageReleasePanic(insPage->RemoveItemId(m_insertOff) == DSTORE_FAIL, MODULE_INDEX,
                        ErrMsg("insertedTuple has been changed"));
    insPage->SetUpper(insPage->GetUpper() + static_cast<uint16>(insertedTuple->GetSize()));
    ErrLog(DSTORE_WARNING, MODULE_INDEX,
           ErrMsg("Restore insertion indexTuple(%hu, %u, %hu) with heapCtid(%hu, %u, %hu), plsn: %lu",
                  m_insertPageBuf->GetPageId().m_fileId, m_insertPageBuf->GetPageId().m_blockId, m_insertOff,
                  m_insertTuple->GetHeapCtid().GetFileId(), m_insertTuple->GetHeapCtid().GetBlockNum(),
                  m_insertTuple->GetHeapCtid().GetOffset(), insPage->GetPlsn()));
}

void BtreeInsert::HandleErrorAndRestorePage(UndoRecord *undoRec)
{
    if (undoRec == nullptr) {
        RestorePageWithoutUndoRec();
    } else {
        /* We have an undoRec now but it has not been inserted into Undo Page(neither disk nor memory). Must
            * rollback this undoRec ourselves here because Transaction will not do so when aborting. */
        HandleErrorWhenGetUndoBuffer(static_cast<BtrPage *>(m_insertPageBuf->GetPage()), undoRec, m_insertTuple);
    }
}

}

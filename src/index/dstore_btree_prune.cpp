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
 * This file defines the BtreePagePrune class.
 *
 * IDENTIFICATION
 *        src/index/dstore_btree_prune.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "index/dstore_btree_prune.h"
#include "index/dstore_btree_wal.h"
#include "wal/dstore_wal_write_context.h"
#include "transaction/dstore_transaction.h"
#include "index/dstore_btree_page_recycle.h"

namespace DSTORE {

#ifndef UT
constexpr uint16 BTREE_TD_PRUNE_THRESHOLD = 75;
#else
constexpr uint16 BTREE_TD_PRUNE_THRESHOLD = 40;
#endif
constexpr uint16 BTREE_TD_PRUNE_FACTOR = 2;

BtreePagePrune::BtreePagePrune(StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scankeyInfo,
                               BufferDesc *pageBuf)
    : Btree(indexRel, indexInfo), m_scankeyInfo(scankeyInfo)
{
    StorageReleasePanic(pageBuf == INVALID_BUFFER_DESC, MODULE_INDEX, ErrMsg("pageBuf invalid"));
    m_pagePayload.InitByBuffDesc(pageBuf);
    if (indexRel != nullptr) {
        m_pdbId = indexRel->m_pdbId;
    }
    Init();
}

BtreePagePrune::BtreePagePrune(PdbId pdbId, BtrPage *page) : Btree(nullptr)
{
    m_pagePayload.page = page;
    m_pagePayload.pageId = page->GetSelfPageId();
    m_pagePayload.linkAndStatus = page->GetLinkAndStatus();
    m_pdbId = pdbId;
    Init();
}

BtreePagePrune::~BtreePagePrune()
{
    DstorePfreeExt(m_isTupleLive);
    DstorePfreeExt(m_liveItems);
}

void BtreePagePrune::Init()
{
    m_isPagePrunable = true;
    m_prunedAll = true;
    m_canCompactTds = true;
    m_tdContext.Init(this->GetPdbId(), NeedWal());
    m_origTdCount = m_pagePayload.GetPage()->GetTdCount();
    m_fixedTdCount = m_origTdCount;

    m_internalDelOffset = INVALID_ITEM_OFFSET_NUMBER;
    m_leafDelOffset = INVALID_ITEM_OFFSET_NUMBER;
    m_numTotalTuples = m_pagePayload.GetPage()->GetMaxOffset(); /* Include high key if exists */
    m_numLiveTuples = m_numTotalTuples;
    m_currMaxCsn = INVALID_CSN;

    /* Arrays of tds & tuples info are not initialized here to save memory in case the page does not need to prune */
    m_isTupleLive = nullptr;
    m_liveItems = nullptr;
}

void BtreePagePrune::GenerateWalForPrune(void *redoPtr, uint32 redoDataSize)
{
    StorageAssert(redoPtr != nullptr);
    WalRecordBtreePagePrune *redoData = static_cast<WalRecordBtreePagePrune *>(redoPtr);
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->RememberPageNeedWal(m_pagePayload.GetBuffDesc());
    bool glsnChangedFlag = (m_pagePayload.GetPage()->GetWalId() != walContext->GetWalId());
    redoData->SetHeader(m_pagePayload.GetPage(), redoDataSize, glsnChangedFlag, m_fixedTdCount, m_numLiveTuples,
                        m_pagePayload.GetBuffDesc()->GetFileVersion());
    uint32 offset = redoData->SetData(m_isTupleLive, m_numTotalTuples);
    redoData->SetAllocTd(m_tdContext, offset);
    walContext->PutNewWalRecord(redoData);
}

RetStatus BtreePagePrune::Prune(OffsetNumber internalDelOffset, OffsetNumber leafDelOffset)
{
    if (STORAGE_VAR_NULL(thrd->GetActiveTransaction())) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr \"Transaction\" in current thread.", __FUNCTION__));
        return DSTORE_FAIL;
    }

    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    if (STORAGE_VAR_NULL(walContext)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Wal writer context is null in current thread.", __FUNCTION__));
        return DSTORE_FAIL;
    }

    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    if (STORAGE_VAR_NULL(csnMgr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr \"CsnMgr\" in current instance.", __FUNCTION__));
        return DSTORE_FAIL;
    }

    RetStatus ret = DSTORE_SUCC;
    uint32 redoDataSize = static_cast<uint32>(sizeof(WalRecordBtreePagePrune));
    WalRecordBtreePagePrune *redoData = nullptr;

    m_leafDelOffset = leafDelOffset;
    m_internalDelOffset = internalDelOffset;
    StorageAssert(m_internalDelOffset == INVALID_ITEM_OFFSET_NUMBER ||
                  m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE));

    /* Step 0. Check if the page is prunable */
    if (!m_pagePayload.GetPage()->HasPrunableTuple()) {
        /* No need to prune a page if it contains no deleted tuple. */
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("No need to prune Btree page {%hu, %u} because no tuple has been deleted",
                      m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId));
        m_isPagePrunable = false;
        goto EXIT;
    }
    StorageAssert(m_pagePayload.GetPage()->CheckSanity());

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Start to prune Btree page {%hu, %u}", m_pagePayload.GetPageId().m_fileId,
                  m_pagePayload.GetPageId().m_blockId));

    /* Step 1. Scan for frozen TDs to figure out if the td space need to be compact */
    if (m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE)) {
        ret = ScanActiveTds();
    }
    if (STORAGE_FUNC_FAIL(ret) || !m_isPagePrunable) {
        goto EXIT;
    }

    /* Step 2. Scan to find live tuples */
    ret = ScanLiveTuples();
    if (STORAGE_FUNC_FAIL(ret)) {
        goto EXIT;
    }
    if (!m_isPagePrunable) {
        goto EXIT;
    }

    if (!(m_fixedTdCount < m_origTdCount) &&
        (m_numLiveTuples == m_numTotalTuples) &&
        !m_pagePayload.GetPage()->HasGarbageSpace()) {
        /* No need to prune if neither frozen tds ratio is beyond the threshold nor any deletable tuple exists. */
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("No need to prune Btree page {%hu, %u} because neither TD space nor tuples are prunable.",
                      m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId));
        m_isPagePrunable = false;
        goto EXIT;
    }

    /* Step 3. Prepare memory for defragmentation */
    /* No error is acceptable after we write the page!!! We must prepare memory and data before we do any change. */
    m_liveItems = static_cast<ItemIdCompact>(DstorePalloc(sizeof(ItemIdCompactData) * m_numLiveTuples));
    if (unlikely(m_liveItems == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc failed when alloc for ItemIdCompactData."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ret = DSTORE_FAIL;
        goto EXIT;
    }

    if (NeedWal()) {
        uint32 tupleStatusSize = (m_numTotalTuples + 1) * static_cast<uint32>(sizeof(bool));
        uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(m_tdContext);
        redoDataSize += tupleStatusSize + allocTdSize;
        redoData = static_cast<WalRecordBtreePagePrune *>(DstorePalloc(redoDataSize));
        if (unlikely(redoData == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when GenerateWalForPrune."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ret = DSTORE_FAIL;
            goto EXIT;
        }
        Xid curXid = thrd->GetActiveTransaction()->GetCurrentXid();
        walContext->BeginAtomicWal(curXid);
    }

    CompactTdSpace();
    CompactItems();
    CompactTuples();

    /* Prune finished, erase tuple_prunable flag. */
    m_pagePayload.GetPage()->SetTuplePrunable(!m_prunedAll);
    StorageAssert(m_pagePayload.GetPage()->CheckSanity());

    UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_pagePayload.GetBuffDesc()));

    /* Write Wal if needed */
    if (NeedWal()) {
        GenerateWalForPrune(redoData, redoDataSize);
        UNUSED_VARIABLE(walContext->EndAtomicWal());
    }

    DstorePfreeExt(redoData);
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Succeed to prune Btree page {%hu, %u}, %hu tuples in total, %hu live",
                  m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId, m_numTotalTuples,
                  m_numLiveTuples));
    return DSTORE_SUCC;

EXIT:
    if (m_tdContext.allocTd.isDirty) {
        StorageAssert(m_tdContext.allocTd.isDirty); /* GenerateWalForPrune() shouldn't have cleaned it */
        (void)m_bufMgr->MarkDirty(m_pagePayload.GetBuffDesc());
        GenerateAllocTdWal(m_pagePayload.GetBuffDesc(), m_tdContext);
    }
    if (unlikely(m_tdContext.allocTd.hasRollbackTd)) {
        (void)m_bufMgr->MarkDirty(m_pagePayload.GetBuffDesc());
        GenerateRollbackTdWal(m_pagePayload.GetBuffDesc(), m_tdContext);
    }
    return ret;
}

RetStatus BtreePagePrune::ScanActiveTds()
{
    m_origTdCount = m_pagePayload.GetPage()->GetTdCount();
    m_fixedTdCount = m_origTdCount;

    m_tdContext.Begin(m_pdbId, g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(m_pdbId));
    TdId tdId = m_pagePayload.GetPage()->TryReuseTdSlots<PageType::INDEX_PAGE_TYPE>(m_tdContext);
    if (m_origTdCount <= DEFAULT_TD_COUNT || /* Already min TD size. No need to prune TD space. */
        tdId == INVALID_TD_SLOT) {           /* Nothing can do since all TDs are occupied and in-progress */
        return DSTORE_SUCC;
    }
    uint8 lastActiveTdId = m_origTdCount - 1;
    uint8 reusableTdCount = 0;
    bool findActiveTd = false;
    for (tdId = lastActiveTdId;; tdId--) {
        TD *td = m_pagePayload.GetPage()->GetTd(tdId);
        if (td->TestStatus(TDStatus::UNOCCUPY_AND_PRUNEABLE)) {
            if (findActiveTd) {
                reusableTdCount++;
            }
        } else {
            if (unlikely(!findActiveTd)) {
                lastActiveTdId = tdId;
                findActiveTd = true;
            }
            if (td->TestStatus(TDStatus::OCCUPY_TRX_END)) {
                reusableTdCount++;
            } else {
                /* In-progress transaction exists, cannot compact TD space.
                 * However, committed dead tuples can still be pruned. */
                m_canCompactTds = false;
            }
        }
        if (tdId == 0U) {
            break;
        }
    }
    if (!findActiveTd) { /* No active TD found, all TDs are UNOCCUPY_AND_PRUNEABLE */
        reusableTdCount = m_origTdCount;
        lastActiveTdId = 0;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Scanning of TD space on Btree page {%hu, %u} finished. %hu active TDs found among %hu in total",
                  m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId, lastActiveTdId + 1,
                  m_origTdCount));

    if (!m_canCompactTds ||                                 /* In-progress transaction exists. Cannot compact TDs. */
        lastActiveTdId == m_origTdCount - 1 ||              /* Found no frozen td. No need to prune. */
        m_origTdCount < DEFAULT_TD_COUNT + EXTEND_TD_NUM) { /* Or TD space has not been extended. */
        return DSTORE_SUCC;
    }

    uint8 minTdKeep = lastActiveTdId + 1;
    if (reusableTdCount == 0U) {
        /* We don't want to do TD extent right after we pruned them. So don't prune them all, keep some reuseable TDs */
        minTdKeep = DstoreMin(minTdKeep + EXTEND_TD_NUM, m_origTdCount);
    }
    m_fixedTdCount = static_cast<uint16>(((minTdKeep + 1) / 2) * 2); /* Number of TD should be muptiple of 2 */
    m_fixedTdCount = DstoreMax(m_fixedTdCount, DEFAULT_TD_COUNT);

    StorageAssert(m_fixedTdCount >= DEFAULT_TD_COUNT);
    StorageAssert(m_fixedTdCount >= lastActiveTdId + 1);

    /* Do not update td_count in page header now because we still need the original ones to get tuples later.
     * Do it after defragmentation if needed */
    return DSTORE_SUCC;
}

/* A tuple is prunable if
 *      1. item_id has a ITEM_ID_DEAD or ITEM_ID_UNUSED flag
 *      2. the tuple has been marked deleted && transaction is committed
 */
bool BtreePagePrune::IsTuplePrunable(OffsetNumber offnum, bool checkCsn)
{
    if (!m_pagePayload.GetLinkAndStatus()->IsRightmost() && offnum == BTREE_PAGE_HIKEY) {
        /* We can never prune high key */
        return false;
    }

    ItemId *item = m_pagePayload.GetPage()->GetItemIdPtr(offnum);
    IndexTuple *tuple = nullptr;
    if (likely(item->IsNormal())) {
        tuple = m_pagePayload.GetPage()->GetIndexTuple(item);
        if (!tuple->IsDeleted()) {
            /* item_id is marked normal and tuple doesn't have any deleted flag. It's not a dead tuple */
            return false;
        }
        if (m_indexInfo->btrIdxStatus != BtrCcidxStatus::NOT_CCINDEX && tuple->IsInsertDeletedForCCindex()) {
            /* The index is under concurrent building/rebuilding, need to keep deleted tuples with insertDeleted flag
             * until concurrent building/rebuilding finished. */
            return false;
        }
        if (m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE) || tuple->TestTdStatus(DETACH_TD)) {
            /* Pivot page doesn't have TD space. Tuple is prunable once deleted. */
            /* Deleted tuple with a detached TD slot is safe to be prune */
            return true;
        }
    } else if (item->IsUnused()) {
        /* Unused item can be pruned immediately */
        return true;
    } else {
        /* item_id has an "rangeholder" flag */
        StorageAssert(item->IsRangePlaceholder());
        StorageAssert(!m_pagePayload.GetPage()->GetIndexTuple(item)->IsDeleted());
        return offnum != m_pagePayload.GetLinkAndStatus()->GetFirstDataOffset();
    }
    StorageAssert(m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));

    /* Here only leaf page's tuples containing deleted tags are left. */
    TdId tdId = tuple->GetTdId();
    StorageAssert(tdId != INVALID_TD_SLOT);
    TD *td = m_pagePayload.GetPage()->GetTd(tdId);
    Xid tupleXid = td->GetXid();
    XidStatus xidStatus(tupleXid, thrd->GetActiveTransaction(), td);

    /*
     * if the td status is OCCUPY_TRX_IN_PROGRESS, the transaction status may be committed or aborted.
     * but if the transaction is in progress, the td status must be OCCUPY_TRX_IN_PROGRESS.
     */
    if (tuple->TestTdStatus(ATTACH_TD_AS_NEW_OWNER) && td->TestStatus(TDStatus::OCCUPY_TRX_IN_PROGRESS)) {
        if (xidStatus.IsInProgress()) {
            /* Tuple has an in-progress transaction. Skip this tuple but allow pruning other committed dead tuples.
             * Undo rollback uses BinarySearch by key value, so offset renumbering from CompactItems is safe. */
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                ErrMsg("Btree page{%hu, %u} tuple (offnum = %hu) skipped: transaction in-progress.",
                       m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId, offnum));
            m_prunedAll = false;
            return false;
        }
    }
    if (tupleXid != INVALID_XID &&
        !(xidStatus.IsCommitted() || xidStatus.IsFrozen() || xidStatus.IsPendingCommit())) {
        /* The change of tuple is not committed yet. We don't prune uncommitted deletions. */
        m_prunedAll = false;
        return false;
    }
    /* Global temp table doesn't need to check csn. Just skip csn checking and keep it on page */
    if (GetBtreeSmgr()->IsGlobalTempIndex()) {
        m_prunedAll = false;
        return false;
    }
    /*
     * Tuple containing deleted tags and committed will be pruned if not check the csn.
     * 1. If it need be visited when the snapshot csn is before the tuple delete csn,
     *    it will be add again from cr page when execute undo for delete.
     * 2. If it need be visited when the snapshot csn is after the tuple delete csn,
     *    it is invisible.
    */
    bool isDirty = false;
    bool isPrunable = true;
    if (checkCsn) {
        CommitSeqNo tupCsn = INVALID_CSN;
        CommitSeqNo recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(m_pdbId);
        isPrunable = m_pagePayload.GetPage()->JudgeTupCommitBeforeSpecCsn<false>(
            m_pdbId, offnum, recycleMinCsn, isDirty, &tupCsn);
        if (tupCsn != INVALID_CSN) {
            SetCurrMaxCsn(tupCsn);
        }
        if (!isPrunable) {
            /* The deleted tuple cannot be pruned for csn >= recycleMinCsn.
             * Not all deleted tuples will be pruned this round. Mark it to keep Prunalbe flag after pruned. */
            m_prunedAll = false;
            if (unlikely(item->IsRangePlaceholder())) {
                ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                    ErrMsg("UnprunableRangeHolder: " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT
                    " min:%hu, max:%hu, rangeholder:%hu, nonDel:%hu",
                    BTR_PAGE_HEADER_VAL(m_pagePayload.GetPage()),
                    BTR_PAGE_LINK_AND_STATUS_VAL(m_pagePayload.GetLinkAndStatus()),
                    m_pagePayload.GetLinkAndStatus()->GetFirstDataOffset(), m_pagePayload.GetPage()->GetMaxOffset(),
                    offnum, m_pagePayload.GetPage()->GetNonDeletedTupleNum()));
            }
        }
    }
    if (isDirty) {
        UNUSED_VARIABLE(m_bufMgr->MarkDirty(GetPagePayload()->GetBuffDesc(), false));
    }
    return isPrunable;
}

RetStatus BtreePagePrune::ScanLiveTuples(bool checkCsn)
{
    m_isPagePrunable = true;
    /* Start to scan page from the first item. If page is rightmost of the level,
     * BTREE_PAGE_HIKEY will represent the first data offset. That also makes sence */
    OffsetNumber minOffset = BTREE_PAGE_HIKEY;
    OffsetNumber maxOffset = m_pagePayload.GetPage()->GetMaxOffset();
    /* The page should have at least one tuple except high key */
    StorageAssert(maxOffset >= m_pagePayload.GetLinkAndStatus()->GetFirstDataOffset());
    m_numLiveTuples = 0;
    m_isTupleLive = (bool *)DstorePalloc(sizeof(bool) * (maxOffset + 1));
    if (unlikely(m_isTupleLive == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when ScanLiveTuples."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }

    if (m_internalDelOffset != INVALID_ITEM_OFFSET_NUMBER) {
        for (OffsetNumber offset = minOffset; offset <= maxOffset; offset = OffsetNumberNext(offset)) {
            m_isTupleLive[offset] = true;
        }
        StorageAssert(IsTuplePrunable(m_internalDelOffset, checkCsn));
        m_isTupleLive[m_internalDelOffset] = false;
        m_numLiveTuples = m_numTotalTuples - 1U;
        return DSTORE_SUCC;
    }

    OffsetNumber hikeyValOffset = OffsetNumberNext(m_pagePayload.GetPage()->GetMaxOffset());
    if (!checkCsn && !m_pagePayload.GetLinkAndStatus()->IsRightmost()) {
        IndexTuple *hikey = m_pagePayload.GetPage()->GetIndexTuple(BTREE_PAGE_HIKEY);
        if (hikey->GetHeapCtid() != INVALID_ITEM_POINTER) {
            /* The hikey has an heapCtid, need to check if there's any key is exactly the same with hikey */
            InitScanKey(m_scankeyInfo);
            UpdateScanKeyWithValues(hikey);
            hikeyValOffset = BinarySearchOnPage(m_pagePayload.GetPage());
            if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                        ErrMsg("Failed to binary search btrPage(%hu, %u) since compare function returns null.",
                        m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId));
                return DSTORE_FAIL;
            }
        }
    }

    bool hitNewLowkey = false;
    for (OffsetNumber offset = minOffset; offset <= maxOffset; offset = OffsetNumberNext(offset)) {
        /* Do not clear a tuple if it's the first data key on page, or if it has the same key value with hikey, until
         * the deletion is visible to all. */
        bool checkCurrTupCsn = checkCsn || unlikely(offset >= hikeyValOffset || !hitNewLowkey);
        if (IsTuplePrunable(offset, checkCurrTupCsn)) {
            m_isTupleLive[offset] = false;
        } else {
            hitNewLowkey = hitNewLowkey ||
                           (m_pagePayload.GetLinkAndStatus()->IsRightmost() || offset != BTREE_PAGE_HIKEY);
            m_isTupleLive[offset] = true;
            m_numLiveTuples++;
            if (!m_isPagePrunable) {
                return DSTORE_SUCC;
            }
        }
    }
    return DSTORE_SUCC;
}

void BtreePagePrune::CompactTdSpace()
{
    if (m_fixedTdCount == m_origTdCount) {
        return;
    }
    OffsetNumber minOffset = m_pagePayload.GetLinkAndStatus()->GetFirstDataOffset();
    OffsetNumber maxOffset = m_pagePayload.GetPage()->GetMaxOffset();

    /* We'll only keep TD slot that carries active TD (in case TD_ACTIVE). Frozen TD will be pruned if its TdId
     * is greater than m_fixedTdCount. Thus tuple that corresponding to the frozen TD need to update its TdId
     * to avoid later access to an unexisted TD slot. */
    for (OffsetNumber offset = minOffset; offset <= maxOffset; offset = OffsetNumberNext(offset)) {
        if (!m_pagePayload.GetPage()->GetItemIdPtr(offset)->IsNormal() && offset != minOffset) {
            continue;
        }
        IndexTuple *tuple = m_pagePayload.GetPage()->GetIndexTuple(offset);
        if (tuple->GetTdId() < m_fixedTdCount) {
            continue;
        }
        tuple->SetTdId(INVALID_TD_SLOT);
        tuple->SetTdStatus(DETACH_TD);
    }
    /* Do not update td_count of the page right now because we still need to use the old values to get item_id */
    StorageAssert(m_pagePayload.GetPage()->CheckSanity());

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Compact TD space on Btree page {%hu, %u} successfully. Reduced number of TD slots from %hhu to %hu",
                  m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId,
                  m_pagePayload.GetPage()->GetTdCount(), m_fixedTdCount));
}

void BtreePagePrune::CompactItems()
{
    StorageAssert(m_isTupleLive != nullptr);

    /* m_fixedTdCount = origTdCount if TD space has not been pruned */
    uint16 fixedTdSize = static_cast<uint16>(m_fixedTdCount * sizeof(TD));
    /* Start to scan m_isTupleLive from the first item on the page. If page is rightmost of the level,
     * BTREE_PAGE_HIKEY will represent the first data offset. That also makes sence */
    OffsetNumber oldMinOff = BTREE_PAGE_HIKEY;
    OffsetNumber oldMaxOff = m_pagePayload.GetPage()->GetMaxOffset();

    ItemId *oriItemId = nullptr;
    /* Count offset of the new ItemId using new TD space size since num of TD slots may have been compacted too */
    ItemId *newItemId = static_cast<ItemId *>(static_cast<void *>(m_pagePayload.GetPage()->GetData() + fixedTdSize));
    uint16 liveItemsCount = 0;
    uint16 lower;
#ifdef DSTORE_USE_ASSERT_CHECKING
    uint16 accumulatedLen = 0;
#endif

    /* Never change the target delete offset without compact item physically. Save temparory value and overwrite
     * the final result only after we finish all compact steps successfully */
    OffsetNumber tmpLeafDelOffsetAfterPrune = m_leafDelOffset;
    for (OffsetNumber offnum = oldMinOff; offnum <= oldMaxOff; offnum = OffsetNumberNext(offnum)) {
        if (!m_isTupleLive[offnum]) {
            if (m_leafDelOffset != INVALID_ITEM_OFFSET_NUMBER && offnum < m_leafDelOffset) {
                tmpLeafDelOffsetAfterPrune -= 1U;
            }
            continue;
        }

        oriItemId = m_pagePayload.GetPage()->GetItemIdPtr(offnum);
        StorageAssert(m_pagePayload.GetPage()->CheckItemIdSanity(oriItemId));

        /* Copy the item to the new place. */
        *newItemId = *oriItemId;
        /* Please be aware that oriItemId may has been overwritten by this action. Do not use it any longer. */

        m_liveItems[liveItemsCount].tupOffset = newItemId->GetOffset();
        m_liveItems[liveItemsCount].itemOffnum = liveItemsCount + oldMinOff;
        m_liveItems[liveItemsCount].itemLen = newItemId->GetLen();
#ifdef DSTORE_USE_ASSERT_CHECKING
        accumulatedLen += m_liveItems[liveItemsCount].itemLen;
#endif
        newItemId++;
        liveItemsCount++;
    }

    /* number of live items must be equal to number of live tuples for index */
    StorageAssert(liveItemsCount == m_numLiveTuples);
    StorageAssert(accumulatedLen <= (m_pagePayload.GetPage()->GetSpecialOffset() -
                                     m_pagePayload.GetPage()->GetDataBeginOffset()));

    lower =
        m_pagePayload.GetPage()->DataHeaderSize() + fixedTdSize + static_cast<uint16>(m_numLiveTuples * sizeof(ItemId));
    m_pagePayload.GetPage()->SetLower(lower);
    m_pagePayload.GetPage()->dataHeader.tdCount = static_cast<uint8>(m_fixedTdCount);
    StorageAssert(m_pagePayload.GetPage()->CheckSanity());

    StorageAssert(liveItemsCount == m_pagePayload.GetPage()->GetMaxOffset());

    StorageAssert(tmpLeafDelOffsetAfterPrune <= m_leafDelOffset);
    m_leafDelOffset = tmpLeafDelOffsetAfterPrune;

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Compact ItemIds on Btree page {%hu, %u} successfully. Reduced number of ItemIds from %hu to %hu",
                  m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId, oldMaxOff,
                  m_pagePayload.GetPage()->GetMaxOffset()));
}

void BtreePagePrune::CompactTuples()
{
    StorageAssert(m_liveItems != nullptr);

    /* Sort live_items by tuples' offset so that tuples would be moved in a backward order to
     * avoid overwrite each other. */
    qsort(m_liveItems, m_numLiveTuples, sizeof(ItemIdCompactData), ItemIdCompactData::Compare);

    uint16 oldUpper = m_pagePayload.GetPage()->GetUpper();
    uint16 tupleStart = m_pagePayload.GetPage()->GetSpecialOffset();
    uint16 upper = m_pagePayload.GetPage()->CompactTuples(m_liveItems, m_numLiveTuples);

    m_pagePayload.GetPage()->SetUpper(upper);
    StorageAssert(m_pagePayload.GetPage()->CheckSanity());

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Compact tuples on Btree page {%hu, %u} successfully. Reduced size of tuples from %hu to %hu",
                  m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId,
                  static_cast<uint16>(oldUpper - tupleStart), static_cast<uint16>(upper - tupleStart)));
}

bool BtreePagePrune::IsPageEmpty(bool needPrune)
{
    if (unlikely(m_pagePayload.GetLinkAndStatus()->IsRightmost())) {
        /* Rightmost page can never be recycled. No need to check empty */
        return false;
    }
    OffsetNumber maxOff = m_pagePayload.GetPage()->GetMaxOffset();
    OffsetNumber minOff = m_pagePayload.GetLinkAndStatus()->GetFirstDataOffset();
    if (maxOff < minOff) {
        /* empty page, no need to prune */
        return true;
    }

    RetStatus ret = DSTORE_SUCC;
    if (needPrune && m_pagePayload.GetPage()->HasPrunableTuple()) {
        ret = Prune();
        maxOff = m_pagePayload.GetPage()->GetMaxOffset();
        minOff = m_pagePayload.GetLinkAndStatus()->GetFirstDataOffset();
    }
    if (m_isTupleLive == nullptr) {
        ret = ScanLiveTuples(true);
    }
    if (STORAGE_FUNC_FAIL(ret)) {
        return false;
    }

    if (maxOff < minOff) {
        /* empty page, no need to prune */
        return true;
    }
    bool hasRangeHolder = m_pagePayload.GetPage()->GetItemIdPtr(minOff)->IsRangePlaceholder();
    bool isEmpty = false;
    if (unlikely(hasRangeHolder)) {
        /* A page is empty if it has only RangeHolder & High key */
        isEmpty = (m_numLiveTuples == BTREE_PAGE_FIRSTKEY) &&
                  (m_isTupleLive[BTREE_PAGE_HIKEY]) && (m_isTupleLive[minOff]);
    } else {
        isEmpty = (m_numLiveTuples == 1) && (m_isTupleLive[BTREE_PAGE_HIKEY]);
    }
    if (!isEmpty) {
        return false;
    }

    return true;
}

BtreePageGPIPrune::BtreePageGPIPrune(StorageRelation indexRel, BufferDesc *pageBuf, IndexInfo *indexInfo,
    GPIPartOidCheckInfo *gpiCheckInfo)
    : BtreePagePrune(indexRel, indexInfo, nullptr, pageBuf)
{
    m_gpiCheckInfo = gpiCheckInfo;
}

bool BtreePageGPIPrune::IsPartVisible(IndexTuple *tuple)
{
    bool isNull;
    Oid partOid = DatumGetUInt32(tuple->GetAttr((m_indexInfo->tableOidAtt), m_indexInfo->attributes, &isNull));
    return !(m_gpiCheckInfo->hook(partOid, m_gpiCheckInfo->dropPartTree));
}

bool BtreePageGPIPrune::GPITupleIsPrunable(OffsetNumber offnum)
{
    if (!m_pagePayload.GetLinkAndStatus()->IsRightmost() && offnum == BTREE_PAGE_HIKEY) {
        /* We can never prune high key */
        return false;
    }

    ItemId *item = m_pagePayload.GetPage()->GetItemIdPtr(offnum);
    /* only deal with normal item, normal prune process to deal with others */
    if (!item->IsNormal()) {
        return false;
    }

    IndexTuple *tuple = m_pagePayload.GetPage()->GetIndexTuple(item);
    if (tuple->IsDeleted()) {
        return false;
    }

    if (IsPartVisible(tuple)) {
        return false;
    }

    if (tuple->TestTdStatus(DETACH_TD)) {
        return true;
    }

    /* Here only leaf page's tuples containing deleted tags are left. */
    TdId tdId = tuple->GetTdId();
    StorageAssert(tdId != INVALID_TD_SLOT);
    TD *td = m_pagePayload.GetPage()->GetTd(tdId);

    /*
     * if the td status is OCCUPY_TRX_IN_PROGRESS, the transaction status may be committed or aborted.
     * but if the transaction is in progress, the td status must be OCCUPY_TRX_IN_PROGRESS.
     */
    if (tuple->TestTdStatus(ATTACH_TD_AS_NEW_OWNER) && td->TestStatus(TDStatus::OCCUPY_TRX_IN_PROGRESS)) {
        XidStatus xs(td->GetXid(), thrd->GetActiveTransaction(), td);
        if (xs.IsInProgress()) {
            /* A page cannot be pruned if it contains any open transaction. */
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                ErrMsg("Btree page{%hu, %u} is unprunable since transaction on tuple (offnum = %hu) is in-progress.",
                       m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId, offnum));
            m_isPagePrunable = false;
            return false;
        }
    }

    /* Global temp table no need check csn. cannot return isPrunable(true) */
    if (GetBtreeSmgr()->IsGlobalTempIndex()) {
        return false;
    }

    bool isDirty = false;
    CommitSeqNo tupCsn = INVALID_CSN;
    BtrPage *page = m_pagePayload.GetPage();
    CommitSeqNo recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(m_pdbId);
    bool isPrunable =
        page->JudgeTupCommitBeforeSpecCsn<true>(m_pdbId, offnum, recycleMinCsn, isDirty, &tupCsn);
    if (isDirty) {
        UNUSED_VARIABLE(m_bufMgr->MarkDirty(GetPagePayload()->GetBuffDesc(), false));
    }

    return isPrunable;
}

RetStatus BtreePageGPIPrune::GPIPrune(bool &needReleaseBuf)
{
    StorageAssert(m_pagePayload.GetPage()->CheckSanity());

    if (STORAGE_VAR_NULL(thrd->GetActiveTransaction())) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr \"Transaction\" in current thread.", __FUNCTION__));
        return DSTORE_FAIL;
    }

    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    if (STORAGE_VAR_NULL(walContext)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Wal writer context is null in current thread.", __FUNCTION__));
        return DSTORE_FAIL;
    }

    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    if (STORAGE_VAR_NULL(csnMgr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("[%s] Unexpected nullptr \"CsnMgr\" in current instance.", __FUNCTION__));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Start to prune GPI Btree page {%hu, %u}", m_pagePayload.GetPageId().m_fileId,
                  m_pagePayload.GetPageId().m_blockId));

    /* Start to scan page from the first item. If page is rightmost of the level,
     * BTREE_PAGE_HIKEY will represent the first data offset. That also makes sence */
    OffsetNumber minOffset = BTREE_PAGE_HIKEY;
    OffsetNumber maxOffset = m_pagePayload.GetPage()->GetMaxOffset();
    m_numLiveTuples = 0;
    m_isTupleLive = static_cast<bool *>(DstorePalloc(sizeof(bool) * (maxOffset + 1)));
    if (unlikely(m_isTupleLive == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when GPIPrune."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }

    for (OffsetNumber offset = minOffset; offset <= maxOffset; offset = OffsetNumberNext(offset)) {
        if (GPITupleIsPrunable(offset)) {
            m_isTupleLive[offset] = false;
        } else {
            m_isTupleLive[offset] = true;
            m_numLiveTuples++;
        }

        if (!m_isPagePrunable) {
            return DSTORE_FAIL;
        }
    }

    if ((m_numLiveTuples == m_numTotalTuples) && !m_pagePayload.GetPage()->HasGarbageSpace()) {
        /* No need to prune if no any deletable tuple exists. */
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("No need to prune Btree page {%hu, %u} because neither TD space nor tuples are prunable.",
                      m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId));
        return DSTORE_SUCC;
    }

    /* No error is acceptable after we write the page!!! We must prepare memory and data before we do any change. */
    m_liveItems = static_cast<ItemIdCompact>(DstorePalloc(sizeof(ItemIdCompactData) * m_numLiveTuples));
    if (unlikely(m_liveItems == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc failed when alloc for ItemIdCompactData."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    uint32 redoDataSize = 0U;
    WalRecordBtreePagePrune *redoData = nullptr;
    if (NeedWal()) {
        uint32 tupleStatusSize = (m_numTotalTuples + 1) * static_cast<uint32>(sizeof(bool));
        redoDataSize = static_cast<uint32>(sizeof(WalRecordBtreePagePrune)) + tupleStatusSize;
        redoData = static_cast<WalRecordBtreePagePrune *>(DstorePalloc(redoDataSize));
        if (unlikely(redoData == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when GenerateWalForPrune."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            return DSTORE_FAIL;
        }
        Xid curXid = thrd->GetActiveTransaction()->GetCurrentXid();
        walContext->BeginAtomicWal(curXid);
    }
    CompactItems();
    CompactTuples();

    StorageAssert(m_pagePayload.GetPage()->CheckSanity());

    UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_pagePayload.GetBuffDesc()));

    /* Write Wal if needed */
    if (NeedWal()) {
        GenerateWalForPrune(redoData, redoDataSize);
        UNUSED_VARIABLE(walContext->EndAtomicWal());
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Succeed to prune Btree page {%hu, %u}, %hu tuples in total, %hu live",
                  m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId, m_numTotalTuples,
                  m_numLiveTuples));

    BtreePageRecycle recycle(m_indexRel);
    /* PutIntoRecycleQueueIfEmpty will release buffer, no need release outer. */
    needReleaseBuf = false;
    UNUSED_VARIABLE(recycle.PutIntoRecycleQueueIfEmpty(m_pagePayload.GetBuffDesc()));

    DstorePfreeExt(redoData);
    return DSTORE_SUCC;
}

}  // namespace DSTORE

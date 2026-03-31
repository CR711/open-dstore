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
 * dstore_heap_update.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/heap/dstore_heap_update.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "errorcode/dstore_transaction_error_code.h"
#include "errorcode/dstore_lock_error_code.h"
#include "framework/dstore_thread.h"
#include "common/memory/dstore_mctx.h"
#include "common/log/dstore_log.h"
#include "buffer/dstore_buf.h"
#include "common/dstore_common_utils.h"
#include "transaction/dstore_transaction_mgr.h"
#include "wal/dstore_wal_write_context.h"
#include "logical_replication/dstore_decode_dict.h"
#include "errorcode/dstore_heap_error_code.h"
#include "heap/dstore_heap_undo_struct.h"
#include "heap/dstore_heap_wal_struct.h"
#include "heap/dstore_heap_prune.h"
#include "heap/dstore_heap_perf_unit.h"
#include "heap/dstore_heap_update.h"
#include "page/dstore_heap_page.h"

namespace DSTORE {

RetStatus HeapUpdateHandler::Update(HeapUpdateContext *updateContext)
{
    LatencyStat::Timer timer(&HeapPerfUnit::GetInstance().m_heapUpdateLatency);

    RetStatus result = DSTORE_FAIL;
    StorageReleasePanic(updateContext->retOldTuple, MODULE_HEAP, ErrMsg("Old tuple in context should be null."));
    updateContext->newCtid = INVALID_ITEM_POINTER;
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    StorageAssert(m_thrd->GetActiveTransaction() != nullptr);
    if (STORAGE_FUNC_FAIL(m_thrd->GetActiveTransaction()->AllocTransactionSlot())) {
        updateContext->failureInfo.reason = HeapHandlerFailureReason::ALLOC_TRANS_SLOT_FAILED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to prepare undo when updating tuple({%hu, %u}, %hu).",
            updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(),
            updateContext->oldCtid.GetOffset()));
        return DSTORE_FAIL;
    }

UPDATESTART:
    if (unlikely(Init(updateContext->oldCtid))) {
        updateContext->failureInfo.reason = HeapHandlerFailureReason::READ_BUFFER_FAILED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to buffer desc when updating tuple({%hu, %u}, %hu).",
            updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(),
            updateContext->oldCtid.GetOffset()));
        return DSTORE_FAIL;
    }
    do {
        /* Failure info must be set inside the BeginUpdate. */
        if (STORAGE_FUNC_FAIL(BeginUpdate(updateContext))) {
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Begin update failed, ctid({%hu, %u}, %hu)",
                updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(),
                updateContext->oldCtid.GetOffset()));
            break;
        }

        if (STORAGE_FUNC_FAIL(DoUpdate(updateContext))) {
            updateContext->failureInfo.reason = HeapHandlerFailureReason::UPDATE_FAILED;
            ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Update failed, ctid({%hu, %u}, %hu)",
                updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(),
                updateContext->oldCtid.GetOffset()));
            break;
        }

        if (STORAGE_FUNC_FAIL(EndUpdate(updateContext))) {
            updateContext->failureInfo.reason = HeapHandlerFailureReason::UPDATE_FAILED;
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("End update failed, ctid({%hu, %u}, %hu)",
                updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(),
                updateContext->oldCtid.GetOffset()));
            break;
        }

        result = DSTORE_SUCC;
    } while (false);

    /* If oldTuple or newTuple is bigtuple, no need to retry alloctd, beacase we don't know where we are going,
     * which small tuple has been updated. Transaction excute fail and rollback directly.
     */
    if (STORAGE_FUNC_FAIL(result) && !m_isBigTuple && CanRetry(updateContext->failureInfo.reason)) {
        ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Update failed, retry alloctd, ctid({%hu, %u}, %hu)",
            updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(),
            updateContext->oldCtid.GetOffset()));
        goto UPDATESTART;
    }

    TryToUnlockReleaseBufferDesc();
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Update tuple, result: %d, old ctid({%hu, %u}, %hu"
        "new ctid({%hu, %u}, %hu), snapshot csn %lu", static_cast<int>(result),
        updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(), updateContext->oldCtid.GetOffset(),
        updateContext->newCtid.GetFileId(), updateContext->newCtid.GetBlockNum(), updateContext->newCtid.GetOffset(),
        thrd->GetSnapShotCsn()));
#endif
    return result;
}

RetStatus HeapUpdateHandler::ForceUpdateTupleDataNoTrx(HeapUpdateContext *updateContext, bool wait_flush)
{
    LatencyStat::Timer timer(&HeapPerfUnit::GetInstance().m_heapUpdateLatency);

    RetStatus result = DSTORE_FAIL;
    StorageReleasePanic(updateContext->retOldTuple, MODULE_HEAP, ErrMsg("Old tuple in context should be null."));
    updateContext->newCtid = INVALID_ITEM_POINTER;
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    if (unlikely(InitBufferDesc(updateContext->oldCtid))) {
        updateContext->failureInfo.reason = HeapHandlerFailureReason::READ_BUFFER_FAILED;
        return DSTORE_FAIL;
    }
    do {
        if (STORAGE_FUNC_FAIL(DoForceUpdateTupleDataNoTrx(updateContext, wait_flush))) {
            updateContext->failureInfo.reason = HeapHandlerFailureReason::UPDATE_FAILED;
            updateContext->newCtid = INVALID_ITEM_POINTER;
            ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Update failed, ctid({%hu, %u}, %hu)",
                updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(),
                updateContext->oldCtid.GetOffset()));
            break;
        }

        result = DSTORE_SUCC;
    } while (false);

    TryToUnlockReleaseBufferDesc();
    return result;
}

RetStatus HeapUpdateHandler::Init(ItemPointerData ctid)
{
    m_tdcontext.Init(m_heapRel->m_pdbId, NeedWal());
    m_tupIsSplited = false;
    m_tupChunks.m_chunkNum = 0;
    return InitBufferDesc(ctid);
}

RetStatus HeapUpdateHandler::BeginUpdate(HeapUpdateContext *updateContext)
{
    updateContext->newCtid = INVALID_ITEM_POINTER;
    CommandId tupleCid = INVALID_CID;
    bool tupleChanged = false;
    RetStatus ret = CheckTupleChanged(updateContext->oldCtid, tupleChanged, tupleCid, &updateContext->snapshot,
        updateContext->executedEpq);
    if (STORAGE_FUNC_FAIL(ret)) {
        if (StorageGetErrorCode() == TRANSACTION_INFO_SAME_THREAD_DEADLOCK ||
            StorageGetErrorCode() == LOCK_ERROR_DEADLOCK) {
            updateContext->failureInfo.reason = HeapHandlerFailureReason::DEADLOCK;
        } else if (StorageGetErrorCode() == LOCK_ERROR_WAIT_TIMEOUT) {
            updateContext->failureInfo.reason = HeapHandlerFailureReason::LOCK_WAIT_TIMEOUT;
        } else if (StorageGetErrorCode() == LOCK_ERROR_WAIT_CANCELED) {
            updateContext->failureInfo.reason = HeapHandlerFailureReason::LOCK_WAIT_CANCELED;
        } else if (StorageGetErrorCode() == HEAP_ERROR_ROLLBACK_BIG_TUPLE_FAILED) {
            updateContext->failureInfo.reason = HeapHandlerFailureReason::ROLLBACK_BIG_TUPLE_FAILED;
        } else {
            updateContext->failureInfo.reason = HeapHandlerFailureReason::CHECK_TUPLE_CHANGED_FAILED;
        }
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Check tuple changed fail when update, ctid({%hu, %u}, %hu)",
            updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(),
            updateContext->oldCtid.GetOffset()));
        return DSTORE_FAIL;
    }

    if (tupleChanged) {
        if (STORAGE_FUNC_FAIL(SetFailureInfo(updateContext->failureInfo, updateContext->oldCtid, tupleCid,
                                            &updateContext->snapshot))) {
            return DSTORE_FAIL;
        }
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Tuple is changed when begin update, ctid({%hu, %u}, %hu)",
        updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(),
        updateContext->oldCtid.GetOffset()));
#endif
        storage_set_error(HEAP_ERROR_TUPLE_IS_CHANGED);
        return DSTORE_FAIL;
    }

    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    if (unlikely(page->IsDiskTupleDeleted(updateContext->oldCtid.GetOffset()))) {
        updateContext->failureInfo.reason = HeapHandlerFailureReason::DELETED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Tuple is deleted when begin update, ctid({%hu, %u}, %hu)",
            updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(),
            updateContext->oldCtid.GetOffset()));
        storage_set_error(HEAP_ERROR_TUPLE_IS_DELETED);
        return DSTORE_FAIL;
    }

    if (unlikely(updateContext->newTuple == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("New tuple is nullptr when begin update, ctid({%hu, %u}, %hu)",
            updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(),
            updateContext->oldCtid.GetOffset()));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }

    m_isBigTuple = page->GetDiskTuple(updateContext->oldCtid.GetOffset())->IsLinked() ||
        HeapPage::TupBiggerThanPage(updateContext->newTuple);

    return DSTORE_SUCC;
}

RetStatus HeapUpdateHandler::UpdateSmallTuple(HeapUpdateContext *updateContext)
{
    HeapTuple *newTuple = nullptr;
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    OffsetNumber offset = updateContext->oldCtid.GetOffset();
    newTuple = updateContext->newTuple;
    ItemId *itemId = page->GetItemIdPtr(offset);
    uint32 itemLen = itemId->GetLen();
    uint32 newTupleSize = newTuple->GetDiskTupleSize();
    RetStatus result = DSTORE_FAIL;
    m_tdcontext.Begin(m_heapRel->m_pdbId, m_instance->GetCsnMgr()->GetRecycleCsnMin(m_heapRel->m_pdbId));
    /* Do not wait for an available TD slot if there is none readily available */
    TdId tdId = page->AllocTd(m_tdcontext);
    if (likely(tdId != INVALID_TD_SLOT)) {
        /* There is an available TD slot that we can use */
        if (likely(newTupleSize <= itemLen)) {
            /* Since new Tuple smaller than old Tuple, do in-place update */
            result = UpdateSmallTupleInplace(updateContext, page, tdId);
        } else if (page->GetFreeSpace<FreeSpaceCondition::RAW>() >= newTupleSize) {
            /* Since we have room to put a new tuple, do same-page update */
            result = UpdateSmallTupleSamePage(updateContext, page, tdId);
        } else {
            if (m_tdcontext.allocTd.isDirty) {
                (void)m_bufMgr->MarkDirty(m_bufferDesc);
                GenerateAllocTdWal(m_bufferDesc);
            }
            /* Try to prune the page and see if more space can be available for same-page update */
            HeapPruneHandler prune(m_instance, m_heapRel, m_thrd, m_bufferDesc, m_isLob);
            prune.TryPrunePage(static_cast<uint16>(newTupleSize), !m_tdcontext.allocTd.isDirty);

            if (page->GetFreeSpace<FreeSpaceCondition::RAW>() >= newTupleSize) {
                /* We have room to put a new tuple, do same-page update */
                result = UpdateSmallTupleSamePage(updateContext, page, tdId);
            } else {
                /* Do another-page update */
                result = UpdateSmallTupleAnotherPage(updateContext, tdId);
            }
        }
    }

    return result;
}

RetStatus HeapUpdateHandler::DoUpdate(HeapUpdateContext *updateContext)
{
    HeapTuple *newTuple = nullptr;
    HeapTuple oldTuple;
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    OffsetNumber offset = updateContext->oldCtid.GetOffset();

    page->GetTuple(&oldTuple, offset);
    newTuple = updateContext->newTuple;

    /* Step 1. Assign oid of old tuple to new tuple if any */
    if (unlikely(oldTuple.HasOid())) {
        newTuple->SetOid(oldTuple.GetOid());
    }

    /* Check if the transaction status is pending_commit or In_progress, if so, panic using storage_relasse_panic */
    Xid oldTupleXid = oldTuple.GetXid();
    StorageReleasePanic(oldTupleXid == INVALID_XID, MODULE_HEAP, ErrMsg("The tuple xid is invalid."));

    /* Step 2. Do the real update here */
    RetStatus result;
    if (likely(!oldTuple.m_diskTuple->IsLinked() && !HeapPage::TupBiggerThanPage(newTuple))) {
        result = UpdateSmallTuple(updateContext);
    } else {
        ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Update big tuple({%hu, %u}, %hu).",
            updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(), offset));
        result = UpdateBigTuple(updateContext);
    }

    if (unlikely(result == DSTORE_FAIL)) {
        updateContext->newCtid = INVALID_ITEM_POINTER;
    }

    StorageAssert(
        (result == DSTORE_FAIL) || (updateContext->needReturnOldTup && updateContext->retOldTuple != nullptr) ||
        (!updateContext->needReturnOldTup && updateContext->needUpdateLob && updateContext->retOldTuple != nullptr) ||
        (!updateContext->needReturnOldTup && !updateContext->needUpdateLob && updateContext->retOldTuple == nullptr));

    return result;
}

RetStatus HeapUpdateHandler::UpdateBigTuple(HeapUpdateContext *updateContext)
{
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    HeapTuple *newTuple = nullptr;
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    OffsetNumber offset = updateContext->oldCtid.GetOffset();
    newTuple = updateContext->newTuple;
    HeapDiskTuple *oldDiskTup = page->GetDiskTuple(offset);
    uint32 oldDiskTupChunksNum = oldDiskTup->GetNumChunks();
    HeapTuple **oldTupChunks = nullptr;
    StorageAssert(oldDiskTup->IsLinked() || HeapPage::TupBiggerThanPage(newTuple));

    /* Step 1: always Lock first tuple chunk to avoid deadlock */
    HeapLockTupleContext lockTupContext;
    lockTupContext.ctid = updateContext->oldCtid;
    TdId tdId = INVALID_TD_SLOT;
    if (STORAGE_FUNC_FAIL(DoLock(&lockTupContext, tdId))) {
        ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Lock failed when update big tuple, ctid({%hu, %u}, %hu)",
            updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(),
            updateContext->oldCtid.GetOffset()));
        return DSTORE_FAIL;
    }

    TryToUnlockReleaseBufferDesc();

    /* Prepare memory for store old tuple chunks if it is linked tuple */
    if ((updateContext->needReturnOldTup || updateContext->needUpdateLob) && oldDiskTup->IsLinked()) {
        oldTupChunks = static_cast<HeapTuple **>(DstorePalloc0(sizeof(HeapTuple *) * oldDiskTupChunksNum));
        if (unlikely(oldTupChunks == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc0 fail size(%u) when update big tuple.",
                static_cast<uint32>(sizeof(HeapTuple *) * oldDiskTupChunksNum)));
            return DSTORE_FAIL;
        }
    }

    /* Step 2: Do big tuple update */
    if (HeapPage::TupBiggerThanPage(newTuple)) {
        RetStatus ret = SplitTupIntoChunks(newTuple);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("split tup into chunks fail when update big tuple."));
            return DSTORE_FAIL;
        }
        if (oldDiskTupChunksNum <= m_tupChunks.m_chunkNum) {
            if (STORAGE_FUNC_FAIL(UpdateBigTupSizeBigger(updateContext, oldTupChunks))) {
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Update big tup bigger fail."));
                return DSTORE_FAIL;
            }
        } else {
            if (STORAGE_FUNC_FAIL(UpdateBigTupSizeSmaller(updateContext, oldTupChunks))) {
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Update big tup smaller fail."));
                return DSTORE_FAIL;
            }
        }
    } else {
        StorageAssert(oldDiskTup->IsLinked());
        if (STORAGE_FUNC_FAIL(UpdateBigTupSizeSmaller(updateContext, oldTupChunks))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Update big tup to one small tuple fail."));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus HeapUpdateHandler::UpdateBigTupSizeSmaller(HeapUpdateContext *updateContext, HeapTuple **oldTupChunks)
{
    UNUSE_PARAM uint32 j = 0;
    uint32 numOldTupChunk = 0;
    ItemPointerData otid = updateContext->oldCtid;
    OffsetNumber offset = otid.GetOffset();
    m_bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, otid.GetPageId(), LW_EXCLUSIVE);
    if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Read page(%hu, %u) failed when update big tuple size smaller.",
            otid.GetPageId().m_fileId, otid.GetPageId().m_blockId));
        return DSTORE_FAIL;
    }
    CheckBufferedPage(m_bufferDesc->GetPage(), otid.GetPageId());
    HeapDiskTuple *oldTupChunk = static_cast<HeapPage *>(m_bufferDesc->GetPage())->GetDiskTuple(offset);
    StorageAssert(oldTupChunk->IsLinked());

    ItemPointerData nextChunkCtid = oldTupChunk->GetNextChunkCtid();
    uint32 oldDiskTupChunksNum = oldTupChunk->GetNumChunks();
    StorageAssert(oldDiskTupChunksNum > m_tupChunks.m_chunkNum);
    uint32 fromPos = static_cast<uint32>(m_tupIsSplited ? m_tupChunks.m_chunkNum : 1);
    numOldTupChunk = oldTupChunk->GetNumChunks();

    /* Unlock buffer in order to avoid holding multiple buffer lock */
    TryToUnlockReleaseBufferDesc();

    /* Step 1: Delete the extra tuple chunks of old tuple */
    HeapDeleteContext heapDeleteChunkContext;
    heapDeleteChunkContext.snapshot = updateContext->snapshot;
    heapDeleteChunkContext.cid = updateContext->cid;
    for (uint32 i = 1; i < oldDiskTupChunksNum; ++i) {
        AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));
        heapDeleteChunkContext.ctid = nextChunkCtid;
        heapDeleteChunkContext.needReturnTup = updateContext->needReturnOldTup;
        heapDeleteChunkContext.needDeleteLob = updateContext->needUpdateLob;

        m_bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, nextChunkCtid.GetPageId(), LW_EXCLUSIVE);
        if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                   ErrMsg("Read next chunk(%hu, %u) failed when update big tuple size smaller.",
                          nextChunkCtid.GetPageId().m_fileId, nextChunkCtid.GetPageId().m_blockId));
            return DSTORE_FAIL;
        }
        CheckBufferedPage(m_bufferDesc->GetPage(), nextChunkCtid.GetPageId());
        oldTupChunk = static_cast<HeapPage *>(m_bufferDesc->GetPage())->GetDiskTuple(nextChunkCtid.GetOffset());
        nextChunkCtid = oldTupChunk->GetNextChunkCtid();
        if (i >= fromPos) {
            RetStatus ret = DeleteSmallTuple(&heapDeleteChunkContext);
            StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_HEAP, ErrMsg("Delete small tuple failed, "
                "ctid{(%hu, %u), %hu}.", heapDeleteChunkContext.ctid.GetFileId(),
                heapDeleteChunkContext.ctid.GetBlockNum(), heapDeleteChunkContext.ctid.GetOffset()));
            if (updateContext->needReturnOldTup || updateContext->needUpdateLob) {
                StorageAssert(oldTupChunks != nullptr);
                oldTupChunks[i] = heapDeleteChunkContext.returnTup;
                j++;
            }
        }
        TryToUnlockReleaseBufferDesc();
    }
    /* Step 2: Update the old tuple chunks as need */
    StorageAssert(nextChunkCtid == INVALID_ITEM_POINTER);
    /* New tuple is splited */
    if (m_tupIsSplited) {
        RetStatus ret = UpdateOldTupChunks(updateContext, INVALID_ITEM_POINTER,
            static_cast<uint32>(m_tupChunks.m_chunkNum), oldTupChunks);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Update old tupchunks failed, chunknum(%u).",
                m_tupChunks.m_chunkNum));
            return DSTORE_FAIL;
        }
        j += static_cast<uint32>(m_tupChunks.m_chunkNum);
    } else {
        m_bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, otid.GetPageId(), LW_EXCLUSIVE);
        if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                ErrMsg("Read page(%hu, %u) failed when update big tuple size smaller.",
                otid.GetPageId().m_fileId, otid.GetPageId().m_blockId));
            return DSTORE_FAIL;
        }
        CheckBufferedPage(m_bufferDesc->GetPage(), otid.GetPageId());
        /* New tuple is small, so directly update the first tuple chunk of old tuple */
        RetStatus ret = UpdateSmallTuple(updateContext);
        if (STORAGE_FUNC_FAIL(ret)) {
            TryToUnlockReleaseBufferDesc();
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Update small tuple failed when update big tuple size smaller."));
            return DSTORE_FAIL;
        }
        if (updateContext->needReturnOldTup || updateContext->needUpdateLob) {
            StorageAssert(oldTupChunks != nullptr);
            StorageAssert(oldTupChunks[0] == nullptr);
            oldTupChunks[0] = updateContext->retOldTuple;
            j++;
        }
        TryToUnlockReleaseBufferDesc();
    }
    /* Step 3: Return old tuple if need */
    if (updateContext->needReturnOldTup || updateContext->needUpdateLob) {
        StorageAssert(j == numOldTupChunk);
        updateContext->retOldTuple = AssembleTuples(oldTupChunks, static_cast<uint32>(numOldTupChunk));
        StorageAssert(oldTupChunks != nullptr);
        for (uint32 i = 0; i < numOldTupChunk; i++) {
            StorageAssert(oldTupChunks[i]);
            DstorePfree(oldTupChunks[i]);
        }
        if (unlikely(updateContext->retOldTuple == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Assemble tuples fail when update big tuple size smaller."));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus HeapUpdateHandler::UpdateBigTupSizeBigger(HeapUpdateContext *updateContext, HeapTuple **oldTupChunks)
{
    HeapDiskTuple *tupChunk = nullptr;
    ItemPointerData otid = updateContext->oldCtid;
    m_bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, otid.GetPageId(), LW_EXCLUSIVE);
    if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Read page(%hu, %u) failed when update big tuple size bigger.",
            otid.GetPageId().m_fileId, otid.GetPageId().m_blockId));
        return DSTORE_FAIL;
    }
    CheckBufferedPage(m_bufferDesc->GetPage(), otid.GetPageId());
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    OffsetNumber offset = otid.GetOffset();
    bool oldTupIsLinked = page->GetDiskTuple(offset)->IsLinked();
    ItemPointerData newTupNextChunkCtid = INVALID_ITEM_POINTER;
    uint32 oldDiskTupChunksNum = page->GetDiskTuple(offset)->GetNumChunks();
    StorageAssert(oldDiskTupChunksNum > 0);
    /* Unlock buffer in order to avoid holding multiple buffer lock */
    TryToUnlockReleaseBufferDesc();
    /*
     * Step 1: First we must insert the extra tuple chunks, because we need store the
     * next ctid in the tuple chunk.
     */
    for (uint32 i = m_tupChunks.m_chunkNum - 1; i > oldDiskTupChunksNum - 1; --i) {
        HeapInsertContext insertContext;
        insertContext.cid = updateContext->cid;
        tupChunk = m_tupChunks.m_chunksData[i];
        tupChunk->SetNextChunkCtid(newTupNextChunkCtid);

        if (STORAGE_FUNC_FAIL(InsertSmallDiskTup(&insertContext, tupChunk, m_tupChunks.m_chunksSize[i]))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Update big tuple insert fail, tuplesize(%hu)",
                tupChunk->GetTupleSize()));
            return DSTORE_FAIL;
        }
        newTupNextChunkCtid = insertContext.ctid;
    }
    m_tupChunks.m_chunksData[oldDiskTupChunksNum - 1]->SetNextChunkCtid(newTupNextChunkCtid);

    /*
     * Step 2: Update old tuple using new tuple chunks.
     */
    if (oldTupIsLinked) {
        /* Old tuple is big */
        RetStatus ret = UpdateOldTupChunks(updateContext, newTupNextChunkCtid, oldDiskTupChunksNum, oldTupChunks);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Update old tupchunks failed, chunknum(%hhu).",
                oldDiskTupChunksNum));
            return DSTORE_FAIL;
        }
        if (updateContext->needReturnOldTup || updateContext->needUpdateLob) {
            updateContext->retOldTuple = AssembleTuples(oldTupChunks, oldDiskTupChunksNum);
            StorageAssert(oldTupChunks != nullptr);
            for (uint32 i = 0; i < oldDiskTupChunksNum; i++) {
                DstorePfree(oldTupChunks[i]);
            }
            if (unlikely(updateContext->retOldTuple == nullptr)) {
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Assemble tuples fail when update big tuple size bigger."));
                return DSTORE_FAIL;
            }
        }
    } else {
        /* Old tuple is small */
        HeapUpdateContext tupChunkUpdateContext;
        tupChunk = m_tupChunks.m_chunksData[0];
        tupChunkUpdateContext.newTuple = static_cast<HeapTuple*>(DstorePalloc(sizeof(HeapTuple)));
        if (unlikely(tupChunkUpdateContext.newTuple == nullptr)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%u) when update big tuple size bigger.",
                static_cast<uint32>(sizeof(HeapTuple))));
            return DSTORE_FAIL;
        }
        tupChunkUpdateContext.newTuple->SetDiskTuple(tupChunk);
        tupChunkUpdateContext.newTuple->SetDiskTupleSize(m_tupChunks.m_chunksSize[0]);
        tupChunkUpdateContext.oldCtid = otid;
        tupChunkUpdateContext.needReturnOldTup = updateContext->needReturnOldTup;
        tupChunkUpdateContext.needUpdateLob = updateContext->needUpdateLob;
        tupChunkUpdateContext.snapshot = updateContext->snapshot;
        tupChunkUpdateContext.cid = updateContext->cid;

        m_bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, tupChunkUpdateContext.oldCtid.GetPageId(), LW_EXCLUSIVE);
        if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Read page(%hu, %u) failed when update big tuple size bigger.",
                tupChunkUpdateContext.oldCtid.GetPageId().m_fileId,
                tupChunkUpdateContext.oldCtid.GetPageId().m_blockId));
            return DSTORE_FAIL;
        }
        CheckBufferedPage(m_bufferDesc->GetPage(), tupChunkUpdateContext.oldCtid.GetPageId());
        RetStatus ret = UpdateSmallTuple(&tupChunkUpdateContext);
        if (STORAGE_FUNC_FAIL(ret)) {
            TryToUnlockReleaseBufferDesc();
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                ErrMsg("Update small tuple((%hu, %u), %u) failed when update big tuple size bigger.",
                tupChunkUpdateContext.oldCtid.GetFileId(), tupChunkUpdateContext.oldCtid.GetBlockNum(),
                tupChunkUpdateContext.oldCtid.GetOffset()));
            return DSTORE_FAIL;
        }
        TryToUnlockReleaseBufferDesc();
        /* Set the new ctid of first tuple chunk */
        updateContext->newCtid = tupChunkUpdateContext.newCtid;

        if (updateContext->newCtid != otid) {
            updateContext->needReturnOldTup = true;
        }

        if (updateContext->needReturnOldTup || updateContext->needUpdateLob) {
            updateContext->retOldTuple = tupChunkUpdateContext.retOldTuple;
        }
        DstorePfreeExt(tupChunkUpdateContext.newTuple);
    }
    return DSTORE_SUCC;
}

RetStatus HeapUpdateHandler::GetBigTupleChunkCtidsInterval(ItemPointerData *oldTupNextChunkCtids, uint32 oldChunkNum)
{
    HeapDiskTuple *oldDiskTup = nullptr;

    StorageAssert(oldTupNextChunkCtids[0] != INVALID_ITEM_POINTER);
    for (uint32 chunkIdx = 1; chunkIdx < oldChunkNum; ++chunkIdx) {
        ItemPointerData curChunkCtid = oldTupNextChunkCtids[chunkIdx - 1];
        m_bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, curChunkCtid.GetPageId(), LW_SHARED);
        if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Read page(%hu, %u) failed when update old tuple chunks.",
                curChunkCtid.GetPageId().m_fileId,
                curChunkCtid.GetPageId().m_blockId));
            return DSTORE_FAIL;
        }
        CheckBufferedPage(m_bufferDesc->GetPage(), oldTupNextChunkCtids[chunkIdx - 1].GetPageId());
        oldDiskTup = static_cast<HeapPage *>(m_bufferDesc->GetPage())->GetDiskTuple(curChunkCtid.GetOffset());
        oldTupNextChunkCtids[chunkIdx] = oldDiskTup->GetNextChunkCtid();
        TryToUnlockReleaseBufferDesc();
    }
    return DSTORE_SUCC;
}

RetStatus HeapUpdateHandler::UpdateOldTupChunks(HeapUpdateContext *updateContext, ItemPointerData newTupNextChunkCtid,
                                                uint32 oldUpdateChunkNum, HeapTuple **oldTupChunks)
{
    HeapDiskTuple *curTupChunk = nullptr;
    HeapTuple tmpHeapTup;
    HeapUpdateContext tupChunkUpdateContext;
    tupChunkUpdateContext.newTuple = &tmpHeapTup;
    StorageAssert(m_tupIsSplited);

    ItemPointerData *oldTupNextChunkCtids =
        static_cast<ItemPointerData*>(DstorePalloc(oldUpdateChunkNum * sizeof(ItemPointerData)));
    ItemPointerData *newTupChunkCtids = static_cast<ItemPointerData*>(DstorePalloc(
        static_cast<uint32>(m_tupChunks.m_chunkNum + 1) * sizeof(ItemPointerData)));
    if (unlikely(oldTupNextChunkCtids == nullptr || newTupChunkCtids == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size1(%u), size(%u) when update old tuple chunks.",
            static_cast<uint32>(oldUpdateChunkNum * sizeof(ItemPointerData)),
            static_cast<uint32>(static_cast<uint32>(m_tupChunks.m_chunkNum + 1) * sizeof(ItemPointerData))));
        if (oldTupNextChunkCtids != nullptr) {
            DstorePfree(oldTupNextChunkCtids);
        }
        if (newTupChunkCtids != nullptr) {
            DstorePfree(newTupChunkCtids);
        }
        return DSTORE_FAIL;
    }

    newTupChunkCtids[oldUpdateChunkNum] = newTupNextChunkCtid;
    m_tupChunks.m_chunksData[oldUpdateChunkNum - 1]->SetNextChunkCtid(newTupNextChunkCtid);
    oldTupNextChunkCtids[0] = updateContext->oldCtid;

    /* Step 1: Get chunk list. */
    if (STORAGE_FUNC_FAIL(GetBigTupleChunkCtidsInterval(oldTupNextChunkCtids, oldUpdateChunkNum))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Get big tuple chunk list failed."));
        DstorePfree(oldTupNextChunkCtids);
        DstorePfree(newTupChunkCtids);
        return DSTORE_FAIL;
    }

    /* Step 2: Update old tuple chunks reverse. */
    for (int32 i = oldUpdateChunkNum - 1; i >= 0; --i) {
        AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));
        curTupChunk = m_tupChunks.m_chunksData[i];
        curTupChunk->SetNextChunkCtid(newTupChunkCtids[i + 1]);
        ItemPointerData curChunkCtid = oldTupNextChunkCtids[i];
        tupChunkUpdateContext.newTuple->SetDiskTuple(curTupChunk);
        tupChunkUpdateContext.newTuple->SetDiskTupleSize(m_tupChunks.m_chunksSize[i]);
        tupChunkUpdateContext.oldCtid = curChunkCtid;
        tupChunkUpdateContext.needReturnOldTup = updateContext->needReturnOldTup;
        tupChunkUpdateContext.needUpdateLob = updateContext->needUpdateLob;
        tupChunkUpdateContext.snapshot = updateContext->snapshot;
        tupChunkUpdateContext.cid = updateContext->cid;

        StorageAssert(((oldTupNextChunkCtids[i] != INVALID_ITEM_POINTER) ||
            (i == (int32)oldUpdateChunkNum - 1 && oldTupNextChunkCtids[i] == INVALID_ITEM_POINTER)));
        m_bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, tupChunkUpdateContext.oldCtid.GetPageId(), LW_EXCLUSIVE);
        if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Read page(%hu, %u) failed when update old tuple chunks.",
                tupChunkUpdateContext.oldCtid.GetPageId().m_fileId,
                tupChunkUpdateContext.oldCtid.GetPageId().m_blockId));
            DstorePfree(oldTupNextChunkCtids);
            DstorePfree(newTupChunkCtids);
            return DSTORE_FAIL;
        }
        CheckBufferedPage(m_bufferDesc->GetPage(), tupChunkUpdateContext.oldCtid.GetPageId());
        RetStatus ret = UpdateSmallTuple(&tupChunkUpdateContext);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Update small tuple failed when update old tuple chunks."));
            DstorePfree(oldTupNextChunkCtids);
            DstorePfree(newTupChunkCtids);
            return DSTORE_FAIL;
        }
        newTupChunkCtids[i] = tupChunkUpdateContext.newCtid;
        if (updateContext->needReturnOldTup || updateContext->needUpdateLob) {
            StorageAssert(oldTupChunks != nullptr);
            oldTupChunks[i] = tupChunkUpdateContext.retOldTuple;
        }

        TryToUnlockReleaseBufferDesc();
    }

    /* Step 3: Set the new first chunk ctid in updateContext */
    updateContext->newCtid = newTupChunkCtids[0];
    DstorePfree(oldTupNextChunkCtids);
    DstorePfree(newTupChunkCtids);
    return DSTORE_SUCC;
}

RetStatus HeapUpdateHandler::EndUpdate(__attribute__((__unused__)) HeapUpdateContext *updateContext) const
{
    return DSTORE_SUCC;
}

RetStatus HeapUpdateHandler::UpdateSmallTupleInplace(HeapUpdateContext *updateContext, HeapPage *page, TdId tdId)
{
    HeapTuple oldTuple;
    OffsetNumber offset = updateContext->oldCtid.GetOffset();
    HeapTuple *newTuple = updateContext->newTuple;
    Transaction *transaction = m_thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    StoragePdb *pdb = m_instance->GetPdb(m_heapRel->m_pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Pdb is null."));
        return DSTORE_FAIL;
    }
    TransactionMgr *txnMgr = pdb->GetTransactionMgr();
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    page->GetTuple(&oldTuple, offset);
    char *oldTuplePtr = oldTuple.GetDiskTuple()->GetData();
    Xid xid = transaction->GetCurrentXid();

    StorageReleasePanic(tdId == INVALID_TD_SLOT, MODULE_HEAP, ErrMsg("Tdid invalid."));
    /* Step 1: remove the locker td id */
    oldTuple.GetDiskTuple()->SetLockerTdId(INVALID_TD_SLOT);

    /* Need return old tuple if the data of index column is changed */
    if (unlikely(updateContext->needReturnOldTup || updateContext->needUpdateLob || WalLogicalActive())) {
        updateContext->retOldTuple = page->CopyTuple(offset);
        if (unlikely(updateContext->retOldTuple == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when update small tuple inplace."));
            m_bufMgr->UnlockAndRelease(m_bufferDesc);
            m_bufferDesc = nullptr;
            return DSTORE_FAIL;
        }
    }

    /* Step 2: Get diff between old and new tuples */
    GetDiffBetweenTuples(&oldTuple, newTuple);

    /* Step 3: Generate undo record */
    UndoType type = GetTableSmgr()->IsGlobalTempTable() ? UNDO_HEAP_INPLACE_UPDATE_TMP : UNDO_HEAP_INPLACE_UPDATE;
    UndoRecord undoRecord(type, tdId, page->GetTd(tdId), updateContext->oldCtid, updateContext->cid);
    uint16 diffSizeForUndo =
        GetTupleDiffSize(static_cast<uint16>(oldTuple.GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE));
    uint16 undoDataRecSize = static_cast<uint16>(sizeof(UndoDataHeapInplaceUpdate) + diffSizeForUndo);
    UndoDataHeapInplaceUpdate *undoData = static_cast<UndoDataHeapInplaceUpdate *>(DstorePalloc(undoDataRecSize));
    if (unlikely(undoData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%hu).", undoDataRecSize));
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        m_bufferDesc = nullptr;
        return DSTORE_FAIL;
    }
    undoData->Init();
    undoData->SetOldTupleSize(static_cast<uint16>(oldTuple.GetDiskTupleSize()));
    undoData->GenerateUndoData(m_tupDiffNum, m_tupDiffPos, oldTuplePtr, oldTuple.GetTdId(),
                               oldTuple.GetDiskTuple()->GetInfo(), oldTuple.GetDiskTuple()->GetXid());
    StorageAssert(undoDataRecSize >= undoData->GetSize());

    RetStatus ret = undoRecord.Append(static_cast<char *>(static_cast<void *>(undoData)), undoData->GetSize());
    if (STORAGE_FUNC_FAIL(ret)) {
        if (m_tdcontext.allocTd.isDirty) {
            (void)m_bufMgr->MarkDirty(m_bufferDesc);
            GenerateAllocTdWal(m_bufferDesc);
        }
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("Append undo record fail(%u) when inplace update.", undoData->GetSize()));
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        m_bufferDesc = nullptr;
        DstorePfree(undoData);
        return DSTORE_FAIL;
    }
    DstorePfree(undoData);
    /* if extend fail, we have done alloctd, so we need record alloctd wal */
    if (STORAGE_FUNC_FAIL(txnMgr->ExtendUndoSpaceIfNeeded(xid, undoRecord.GetRecordSize()))) {
        if (m_tdcontext.allocTd.isDirty) {
            (void)m_bufMgr->MarkDirty(m_bufferDesc);
            GenerateAllocTdWal(m_bufferDesc);
        }
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Extend undo space fail when inplace update."));
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        m_bufferDesc = nullptr;
        return DSTORE_FAIL;
    }

    /* Step 4: Inplace update tuple */
    StorageAssert(tdId < page->GetTdCount());
    newTuple->m_diskTuple->SetTdId(tdId);
    newTuple->m_diskTuple->SetXid(xid);
    newTuple->m_diskTuple->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    newTuple->m_diskTuple->SetLockerTdId(INVALID_TD_SLOT);
    newTuple->m_diskTuple->SetLiveMode(HeapDiskTupLiveMode::NEW_TUPLE_BY_INPLACE_UPDATE);
    newTuple->m_diskTuple->SetTupleSize(static_cast<uint16>(newTuple->GetDiskTupleSize()));
    page->UpdateTuple(offset, newTuple->GetDiskTuple(), newTuple->GetDiskTupleSize());
    page->SetTuplePrunable(true);
    (void)m_bufMgr->MarkDirty(m_bufferDesc);

    /* Step 5: Append undo record to undo page and insert redo log */
    walContext->BeginAtomicWal(xid);
    UndoRecPtr undoRecPtr = InsertUndoAndCheck(&undoRecord);
    if (unlikely(undoRecPtr == INVALID_UNDO_RECORD_PTR)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Insert undo record fail when update small tuple inplace."));
        walContext->ResetForAbort();
        return DSTORE_FAIL;
    }
    page->SetTd(tdId, xid, undoRecPtr, updateContext->cid);

    if (likely(NeedWal())) {
        StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_HEAP,
                            ErrMsg("Wal has not atomic begin."));
        walContext->RememberPageNeedWal(m_bufferDesc);
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        if (updateContext->replicaKeyAttrs == nullptr || BmsIsEmpty(updateContext->replicaKeyAttrs)) {
            GenerateInplaceUpdateWal(m_bufferDesc, newTuple->GetDiskTuple(), nullptr,
                                     {offset, undoRecPtr, tdId, static_cast<uint16>(newTuple->GetDiskTupleSize()),
                                      newTuple->GetDiskTuple()->m_info.m_info},
                                     updateContext->cid);
        } else {
            /* inplace-update needs store whole old-disk tuple (for logical decode). if inplace-update changed */
            GenerateInplaceUpdateWal(m_bufferDesc, newTuple->GetDiskTuple(), updateContext->retOldTuple->GetDiskTuple(),
                                     {offset, undoRecPtr, tdId, static_cast<uint16>(newTuple->GetDiskTupleSize()),
                                      newTuple->GetDiskTuple()->m_info.m_info},
                                     updateContext->cid);
        }
    } else {
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
    }

    if (!updateContext->needReturnOldTup && !updateContext->needUpdateLob && WalLogicalActive()) {
        DstorePfreeExt(updateContext->retOldTuple);
    }
    (void)walContext->EndAtomicWal();
    m_bufferDesc = nullptr;
    updateContext->newCtid = updateContext->oldCtid;
    return DSTORE_SUCC;
}

RetStatus HeapUpdateHandler::DoForceUpdateTupleDataNoTrx(HeapUpdateContext *updateContext, bool wait_flush)
{
    HeapTuple oldTuple;
    HeapTuple *newTuple = updateContext->newTuple;
    OffsetNumber offset = updateContext->oldCtid.GetOffset();
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    page->GetTuple(&oldTuple, offset);
    char *oldTuplePtr = oldTuple.GetDiskTuple()->GetData();
    char *newTuplePtr = newTuple->GetDiskTuple()->GetData();

    uint32 oldLen = oldTuple.GetDiskTupleSize() - oldTuple.GetDiskTuple()->GetHeaderSize();
    uint32 newLen = newTuple->GetDiskTupleSize() - newTuple->GetDiskTuple()->GetHeaderSize();
    if (oldLen != newLen || oldTuple.GetDiskTuple()->GetHeaderSize() != newTuple->GetDiskTuple()->GetHeaderSize()) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
            ErrMsg("Force update tuple data no trx: wrong tuple length. oldLen(%u), newLen(%u), old header size(%u), "
                   "new header size(%u).",
            oldLen, newLen, oldTuple.GetDiskTuple()->GetHeaderSize(), newTuple->GetDiskTuple()->GetHeaderSize()));
        return DSTORE_FAIL;
    }

    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;

    /* Need return old tuple for invalidation. */
    if (updateContext->needReturnOldTup) {
        updateContext->retOldTuple = page->CopyTuple(offset);
        if (unlikely(updateContext->retOldTuple == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when do force update tuple data."));
            return DSTORE_FAIL;
        }
    }

    /* force update tuple data no trx */
    errno_t rc = memcpy_s(oldTuplePtr, newLen, newTuplePtr, newLen);
    storage_securec_check(rc, "\0", "\0");
    (void)m_bufMgr->MarkDirty(m_bufferDesc);

    /* Insert redo log */
    walContext->BeginAtomicWal(INVALID_XID);
    if (NeedWal()) {
        StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_HEAP, ErrMsg("Wal has not atomic begin."));
        walContext->RememberPageNeedWal(m_bufferDesc);
        bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
        uint64 fileVersion = m_bufferDesc->GetFileVersion();
        uint32 walDataSize = sizeof(WalRecordHeapForceUpdateTupleDataNoTrx) + newLen;

        /* record WalRecordHeapForceUpdateTupleDataNoTrx */
        WalRecordHeapForceUpdateTupleDataNoTrx *walData =
            static_cast<WalRecordHeapForceUpdateTupleDataNoTrx *>(walContext->GetTempWalBuf());
        StorageReleasePanic(walDataSize > BLCKSZ, MODULE_HEAP, ErrMsg("The value of dstore palloc exceeds BLCKSZ, "
        "the walDataSize is (%u)", walDataSize));
        walData->SetHeader({WAL_HEAP_FORCE_UPDATE_TUPLE_DATA_NO_TRX, walDataSize, m_bufferDesc->GetPageId(),
            page->GetWalId(), page->GetPlsn(), page->GetGlsn(), glsnChangedFlag, fileVersion},
            offset, newLen);
        walData->SetData(newTuplePtr, newLen);
        walContext->PutNewWalRecord(walData);
    }

    if (!updateContext->needReturnOldTup) {
        DstorePfreeExt(updateContext->retOldTuple);
    }

    m_bufMgr->UnlockAndRelease(m_bufferDesc);
    (void)walContext->EndAtomicWal();
    m_bufferDesc = nullptr;
    updateContext->newCtid = updateContext->oldCtid;

    if (wait_flush) {
        StorageReleasePanic(wait_flush == true, MODULE_HEAP,
            ErrMsg("The parameter value of wait_falsh is incorrect: %d.", wait_flush));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus HeapUpdateHandler::UpdateSmallTupleSamePage(HeapUpdateContext *updateContext, HeapPage *page, TdId tdId)
{
    HeapTuple oldTuple;
    OffsetNumber offset = updateContext->oldCtid.GetOffset();
    HeapTuple *newTuple = updateContext->newTuple;
    Transaction *transaction = m_thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    TransactionMgr *txnMgr = m_instance->GetPdb(m_heapRel->m_pdbId)->GetTransactionMgr();
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;

    page->GetTuple(&oldTuple, offset);
    Xid xid = transaction->GetCurrentXid();

    StorageReleasePanic(tdId == INVALID_TD_SLOT, MODULE_HEAP, ErrMsg("Tdid invalid."));
    /* Step 1: remove the locker td id */
    HeapDiskTuple *oldTupleDisk = oldTuple.GetDiskTuple();
    oldTupleDisk->SetLockerTdId(INVALID_TD_SLOT);

    /*
     * For updates on the same page, the ctid cannot be changed because we do not want to insert index.
     * The ctid needs to be pointed from the old position to the new position.
     */
    updateContext->newCtid = updateContext->oldCtid;

    /* Need return old tuple if the data of index column is changed */
    if (updateContext->needReturnOldTup || updateContext->needUpdateLob) {
        updateContext->retOldTuple = page->CopyTuple(offset);
        if (unlikely(updateContext->retOldTuple == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when update small tuple same page."));
            m_bufMgr->UnlockAndRelease(m_bufferDesc);
            m_bufferDesc = nullptr;
            return DSTORE_FAIL;
        }
    }

    /* Step 2: Delete old tuple in page */
    Size undoSize = static_cast<Size>(sizeof(UndoDataHeapSamePageAppendUpdate) + oldTuple.GetDiskTupleSize());
    UndoDataHeapSamePageAppendUpdate* undoData =
        static_cast<UndoDataHeapSamePageAppendUpdate*>(DstorePalloc(undoSize));
    if (unlikely(undoData == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%u).", static_cast<uint32>(undoSize)));
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        m_bufferDesc = nullptr;
        return DSTORE_FAIL;
    }
    undoData->Init();
    uint32 oldTupleSize = oldTuple.GetDiskTupleSize();
    undoData->Append(static_cast<char *>(static_cast<void *>(oldTupleDisk)), static_cast<uint16>(oldTupleSize));

    UndoType type = GetTableSmgr()->IsGlobalTempTable() ? UNDO_HEAP_SAME_PAGE_APPEND_UPDATE_TMP
                                                     : UNDO_HEAP_SAME_PAGE_APPEND_UPDATE;
    UndoRecord undoRecord(type, tdId, page->GetTd(tdId), updateContext->newCtid, updateContext->cid);
    RetStatus ret = undoRecord.Append(static_cast<char *>(static_cast<void *>(undoData)), static_cast<int32>(undoSize));
    if (STORAGE_FUNC_FAIL(ret)) {
        if (m_tdcontext.allocTd.isDirty) {
            (void)m_bufMgr->MarkDirty(m_bufferDesc);
            GenerateAllocTdWal(m_bufferDesc);
        }
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("Append undo space fail when same page update."));
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        m_bufferDesc = nullptr;
        DstorePfree(undoData);
        return DSTORE_FAIL;
    }
    DstorePfree(undoData);
    /* if extend fail, we have done alloctd, so we need record alloctd wal */
    if (STORAGE_FUNC_FAIL(txnMgr->ExtendUndoSpaceIfNeeded(xid, undoRecord.GetRecordSize()))) {
        if (m_tdcontext.allocTd.isDirty) {
            (void)m_bufMgr->MarkDirty(m_bufferDesc);
            GenerateAllocTdWal(m_bufferDesc);
        }
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Extend undo space fail when same page update."));
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        m_bufferDesc = nullptr;
        return DSTORE_FAIL;
    }
    StorageAssert(tdId < page->GetTdCount());
    oldTupleDisk->SetTdId(tdId);
    oldTupleDisk->SetXid(xid);
    oldTupleDisk->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    page->AddPotentialDelItemSize(offset);
    page->SetTuplePrunable(true);

    /* Step 3: Insert new tuple into the same page and generate undo record */
    newTuple->GetDiskTuple()->SetTdId(tdId);
    newTuple->GetDiskTuple()->SetXid(xid);
    newTuple->GetDiskTuple()->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    newTuple->GetDiskTuple()->SetLockerTdId(INVALID_TD_SLOT);
    newTuple->GetDiskTuple()->SetTupleSize(static_cast<uint16>(newTuple->GetDiskTupleSize()));
    newTuple->GetDiskTuple()->SetLiveMode(HeapDiskTupLiveMode::NEW_TUPLE_BY_SAME_PAGE_UPDATE);
    UNUSE_PARAM OffsetNumber newOffset =
        page->AddTuple(newTuple->GetDiskTuple(), static_cast<uint16>(newTuple->GetDiskTupleSize()), offset);
    StorageReleasePanic(newOffset != offset, MODULE_HEAP,
                        ErrMsg("Failed to add tuple in page(%hu, %u).", page->GetFileId(), page->GetBlockNum()));
    oldTupleDisk->SetLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_SAME_PAGE_UPDATE);
    (void)m_bufMgr->MarkDirty(m_bufferDesc);

    /* Step 4: Put undo record and generate redo log */
    walContext->BeginAtomicWal(xid);
    UndoRecPtr undoRecPtr = InsertUndoAndCheck(&undoRecord);
    if (unlikely(undoRecPtr == INVALID_UNDO_RECORD_PTR)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Insert undo record fail when update small tuples same page."));
        walContext->ResetForAbort();
        return DSTORE_FAIL;
    }
    page->SetTd(tdId, xid, undoRecPtr, updateContext->cid);

    if (likely(NeedWal())) {
        /* record WalRecordHeapSamePageAppendUpdate */
        StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_HEAP, ErrMsg("Wal has not atomic begin."));
        walContext->RememberPageNeedWal(m_bufferDesc);
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        GenerateAppendUpdateWal(m_bufferDesc, newTuple->GetDiskTuple(), offset, undoRecPtr, updateContext->cid,
                                &oldTuple, updateContext->replicaKeyAttrs);
    } else {
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
    }
    (void)walContext->EndAtomicWal();

    m_bufferDesc = nullptr;

    return DSTORE_SUCC;
}

RetStatus HeapUpdateHandler::UpdateSmallTupleAnotherPage(HeapUpdateContext *updateContext, TdId oldPageTdId)
{
    /* Step 1: Lock tuple on old page to prevent other transactions from modify it. */
    updateContext->needReturnOldTup = updateContext->hasIndex;

    HeapLockTupleContext lockTupleContext;
    lockTupleContext.needRetTup = (updateContext->needReturnOldTup ||
                                   updateContext->needUpdateLob ||
                                   WalLogicalActive());
    lockTupleContext.ctid = updateContext->oldCtid;

    RetStatus status = DoLock(&lockTupleContext, oldPageTdId);
    if (STORAGE_FUNC_FAIL(status)) {
        ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Lock failed when udpate another page, ctid({%hu, %u}, %hu)",
            updateContext->oldCtid.GetFileId(), updateContext->oldCtid.GetBlockNum(),
            updateContext->oldCtid.GetOffset()));
        return DSTORE_FAIL;
    }

    updateContext->retOldTuple = lockTupleContext.retTup;
    if (updateContext->retOldTuple != nullptr) {
        updateContext->retOldTuple->GetDiskTuple()->SetLockerTdId(INVALID_TD_SLOT);
    }
    TryToUnlockReleaseBufferDesc();

    /* Step 2: Insert on new page */
    ItemPointerData newCtid = UpdateSmallTupleNewPage(updateContext);
    /* free the memory of retOldTuple first. */
    if (newCtid == INVALID_ITEM_POINTER || (!updateContext->needReturnOldTup &&
        !updateContext->needUpdateLob && WalLogicalActive())) {
        DstorePfreeExt(updateContext->retOldTuple);
        updateContext->retOldTuple = nullptr;
    }
    if (newCtid == INVALID_ITEM_POINTER) {
        return DSTORE_FAIL;
    }
    updateContext->newCtid = newCtid;

    return UpdateSmallTupleOldPage(updateContext->oldCtid, newCtid, oldPageTdId, updateContext->cid);
}

RetStatus HeapUpdateHandler::UpdateSmallTupleOldPage(ItemPointerData otid, ItemPointerData newCtid, TdId tdId,
                                                     CommandId cid)
{
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    Transaction *transaction = m_thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    Xid xid = transaction->GetCurrentXid();
    HeapTuple oldTuple;

    /* Step 1: Re-read old page, the tuple will not change because of tuple lock. */
    m_bufferDesc =
        m_bufMgr->Read(m_heapRel->m_pdbId, otid.GetPageId(), LW_EXCLUSIVE);
    if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("Read page(%hu, %u) failed.", otid.GetPageId().m_fileId, otid.GetPageId().m_blockId));
        return DSTORE_FAIL;
    }
    CheckBufferedPage(m_bufferDesc->GetPage(), otid.GetPageId());
    HeapPage *oldPage = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    oldPage->GetTuple(&oldTuple, otid.GetOffset());
    StorageReleasePanic(tdId == INVALID_TD_SLOT, MODULE_HEAP, ErrMsg("Tdid invalid."));
    (void)m_bufMgr->MarkDirty(m_bufferDesc);

    /* Step 2: Generate undo record. */
    UndoType type = GetTableSmgr()->IsGlobalTempTable() ? UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE_TMP
                                                     : UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE;
    UndoRecord undoRecord(type, tdId, oldPage->GetTd(tdId), otid, cid);
    HeapDiskTuple *oldTupleDisk = oldTuple.GetDiskTuple();
    oldTupleDisk->SetLockerTdId(INVALID_TD_SLOT);
    uint32 size = sizeof(UndoDataHeapAnotherPageAppendUpdate) + oldTuple.GetDiskTupleSize();
    UndoDataHeapAnotherPageAppendUpdate *undoData =
        static_cast<UndoDataHeapAnotherPageAppendUpdate *>(walContext->GetTempWalBuf());
    StorageReleasePanic(size > BLCKSZ, MODULE_HEAP, ErrMsg("The value of dstore palloc exceeds BLCKSZ, "
    "the size is (%u)", size));
    undoData->Init();
    undoData->Append(static_cast<char *>(static_cast<void *>(oldTupleDisk)),
                     static_cast<uint16>(oldTuple.GetDiskTupleSize()));
    /*
     * Let other transactions know where to find the updated version of the
     * old tuple by saving the new tuple CTID on the old tuple undo record.
     */
    undoData->SetNewCtid(newCtid);
    RetStatus ret = undoRecord.Append(static_cast<char *>(static_cast<void *>(undoData)), static_cast<int32>(size));
    if (STORAGE_FUNC_FAIL(ret)) {
        HandleErrorWhenGetUndoBuffer(m_bufferDesc, &undoRecord, undoData);
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("Append undo record failed(%u) when old page update.", undoData->GetSize()));
        return DSTORE_FAIL;
    }

    TransactionMgr *txnMgr = m_instance->GetPdb(m_heapRel->m_pdbId)->GetTransactionMgr();
    /* if extend fail, why here no need record alloctd wal, because we has done before when call UpdateSmallTuple */
    if (STORAGE_FUNC_FAIL(txnMgr->ExtendUndoSpaceIfNeeded(xid, undoRecord.GetRecordSize()))) {
        HandleErrorWhenGetUndoBuffer(m_bufferDesc, &undoRecord);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Extend undo space fail when old page update."));
        return DSTORE_FAIL;
    }

    /* Step 3: Mark old tuple as updated. */
    StorageAssert(tdId < oldPage->GetTdCount());
    oldTupleDisk->SetLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE);
    oldTupleDisk->SetTdId(tdId);
    oldTupleDisk->SetXid(xid);
    oldTupleDisk->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    oldPage->SetTuplePrunable(true);
    oldPage->AddPotentialDelItemSize(otid.GetOffset());

    /* Step 4: Generate redo log. */
    walContext->BeginAtomicWal(xid);
    UndoRecPtr undoRecPtr = transaction->InsertUndoRecord(&undoRecord);
    /* if insert fail, why here no need record alloctd wal, because we has done before  when call UpdateSmallTuple */
    if (unlikely(undoRecPtr == INVALID_UNDO_RECORD_PTR)) {
        HandleErrorWhenGetUndoBuffer(m_bufferDesc, &undoRecord);
        walContext->ResetForAbort();
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Insert undo record fail when update old page."));
        return DSTORE_FAIL;
    }
    oldPage->SetTd(tdId, xid, undoRecPtr, cid);

    if (likely(NeedWal())) {
        /* record WalRecordHeapAnotherPageAppendUpdateOldPage */
        StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_HEAP, ErrMsg("Wal has not atomic begin."));
        walContext->RememberPageNeedWal(m_bufferDesc);
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        GenerateAnotherUpdateOldPageWal(m_bufferDesc, otid.GetOffset(), undoRecPtr, tdId);
    } else {
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
    }
    (void)walContext->EndAtomicWal();

    m_bufferDesc = nullptr;
    return DSTORE_SUCC;
}

ItemPointerData HeapUpdateHandler::UpdateSmallTupleNewPage(HeapUpdateContext *updateContext)
{
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    Transaction *transaction = m_thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    Xid xid = transaction->GetCurrentXid();
    HeapTuple *newTuple = updateContext->newTuple;
    uint16 diskTupSize = static_cast<uint16>(newTuple->GetDiskTupleSize());

    /* Step 1: Get new buffer page and allocate TD */
    TdId newTdId = INVALID_TD_SLOT;
    /*
     * We don't want to fetch the old page again for another page update.
     * Hence, below we prevent fetching the old page by passing the old page id as excludePageId.
     */
    m_bufferDesc = GetBuffer(newTdId, diskTupSize, updateContext->oldCtid.GetPageId());
    if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to get buffer when updating diskTuple in new page. "
            "Size of diskTuple is %hu.", diskTupSize));
        return INVALID_ITEM_POINTER;
    }
    HeapPage *newPage = static_cast<HeapPage *>(m_bufferDesc->GetPage());

    /* Step 2: Insert new tuple into the new page and generate undo record */
    StorageAssert(newTdId < newPage->GetTdCount());
    newTuple->GetDiskTuple()->SetTdId(newTdId);
    newTuple->GetDiskTuple()->SetXid(xid);
    newTuple->GetDiskTuple()->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    newTuple->GetDiskTuple()->SetLockerTdId(INVALID_TD_SLOT);
    newTuple->GetDiskTuple()->SetLiveMode(HeapDiskTupLiveMode::NEW_TUPLE_BY_ANOTHER_PAGE_UPDATE);
    newTuple->GetDiskTuple()->SetTupleSize(diskTupSize);
    OffsetNumber newOffset = newPage->AddTuple(newTuple->GetDiskTuple(), diskTupSize);
    StorageReleasePanic(newOffset == INVALID_ITEM_OFFSET_NUMBER, MODULE_HEAP,
        ErrMsg("Failed to add tuple in page(%hu, %u).", newPage->GetFileId(), newPage->GetBlockNum()));
    (void)m_bufMgr->MarkDirty(m_bufferDesc);
    ItemPointerData newCtid(m_bufferDesc->GetPageId(), newOffset);
    UNUSE_PARAM TD *td = newPage->GetTd(newTdId);
    UndoType type = GetTableSmgr()->IsGlobalTempTable() ? UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE_TMP
                                                     : UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE;
    UndoRecord undoRecord(type, newTdId, td, newCtid, updateContext->cid);
    TransactionMgr *txnMgr = m_instance->GetPdb(m_heapRel->m_pdbId)->GetTransactionMgr();
    /* if extend fail, we have done alloctd and modify page, need rollback page changes and record alloctd wal */
    if (STORAGE_FUNC_FAIL(txnMgr->ExtendUndoSpaceIfNeeded(xid, undoRecord.GetRecordSize()))) {
        if (m_tdcontext.allocTd.isDirty) {
            (void)m_bufMgr->MarkDirty(m_bufferDesc);
            GenerateAllocTdWal(m_bufferDesc);
        }
        HandleErrorWhenGetUndoBuffer(m_bufferDesc, &undoRecord);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Extend undo space fail when update small tuple new page."));
        return INVALID_ITEM_POINTER;
    }
    walContext->BeginAtomicWal(xid);
    UndoRecPtr undoRecPtr = InsertUndoAndCheck(&undoRecord);
    if (unlikely(undoRecPtr == INVALID_UNDO_RECORD_PTR)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Insert undo record fail when update small tuple new page."));
        return INVALID_ITEM_POINTER;
    }
    newPage->SetTd(newTdId, xid, undoRecPtr, updateContext->cid);

    /* Step 3: Generate redo log. */
    if (likely(NeedWal())) {
        /* record WalRecordHeapAnotherPageAppendUpdateNewPage */
        StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_HEAP, ErrMsg("Wal has not atomic begin."));
        walContext->RememberPageNeedWal(m_bufferDesc);
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        GenerateAnotherUpdateNewPageWal(m_bufferDesc, newTuple->GetDiskTuple(), newCtid.GetOffset(),
                                        undoRecPtr, updateContext->cid, updateContext->retOldTuple,
                                        updateContext->replicaKeyAttrs);
    } else {
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
    }
    (void)walContext->EndAtomicWal();

    m_bufferDesc = nullptr;
    return newCtid;
}

void HeapUpdateHandler::GetDiffBetweenTuples(HeapTuple *oldTup, HeapTuple *newTup)
{
    uint16 oldLen = static_cast<uint16>(oldTup->GetDiskTupleSize() - HeapDiskTuple::GetHeaderSize());
    uint16 newLen = static_cast<uint16>(newTup->GetDiskTupleSize() - HeapDiskTuple::GetHeaderSize());
    uint16 oldOffset = static_cast<uint16>(oldLen - 1);
    uint16 newOffset = static_cast<uint16>(newLen - 1);
    char *oldp = oldTup->GetDiskTuple()->GetData() + oldOffset;
    char *newp = newTup->GetDiskTuple()->GetData() + newOffset;
    uint16 shortLen = (oldLen < newLen) ? oldLen : newLen;
    uint16 longLen = (oldLen >= newLen) ? oldLen : newLen;
    m_numDiffPos = 0;
    StorageAssert(shortLen <= longLen);

    uint16 index = 0;
    for (uint16 i = 0; i < MAX_DIFF_POS_NUM; i += NUM_DIFF_STEP) {
        /* Find next start point of difference. */
        while ((index < shortLen) && (*(oldp - index) == *(newp - index))) {
            index++;
        }
        uint16 start = index;

        /* Find next end point of difference. */
        while ((index < shortLen) && (*(oldp - index) != *(newp - index))) {
            index++;
        }
        uint16 end = (index >= shortLen) ? longLen : index;
        index = end;

        /* Stop if scan tuple finished. */
        if (start == end) {
            break;
        }

        /* Record difference. */
        m_diffPos[i] = start;
        m_diffPos[i + 1] = end;
        m_numDiffPos = i + NUM_DIFF_STEP;
    }

    /* Readjust end point of last difference if m_diffPos is used up. */
    if (index < shortLen) {
        StorageAssert(m_numDiffPos > 0);
        m_diffPos[m_numDiffPos - 1] = longLen;
    }

    StorageAssert(m_numDiffPos % NUM_DIFF_STEP == 0);
}

uint16 HeapUpdateHandler::GetTupleDiffSize(uint16 tupDataSize)
{
    uint16 diffSize = 0;
    m_tupDiffNum = 0;

    for (int i = 1; i < m_numDiffPos; i += NUM_DIFF_STEP) {
        m_tupDiffPos[i] = m_diffPos[i];
        m_tupDiffPos[i - 1] = m_diffPos[i - 1];

        if (m_tupDiffPos[i] > tupDataSize) {
            m_tupDiffPos[i] = tupDataSize;
            if (m_tupDiffPos[i] == m_tupDiffPos[i - 1]) {
                break;
            }
        }

        m_tupDiffNum += NUM_DIFF_STEP;
        diffSize += static_cast<uint16>(m_tupDiffPos[i] - m_tupDiffPos[i - 1]);
    }

    diffSize += (m_tupDiffNum * static_cast<uint16>(sizeof(m_diffPos[0])));
    return diffSize;
}

void HeapUpdateHandler::GenerateInplaceUpdateWal(BufferDesc *bufferDesc, HeapDiskTuple *newTuple,
                                                 HeapDiskTuple *oldTuple,
                                                 const WalRecordHeapInplaceUpdateHeaderContext &inplaceUpdateHeader,
                                                 CommandId cid)
{
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
    bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = bufferDesc->GetFileVersion();
    uint32 diffSizeForRedo =
        GetTupleDiffSize(static_cast<uint16>(newTuple->GetTupleSize() - HEAP_DISK_TUP_HEADER_SIZE));
    uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(m_tdcontext);
    uint32 walDataSize = sizeof(WalRecordHeapInplaceUpdate) + diffSizeForRedo + allocTdSize;
#ifdef ENABLE_LOGICAL_REPL
    Oid tableOid = GetTableOid();
    if (WalLogicalActive() && oldTuple != nullptr) {
        walDataSize += oldTuple->GetTupleSize();
        walDataSize = walDataSize + sizeof(Oid) + sizeof(CommitSeqNo);
        if (unlikely(IsLogicalDecodeDictNeeded(tableOid))) {
            walDataSize += sizeof(CommandId);
        }
    }
#else
    UNUSED_VARIABLE(oldTuple);
    UNUSED_VARIABLE(cid);
#endif

    /* record WalRecordHeapInplaceUpdate */
    WalRecordHeapInplaceUpdate *walData = static_cast<WalRecordHeapInplaceUpdate *>(walContext->GetTempWalBuf());
    StorageReleasePanic(walDataSize > BLCKSZ, MODULE_HEAP, ErrMsg("The value of dstore palloc exceeds BLCKSZ, "
    "the walDataSize is (%u)", walDataSize));
    walData->SetHeader({WAL_HEAP_INPLACE_UPDATE, walDataSize, bufferDesc->GetPageId(), page->GetWalId(),
        page->GetPlsn(), page->GetGlsn(), glsnChangedFlag, fileVersion}, inplaceUpdateHeader);
    walData->SetData(m_tupDiffNum, m_tupDiffPos, newTuple->GetData());
#ifdef ENABLE_LOGICAL_REPL
    if (WalLogicalActive() && oldTuple != nullptr) {
        CommitSeqNo snapshotCsn = m_thrd->GetActiveTransaction()->GetSnapshotCsn();
        CommandId snapshotCid = INVALID_CID;
        if (unlikely(IsLogicalDecodeDictNeeded(tableOid))) {
            snapshotCid = cid;
        }
        walData->SetLogicalDecodeInfo(oldTuple, &tableOid, &snapshotCsn, &snapshotCid);
    }
#endif
    walData->SetAllocTd(m_tdcontext);
    walContext->PutNewWalRecord(walData);
}

void HeapUpdateHandler::GenerateAppendUpdateWal(BufferDesc *bufferDesc, HeapDiskTuple *newTuple, OffsetNumber offset,
                                                UndoRecPtr undoPtr, CommandId cid, HeapTuple *oldTuple,
                                                Bitmapset *replicaKeyAttrs)
{
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
    bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = bufferDesc->GetFileVersion();
    uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(m_tdcontext);
    uint32 walDataSize = sizeof(WalRecordHeapSamePageAppendUpdate) + newTuple->GetTupleSize() + allocTdSize;
#ifdef ENABLE_LOGICAL_REPL
    HeapTuple *oldTupleKey = nullptr;
    HeapDiskTuple *oldDiskTupleKey = nullptr;
    Oid tableOid = GetTableOid();
    if (WalLogicalActive()) {
        oldTupleKey = FormIdentityTuple(oldTuple, replicaKeyAttrs);
        oldDiskTupleKey = (oldTupleKey != nullptr ? oldTupleKey->GetDiskTuple() : nullptr);
        StorageReleasePanic(oldDiskTupleKey == nullptr, MODULE_HEAP, ErrMsg("oldDiskTupleKey is null."));
        walDataSize += oldDiskTupleKey->GetTupleSize();
        walDataSize = walDataSize + sizeof(Oid) + sizeof(CommitSeqNo);
        if (unlikely(IsLogicalDecodeDictNeeded(tableOid))) {
            walDataSize += sizeof(CommandId);
        }
    }
#else
    UNUSED_VARIABLE(oldTuple);
    UNUSED_VARIABLE(cid);
    UNUSED_VARIABLE(replicaKeyAttrs);
#endif
    /* record WalRecordHeapSamePageAppendUpdate */
    WalRecordHeapSamePageAppendUpdate *walData =
        static_cast<WalRecordHeapSamePageAppendUpdate*>(walContext->GetTempWalBuf());
    StorageReleasePanic(walDataSize > BLCKSZ, MODULE_HEAP, ErrMsg("The value of dstore palloc exceeds BLCKSZ, "
    "the walDataSize is (%u)", walDataSize));
    walData->SetHeader({WAL_HEAP_SAME_PAGE_APPEND, walDataSize, bufferDesc->GetPageId(), page->GetWalId(),
        page->GetPlsn(), page->GetGlsn(), glsnChangedFlag, fileVersion}, offset, undoPtr);
    walData->SetData(static_cast<char *>(static_cast<void *>(newTuple)), newTuple->GetTupleSize());
#ifdef ENABLE_LOGICAL_REPL
    if (WalLogicalActive()) {
        CommitSeqNo snapshotCsn = m_thrd->GetActiveTransaction()->GetSnapshotCsn();
        CommandId snapshotCid = INVALID_CID;
        if (unlikely(IsLogicalDecodeDictNeeded(tableOid))) {
            snapshotCid = cid;
        }
        walData->SetLogicalDecodeInfo(oldDiskTupleKey, &tableOid, &snapshotCsn, &snapshotCid);
        DstorePfreeExt(oldTupleKey);
    }
#endif
    walData->SetAllocTd(m_tdcontext);

    walContext->PutNewWalRecord(walData);
}

void HeapUpdateHandler::GenerateAnotherUpdateOldPageWal(BufferDesc *bufferDesc, OffsetNumber offset, UndoRecPtr undoPtr,
                                                        uint8 tdId)
{
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
    bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = bufferDesc->GetFileVersion();
    uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(m_tdcontext);
    uint32 walDataSize = sizeof(WalRecordHeapAnotherPageAppendUpdateOldPage) + allocTdSize;

    /* record WalRecordHeapAnotherPageAppendUpdateOldPage */
    WalRecordHeapAnotherPageAppendUpdateOldPage *walData =
        static_cast<WalRecordHeapAnotherPageAppendUpdateOldPage *>(walContext->GetTempWalBuf());
    StorageReleasePanic(walDataSize > BLCKSZ, MODULE_HEAP, ErrMsg("The value of dstore palloc exceeds BLCKSZ, "
    "the walDataSize is (%u)", walDataSize));
    walData->SetHeader({WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE, walDataSize, bufferDesc->GetPageId(),
        page->GetWalId(), page->GetPlsn(), page->GetGlsn(), glsnChangedFlag, fileVersion}, offset, undoPtr, tdId);
    walData->SetAllocTd(m_tdcontext);

    walContext->PutNewWalRecord(walData);
}

void HeapUpdateHandler::GenerateAnotherUpdateNewPageWal(BufferDesc *bufferDesc, HeapDiskTuple *newTuple,
                                                        OffsetNumber offset, UndoRecPtr undoPtr, CommandId cid,
                                                        HeapTuple *oldTuple, Bitmapset *replicaKeyAttrs)
{
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;

    HeapPage *newPage = static_cast<HeapPage *>(bufferDesc->GetPage());
    bool glsnChangedFlag = (newPage->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = bufferDesc->GetFileVersion();
    uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(m_tdcontext);
    uint32 walDataSize = sizeof(WalRecordHeapAnotherPageAppendUpdateNewPage) + newTuple->GetTupleSize() + allocTdSize;
#ifdef ENABLE_LOGICAL_REPL
    HeapTuple *oldTupleKey = nullptr;
    HeapDiskTuple *oldDiskTupleKey = nullptr;
    Oid tableOid = GetTableOid();
    if (WalLogicalActive()) {
        oldTupleKey = FormIdentityTuple(oldTuple, replicaKeyAttrs);
        oldDiskTupleKey = (oldTupleKey != nullptr ? oldTupleKey->GetDiskTuple() : nullptr);
        walDataSize += oldDiskTupleKey->GetTupleSize();
        walDataSize = walDataSize + sizeof(Oid) + sizeof(CommitSeqNo);
        if (unlikely(IsLogicalDecodeDictNeeded(tableOid))) {
            walDataSize += sizeof(CommandId);
        }
    }
#else
    UNUSED_VARIABLE(cid);
    UNUSED_VARIABLE(oldTuple);
    UNUSED_VARIABLE(replicaKeyAttrs);
#endif

    /* record WalRecordHeapAnotherPageAppendUpdateNewPage */
    WalRecordHeapAnotherPageAppendUpdateNewPage *walData =
        static_cast<WalRecordHeapAnotherPageAppendUpdateNewPage *>(walContext->GetTempWalBuf());
    StorageReleasePanic(walDataSize > BLCKSZ, MODULE_HEAP, ErrMsg("The value of dstore palloc exceeds BLCKSZ, "
    "the walDataSize is (%u)", walDataSize));
    walData->SetHeader({WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE, walDataSize, bufferDesc->GetPageId(),
        newPage->GetWalId(), newPage->GetPlsn(), newPage->GetGlsn(), glsnChangedFlag, fileVersion}, offset, undoPtr);
    walData->SetData(static_cast<char *>(static_cast<void *>(newTuple)), newTuple->GetTupleSize());
#ifdef ENABLE_LOGICAL_REPL
    if (WalLogicalActive()) {
        CommitSeqNo snapshotCsn = m_thrd->GetActiveTransaction()->GetSnapshotCsn();
        CommandId snapshotCid = INVALID_CID;
        if (unlikely(IsLogicalDecodeDictNeeded(tableOid))) {
            snapshotCid = cid;
        }
        walData->SetLogicalDecodeInfo(oldDiskTupleKey, &tableOid, &snapshotCsn, &snapshotCid);
        DstorePfreeExt(oldTupleKey);
    }
#endif
    walData->SetAllocTd(m_tdcontext);

    walContext->PutNewWalRecord(walData);
}

HeapUpdateHandler::HeapUpdateHandler(StorageInstance *instance, ThreadContext *thread, StorageRelation heapRel,
                                     bool isLobOperation)
    : HeapHandler(instance, thread, heapRel, isLobOperation),
      HeapDeleteHandler(instance, thread, heapRel, isLobOperation),
      HeapInsertHandler(instance, thread, heapRel, isLobOperation),
      m_numDiffPos(0),
      m_tupDiffNum(0),
      m_isBigTuple(false)
{
    /* m_diffPos and m_tupDiffPos is not init hear, which is protected by m_numDiffPos and m_tupDiffNum */
}

}  // namespace DSTORE

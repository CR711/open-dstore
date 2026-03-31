#include "dfx/dstore_heap_verify.h"

#include <cstdarg>
#include <cstdio>
#include <unordered_set>

#include "buffer/dstore_buf_mgr.h"
#include "fsm/dstore_partition_fsm.h"
#include "framework/dstore_thread.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_interface.h"
#include "tablespace/dstore_data_segment_context.h"
#include "dfx/dstore_page_verify.h"

namespace DSTORE {

namespace {

class RelationHeapVerifyPageSource : public HeapVerifyPageSource {
public:
    RelationHeapVerifyPageSource(BufMgrInterface *bufMgr, StorageRelation heapRel)
        : m_bufMgr(bufMgr), m_heapRel(heapRel), m_scanContext(bufMgr, heapRel, SmgrType::HEAP_SMGR)
    {}

    PageId GetFirstPageId() override
    {
        return m_scanContext.GetFirstPageId();
    }

    PageId GetNextPageId() override
    {
        return m_scanContext.GetNextPageId();
    }

    HeapPage *ReadHeapPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        if (bufferDesc == nullptr || m_bufMgr == nullptr || m_heapRel == nullptr) {
            return nullptr;
        }

        *bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, pageId, LW_SHARED);
        if (*bufferDesc == INVALID_BUFFER_DESC) {
            return nullptr;
        }
        return static_cast<HeapPage *>((*bufferDesc)->GetPage());
    }

    void ReleaseHeapPage(BufferDesc *bufferDesc) override
    {
        if (bufferDesc != INVALID_BUFFER_DESC && m_bufMgr != nullptr) {
            m_bufMgr->UnlockAndRelease(bufferDesc);
        }
    }

    bool GetRecordedFsmSpace(const FsmIndex &fsmIndex, uint16 *listId, uint32 *spaceUpperBound) override
    {
        if (m_bufMgr == nullptr || m_heapRel == nullptr || fsmIndex.page.IsInvalid() || fsmIndex.slot >= FSM_MAX_HWM) {
            return false;
        }

        BufferDesc *bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, fsmIndex.page, LW_SHARED);
        if (bufferDesc == INVALID_BUFFER_DESC) {
            return false;
        }

        FsmPage *fsmPage = static_cast<FsmPage *>(bufferDesc->GetPage());
        bool found = false;
        if (fsmPage != nullptr && fsmIndex.slot < fsmPage->fsmPageHeader.hwm) {
            const uint16 currentListId = fsmPage->FsmNodePtr(fsmIndex.slot)->listId;
            if (listId != nullptr) {
                *listId = currentListId;
            }
            if (spaceUpperBound != nullptr) {
                *spaceUpperBound = FSM_SPACE_LINE[currentListId];
            }
            found = true;
        }

        m_bufMgr->UnlockAndRelease(bufferDesc);
        return found;
    }

private:
    BufMgrInterface *m_bufMgr{nullptr};
    StorageRelation m_heapRel{nullptr};
    DataSegmentScanContext m_scanContext;
};

constexpr const char *HEAP_VERIFY_TARGET = "heap_segment";

bool IsTupleStorageItem(const ItemId *itemId)
{
    return itemId != nullptr && itemId->IsNormal();
}

}  // namespace

HeapSegmentVerifier::HeapSegmentVerifier(
    HeapVerifyPageSource *pageSource, const HeapVerifyOptions &options, VerifyContext *context)
    : m_pageSource(pageSource), m_options(options), m_context(context)
{}

RetStatus HeapSegmentVerifier::Verify()
{
    if (m_pageSource == nullptr || m_context == nullptr || m_context->GetReport() == nullptr) {
        return DSTORE_FAIL;
    }

    for (PageId pageId = m_pageSource->GetFirstPageId(); pageId.IsValid(); pageId = m_pageSource->GetNextPageId()) {
        BufferDesc *bufferDesc = INVALID_BUFFER_DESC;
        HeapPage *page = m_pageSource->ReadHeapPage(pageId, &bufferDesc);
        if (page == nullptr) {
            ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "heap_page_read_failed", 1, 0,
                "Failed to read heap page (%hu,%u)", pageId.m_fileId, pageId.m_blockId);
            return DSTORE_FAIL;
        }

        RetStatus pageRet = VerifyHeapPage(page);
        m_pageSource->ReleaseHeapPage(bufferDesc);
        if (pageRet != DSTORE_SUCC || m_context->HasReachedErrorLimit()) {
            return DSTORE_FAIL;
        }
    }

    return m_context->GetReport()->GetRetStatus();
}

RetStatus HeapSegmentVerifier::VerifyHeapPage(HeapPage *page)
{
    VerifyReport *report = m_context->GetReport();
    if (VerifyPage(page, VerifyLevel::HEAVYWEIGHT, report) != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }

    for (OffsetNumber offset = FIRST_ITEM_OFFSET_NUMBER; offset <= page->GetMaxOffset(); ++offset) {
        const ItemId *itemId = page->GetItemIdPtr(offset);
        if (!IsTupleStorageItem(itemId)) {
            continue;
        }

        if (VerifyTuple(page, offset) != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }

        if (m_options.checkBigTupleChains && VerifyBigTupleChain(page, offset) != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
    }

    if (m_options.checkFsmConsistency) {
        VerifyFsmConsistency(page);
    }

    return report->GetRetStatus();
}

RetStatus HeapSegmentVerifier::VerifyTuple(HeapPage *page, OffsetNumber offset)
{
    ItemId *itemId = page->GetItemIdPtr(offset);
    HeapDiskTuple *tuple = page->GetDiskTuple(offset);
    const PageId pageId = page->GetSelfPageId();

    if (tuple == nullptr) {
        ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "tuple_missing", 1, 0,
            "Tuple data missing for offset %hu", offset);
        return DSTORE_FAIL;
    }

    if (itemId->GetLen() != tuple->GetTupleSize()) {
        ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "tuple_size_mismatch", itemId->GetLen(),
            tuple->GetTupleSize(), "ItemId[%hu] length %hu != tuple size %hu", offset, itemId->GetLen(),
            tuple->GetTupleSize());
        return DSTORE_FAIL;
    }

    if (tuple->GetNumColumn() == 0) {
        ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "tuple_column_count_invalid", 1, 0,
            "Tuple at offset %hu has zero columns", offset);
        return DSTORE_FAIL;
    }

    const uint32 valueOffset = ResolveTupleValueOffset(tuple);
    if (valueOffset > tuple->GetTupleSize()) {
        ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "tuple_value_offset_invalid", tuple->GetTupleSize(),
            valueOffset, "Tuple at offset %hu has values offset %u beyond tuple size %hu", offset, valueOffset,
            tuple->GetTupleSize());
        return DSTORE_FAIL;
    }

    const uintptr_t pageStart = reinterpret_cast<uintptr_t>(page);
    const uintptr_t tupleStart = reinterpret_cast<uintptr_t>(tuple);
    const uintptr_t tupleEnd = tupleStart + itemId->GetLen();
    if (tupleStart < pageStart || tupleEnd > pageStart + BLCKSZ) {
        ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "tuple_overflow_page_boundary", BLCKSZ,
            static_cast<uint64>(tupleEnd - pageStart),
            "Tuple at offset %hu exceeds page boundary", offset);
        return DSTORE_FAIL;
    }

    if (ShouldSkipTupleOnline(tuple, {pageId, offset})) {
        return DSTORE_SUCC;
    }

    return DSTORE_SUCC;
}

RetStatus HeapSegmentVerifier::VerifyBigTupleChain(HeapPage *page, OffsetNumber offset)
{
    HeapDiskTuple *firstChunk = page->GetDiskTuple(offset);
    if (firstChunk == nullptr || !firstChunk->IsLinked()) {
        return DSTORE_SUCC;
    }

    const PageId pageId = page->GetSelfPageId();
    if (!firstChunk->IsFirstLinkChunk()) {
        return DSTORE_SUCC;
    }

    const uint32 expectedChunks = firstChunk->GetNumChunks();
    if (expectedChunks == 0) {
        ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "big_tuple_chunk_count_invalid", 1, 0,
            "Big tuple at offset %hu declares zero chunks", offset);
        return DSTORE_FAIL;
    }

    std::unordered_set<uint64> visitedCtids;
    visitedCtids.insert(ItemPointerToUint64({pageId, offset}));

    ItemPointerData nextChunkCtid = firstChunk->GetNextChunkCtid();
    uint32 actualChunks = 1;
    while (nextChunkCtid != INVALID_ITEM_POINTER) {
        if (!visitedCtids.insert(ItemPointerToUint64(nextChunkCtid)).second) {
            ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "big_tuple_chain_cycle", expectedChunks, actualChunks,
                "Detected cycle in big tuple chain starting at offset %hu", offset);
            return DSTORE_FAIL;
        }

        BufferDesc *chunkBuffer = INVALID_BUFFER_DESC;
        HeapPage *chunkPage = nullptr;
        HeapDiskTuple *chunkTuple = nullptr;
        ItemId *chunkItemId = nullptr;
        if (!ReadChunkTuple(nextChunkCtid, &chunkBuffer, &chunkPage, &chunkTuple, &chunkItemId)) {
            ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "big_tuple_chain_broken", expectedChunks, actualChunks,
                "Missing tuple chunk (%hu,%u,%hu) for big tuple at offset %hu", nextChunkCtid.GetFileId(),
                nextChunkCtid.GetBlockNum(), nextChunkCtid.GetOffset(), offset);
            return DSTORE_FAIL;
        }

        ++actualChunks;
        if (!chunkTuple->IsLinked() || chunkTuple->IsFirstLinkChunk()) {
            m_pageSource->ReleaseHeapPage(chunkBuffer);
            ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "big_tuple_chain_order_invalid", expectedChunks,
                actualChunks, "Tuple chunk (%hu,%u,%hu) is not a continuation chunk", nextChunkCtid.GetFileId(),
                nextChunkCtid.GetBlockNum(), nextChunkCtid.GetOffset());
            return DSTORE_FAIL;
        }

        if (chunkItemId->GetLen() != chunkTuple->GetTupleSize()) {
            m_pageSource->ReleaseHeapPage(chunkBuffer);
            ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "big_tuple_chunk_size_mismatch", chunkItemId->GetLen(),
                chunkTuple->GetTupleSize(), "Chunk tuple (%hu,%u,%hu) length %hu != tuple size %hu",
                nextChunkCtid.GetFileId(), nextChunkCtid.GetBlockNum(), nextChunkCtid.GetOffset(), chunkItemId->GetLen(),
                chunkTuple->GetTupleSize());
            return DSTORE_FAIL;
        }

        nextChunkCtid = chunkTuple->GetNextChunkCtid();
        m_pageSource->ReleaseHeapPage(chunkBuffer);
    }

    if (actualChunks != expectedChunks) {
        ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "big_tuple_chunk_count_mismatch", expectedChunks,
            actualChunks, "Big tuple at offset %hu expected %u chunks but walked %u", offset, expectedChunks,
            actualChunks);
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

void HeapSegmentVerifier::VerifyFsmConsistency(HeapPage *page)
{
    const FsmIndex fsmIndex = page->GetFsmIndex();
    if (fsmIndex.page.IsInvalid()) {
        return;
    }

    uint16 listId = 0;
    uint32 recordedSpaceUpperBound = 0;
    if (!m_pageSource->GetRecordedFsmSpace(fsmIndex, &listId, &recordedSpaceUpperBound)) {
        ReportResult(VerifySeverity::WARNING_LEVEL, page->GetSelfPageId(), "fsm_lookup_failed", 1, 0,
            "Failed to locate FSM entry (%hu,%u,%hu) for page (%hu,%u)", fsmIndex.page.m_fileId,
            fsmIndex.page.m_blockId, fsmIndex.slot, page->GetFileId(), page->GetBlockNum());
        return;
    }

    const uint32 actualFreeSpace = page->GetFreeSpace<FreeSpaceCondition::RAW>();
    const uint16 actualListId = PartitionFreeSpaceMap::GetListId(static_cast<uint16>(actualFreeSpace));
    if (actualListId != listId) {
        ReportResult(VerifySeverity::WARNING_LEVEL, page->GetSelfPageId(), "fsm_space_mismatch",
            recordedSpaceUpperBound, actualFreeSpace,
            "FSM entry (%hu,%u,%hu) records list %hu (<= %u bytes), actual page free space is %u bytes",
            fsmIndex.page.m_fileId, fsmIndex.page.m_blockId, fsmIndex.slot, listId, recordedSpaceUpperBound,
            actualFreeSpace);
    }
}

bool HeapSegmentVerifier::ShouldSkipTupleOnline(HeapDiskTuple *tuple, const ItemPointerData &ctid)
{
    if (!m_options.isOnline || tuple == nullptr || thrd == nullptr || thrd->GetActiveTransaction() == nullptr) {
        return false;
    }

    Transaction *transaction = thrd->GetActiveTransaction();
    Snapshot snapshot = ResolveSnapshot();
    if (snapshot == nullptr) {
        return false;
    }

    SnapshotData mvccSnapshot = *snapshot;
    if (mvccSnapshot.GetSnapshotType() != SnapshotType::SNAPSHOT_MVCC) {
        mvccSnapshot.SetSnapshotType(SnapshotType::SNAPSHOT_MVCC);
        mvccSnapshot.SetCsn(TransactionInterface::GetLatestSnapshotCsn());
        mvccSnapshot.SetCid(INVALID_CID);
    }
    snapshot = &mvccSnapshot;

    XidStatus xidStatus(tuple->GetXid(), transaction);
    if (xidStatus.IsAborted()) {
        ReportResult(VerifySeverity::INFO_LEVEL, ctid.GetPageId(), "tuple_skipped_aborted", 0, tuple->GetXid().m_placeHolder,
            "Skipped aborted tuple at (%hu,%u,%hu) during online heap verification", ctid.GetFileId(),
            ctid.GetBlockNum(), ctid.GetOffset());
        return true;
    }
    if (xidStatus.IsInProgress() || xidStatus.IsPendingCommit()) {
        ReportResult(VerifySeverity::INFO_LEVEL, ctid.GetPageId(), "tuple_skipped_in_progress", 0,
            tuple->GetXid().m_placeHolder, "Skipped in-progress tuple at (%hu,%u,%hu) during online heap verification",
            ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset());
        return true;
    }

    return false;
}

Snapshot HeapSegmentVerifier::ResolveSnapshot() const
{
    if (m_options.snapshot != nullptr) {
        return m_options.snapshot;
    }
    if (m_context != nullptr && m_context->GetSnapshot() != nullptr) {
        return m_context->GetSnapshot();
    }
    if (thrd != nullptr && thrd->GetActiveTransaction() != nullptr) {
        return thrd->GetActiveTransaction()->GetSnapshot();
    }
    return nullptr;
}

bool HeapSegmentVerifier::ReadChunkTuple(const ItemPointerData &ctid, BufferDesc **bufferDesc, HeapPage **page,
    HeapDiskTuple **tuple, ItemId **itemId) const
{
    if (bufferDesc == nullptr || page == nullptr || tuple == nullptr || itemId == nullptr) {
        return false;
    }

    *bufferDesc = INVALID_BUFFER_DESC;
    *page = nullptr;
    *tuple = nullptr;
    *itemId = nullptr;

    HeapPage *chunkPage = m_pageSource->ReadHeapPage(ctid.GetPageId(), bufferDesc);
    if (chunkPage == nullptr) {
        return false;
    }

    if (ctid.GetOffset() < FIRST_ITEM_OFFSET_NUMBER || ctid.GetOffset() > chunkPage->GetMaxOffset()) {
        m_pageSource->ReleaseHeapPage(*bufferDesc);
        *bufferDesc = INVALID_BUFFER_DESC;
        return false;
    }

    ItemId *chunkItemId = chunkPage->GetItemIdPtr(ctid.GetOffset());
    if (!IsTupleStorageItem(chunkItemId)) {
        m_pageSource->ReleaseHeapPage(*bufferDesc);
        *bufferDesc = INVALID_BUFFER_DESC;
        return false;
    }

    *page = chunkPage;
    *tuple = chunkPage->GetDiskTuple(ctid.GetOffset());
    *itemId = chunkItemId;
    return *tuple != nullptr;
}

void HeapSegmentVerifier::ReportResult(VerifySeverity severity, const PageId &pageId, const char *checkName,
    uint64 expected, uint64 actual, const char *format, ...)
{
    if (m_context == nullptr || m_context->GetReport() == nullptr) {
        return;
    }

    VerifyResult result;
    result.severity = severity;
    result.targetType = HEAP_VERIFY_TARGET;
    result.targetId = pageId;
    result.checkName = checkName;
    result.expected = expected;
    result.actual = actual;

    va_list args;
    va_start(args, format);
    vsnprintf(result.message, sizeof(result.message), format, args);
    va_end(args);

    m_context->GetReport()->AddResult(result);
}

uint64 HeapSegmentVerifier::ItemPointerToUint64(const ItemPointerData &ctid)
{
    return (static_cast<uint64>(ctid.GetFileId()) << 48) | (static_cast<uint64>(ctid.GetBlockNum()) << 16) |
        ctid.GetOffset();
}

uint32 HeapSegmentVerifier::ResolveTupleValueOffset(HeapDiskTuple *tuple)
{
    if (tuple == nullptr) {
        return 0;
    }

    if (tuple->IsLinked() && tuple->IsFirstLinkChunk()) {
        return HeapDiskTuple::GetValuesOffset(tuple->GetNumColumn(), tuple->HasNull(), tuple->HasOid(), false) +
            LINKED_TUP_CHUNK_EXTRA_HEADER_SIZE;
    }
    return tuple->GetValuesOffset();
}

RetStatus VerifyHeapSegment(
    BufMgrInterface *bufMgr, StorageRelation heapRel, const HeapVerifyOptions &options, VerifyReport *report)
{
    if (bufMgr == nullptr || heapRel == nullptr || report == nullptr) {
        return DSTORE_FAIL;
    }

    VerifyContext context(report, options.snapshot, options.sampleRatio, options.isOnline, options.maxErrors);
    RelationHeapVerifyPageSource pageSource(bufMgr, heapRel);
    HeapSegmentVerifier verifier(&pageSource, options, &context);
    return verifier.Verify();
}

}  // namespace DSTORE

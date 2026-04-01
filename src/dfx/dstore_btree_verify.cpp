#include "dfx/dstore_btree_verify.h"

#include <cstdarg>
#include <cstdio>

#include "buffer/dstore_buf_mgr.h"
#include "catalog/dstore_function.h"
#include "framework/dstore_instance.h"
#include "dfx/dstore_page_verify.h"
#include "framework/dstore_thread.h"
#include "heap/dstore_heap_scan.h"
#include "index/dstore_btree.h"
#include "index/dstore_btree_scan.h"
#include "tuple/dstore_memheap_tuple.h"

namespace DSTORE {

namespace {

class RelationBtreeVerifyPageSource : public BtreeVerifyPageSource {
public:
    RelationBtreeVerifyPageSource(StorageRelation indexRel, StorageRelation heapRel, const BtreeVerifyOptions &options)
        : m_indexRel(indexRel),
          m_heapRel(heapRel),
          m_bufMgr((indexRel != nullptr && indexRel->btreeSmgr != nullptr && indexRel->btreeSmgr->IsGlobalTempIndex()) ?
              thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr())
    {
        m_snapshot.Init();
        if (options.snapshot != nullptr) {
            m_snapshot = *options.snapshot;
        }
        if (m_snapshot.GetSnapshotType() != SnapshotType::SNAPSHOT_MVCC) {
            m_snapshot.SetSnapshotType(SnapshotType::SNAPSHOT_MVCC);
        }

        if (m_heapRel != nullptr) {
            m_heapScan = DstoreNew(thrd->GetTransactionMemoryContext()) HeapScanHandler(g_storageInstance, thrd, m_heapRel);
            if (m_heapScan != nullptr) {
                (void)m_heapScan->Begin(&m_snapshot);
            }
        }
    }

    ~RelationBtreeVerifyPageSource() override
    {
        if (m_heapScan != nullptr) {
            m_heapScan->End();
            delete m_heapScan;
        }
    }

    bool GetRootInfo(PageId *rootPageId, uint32 *rootLevel) override
    {
        if (m_indexRel == nullptr || m_indexRel->btreeSmgr == nullptr || m_bufMgr == nullptr || rootPageId == nullptr ||
            rootLevel == nullptr) {
            return false;
        }

        BufferDesc *metaBuffer = INVALID_BUFFER_DESC;
        BtrMeta *meta = m_indexRel->btreeSmgr->GetBtrMeta(LW_SHARED, &metaBuffer);
        if (meta == nullptr || metaBuffer == INVALID_BUFFER_DESC) {
            return false;
        }

        *rootPageId = meta->GetRootPageId();
        *rootLevel = meta->GetRootLevel();
        m_bufMgr->UnlockAndRelease(metaBuffer);
        return rootPageId->IsValid();
    }

    IndexInfo *GetIndexInfo() override
    {
        return m_indexRel == nullptr ? nullptr : m_indexRel->index;
    }

    Int32Vector *GetIndexKeyMap() override
    {
        return m_indexRel == nullptr ? nullptr : m_indexRel->indKey;
    }

    TupleDesc GetHeapTupleDesc() override
    {
        return m_heapRel == nullptr ? nullptr : m_heapRel->attr;
    }

    BtrPage *ReadBtreePage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        if (m_bufMgr == nullptr || m_indexRel == nullptr || bufferDesc == nullptr) {
            return nullptr;
        }

        *bufferDesc = m_bufMgr->Read(m_indexRel->m_pdbId, pageId, LW_SHARED);
        if (*bufferDesc == INVALID_BUFFER_DESC) {
            return nullptr;
        }
        return static_cast<BtrPage *>((*bufferDesc)->GetPage());
    }

    void ReleaseBtreePage(BufferDesc *bufferDesc) override
    {
        if (bufferDesc != INVALID_BUFFER_DESC && m_bufMgr != nullptr) {
            m_bufMgr->UnlockAndRelease(bufferDesc);
        }
    }

    HeapTuple *FetchHeapTuple(const ItemPointerData &heapCtid, bool needCheckVisibility) override
    {
        if (m_heapScan == nullptr) {
            return nullptr;
        }

        ItemPointerData ctid = heapCtid;
        return m_heapScan->FetchTuple(ctid, needCheckVisibility);
    }

    void ReleaseHeapTuple(HeapTuple *heapTuple) override
    {
        if (heapTuple != nullptr) {
            DstorePfree(heapTuple);
        }
    }

private:
    StorageRelation m_indexRel{nullptr};
    StorageRelation m_heapRel{nullptr};
    BufMgrInterface *m_bufMgr{nullptr};
    HeapScanHandler *m_heapScan{nullptr};
    SnapshotData m_snapshot;
};

constexpr const char *BTREE_VERIFY_TARGET = "btree";

bool CompareNulls(Datum leftDatum, bool isLeftNull, Datum rightDatum, bool isRightNull, int16 indexOption, int *result)
{
    if (!isLeftNull && !isRightNull) {
        return false;
    }

    if (isLeftNull && isRightNull) {
        *result = 0;
        return true;
    }

    const bool nullsFirst = (static_cast<uint16>(indexOption) & INDEX_OPTION_NULLS_FIRST) != 0;
    *result = isLeftNull ? (nullsFirst ? -1 : 1) : (nullsFirst ? 1 : -1);
    (void)leftDatum;
    (void)rightDatum;
    return true;
}

int CompareIndexDatum(Datum leftDatum, bool isLeftNull, Datum rightDatum, bool isRightNull, IndexInfo *indexInfo,
    uint16 attrIndex)
{
    int result = 0;
    if (CompareNulls(leftDatum, isLeftNull, rightDatum, isRightNull, indexInfo->indexOption[attrIndex], &result)) {
        return result;
    }

    if (indexInfo->opcinType[attrIndex] == INT4OID) {
        const int32 leftValue = DatumGetInt32(leftDatum);
        const int32 rightValue = DatumGetInt32(rightDatum);
        return leftValue < rightValue ? -1 : (leftValue > rightValue ? 1 : 0);
    }

    FmgrInfo fmgrInfo;
    fmgrInfo.fnOid = DSTORE_INVALID_OID;
    FillProcFmgrInfo(indexInfo->m_indexSupportProcInfo, indexInfo->opcinType[attrIndex], attrIndex + 1, MAINTAIN_ORDER,
        fmgrInfo);
    result = DatumGetInt32(FunctionCall2Coll(&fmgrInfo, indexInfo->attributes->attrs[attrIndex]->attcollation,
        rightDatum, leftDatum));
    if (!(static_cast<uint16>(indexInfo->indexOption[attrIndex]) & INDEX_OPTION_DESC)) {
        InvertCompareResult(&result);
    }
    return result;
}

}  // namespace

BtreeVerifier::BtreeVerifier(
    BtreeVerifyPageSource *pageSource, const BtreeVerifyOptions &options, VerifyContext *context)
    : m_pageSource(pageSource), m_options(options), m_context(context)
{}

RetStatus BtreeVerifier::Verify()
{
    if (m_pageSource == nullptr || m_context == nullptr || m_context->GetReport() == nullptr) {
        return DSTORE_FAIL;
    }

    PageId rootPageId = INVALID_PAGE_ID;
    uint32 rootLevel = 0;
    if (!m_pageSource->GetRootInfo(&rootPageId, &rootLevel)) {
        ReportResult(VerifySeverity::ERROR_LEVEL, INVALID_PAGE_ID, "btree_root_missing", 1, 0,
            "Failed to resolve btree root page");
        return DSTORE_FAIL;
    }

    IndexInfo *indexInfo = m_pageSource->GetIndexInfo();
    if (indexInfo == nullptr) {
        ReportResult(VerifySeverity::ERROR_LEVEL, rootPageId, "btree_index_info_missing", 1, 0,
            "Failed to resolve btree index metadata for root page (%hu,%u)", rootPageId.m_fileId, rootPageId.m_blockId);
        return DSTORE_FAIL;
    }

    for (int32 level = static_cast<int32>(rootLevel); level >= 0; --level) {
        const PageId leftmostPageId = DescendToLevel(rootPageId, rootLevel, static_cast<uint32>(level));
        if (!leftmostPageId.IsValid()) {
            ReportResult(VerifySeverity::ERROR_LEVEL, rootPageId, "btree_leftmost_resolution_failed", rootLevel,
                static_cast<uint64>(level), "Failed to resolve leftmost page at level %d", level);
            return DSTORE_FAIL;
        }

        if (VerifyLevel(leftmostPageId, static_cast<uint32>(level), indexInfo) != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
    }

    return m_context->GetReport()->GetRetStatus();
}

RetStatus BtreeVerifier::VerifyLevel(const PageId &leftmostPageId, uint32 targetLevel, IndexInfo *indexInfo)
{
    PageId currentPageId = leftmostPageId;
    PageId previousPageId = INVALID_PAGE_ID;
    IndexTuple *previousLastTuple = nullptr;

    while (currentPageId.IsValid()) {
        if (!m_context->VisitPage(currentPageId)) {
            ReportResult(VerifySeverity::ERROR_LEVEL, currentPageId, "btree_sibling_cycle", 1, 0,
                "Detected sibling cycle while scanning level %u starting from (%hu,%u)",
                targetLevel, leftmostPageId.m_fileId, leftmostPageId.m_blockId);
            return DSTORE_FAIL;
        }

        BufferDesc *bufferDesc = INVALID_BUFFER_DESC;
        BtrPage *page = m_pageSource->ReadBtreePage(currentPageId, &bufferDesc);
        if (page == nullptr) {
            ReportResult(VerifySeverity::ERROR_LEVEL, currentPageId, "btree_page_read_failed", 1, 0,
                "Failed to read btree page (%hu,%u)", currentPageId.m_fileId, currentPageId.m_blockId);
            return DSTORE_FAIL;
        }

        const PageId nextPageId = page->GetRight();
        if (VerifyPageStructure(page, targetLevel, previousPageId) != DSTORE_SUCC) {
            m_pageSource->ReleaseBtreePage(bufferDesc);
            return DSTORE_FAIL;
        }

        IndexTuple *firstTuple = nullptr;
        IndexTuple *lastTuple = nullptr;
        if (VerifyPageKeyOrdering(page, indexInfo, &firstTuple, &lastTuple) != DSTORE_SUCC) {
            m_pageSource->ReleaseBtreePage(bufferDesc);
            return DSTORE_FAIL;
        }

        if (previousLastTuple != nullptr && firstTuple != nullptr &&
            VerifyCrossPageOrdering(previousPageId, previousLastTuple, currentPageId, firstTuple, indexInfo) != DSTORE_SUCC) {
            m_pageSource->ReleaseBtreePage(bufferDesc);
            return DSTORE_FAIL;
        }

        if (targetLevel == 0 && m_options.checkHeapConsistency && VerifyLeafTupleConsistency(page, indexInfo) != DSTORE_SUCC) {
            m_pageSource->ReleaseBtreePage(bufferDesc);
            return DSTORE_FAIL;
        }

        if (targetLevel > 0 && VerifyParentChildConsistency(page, indexInfo) != DSTORE_SUCC) {
            m_pageSource->ReleaseBtreePage(bufferDesc);
            return DSTORE_FAIL;
        }

        previousPageId = currentPageId;
        previousLastTuple = lastTuple;
        currentPageId = nextPageId;
        m_pageSource->ReleaseBtreePage(bufferDesc);
        if (m_context->HasReachedErrorLimit()) {
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

RetStatus BtreeVerifier::VerifyLeafTupleConsistency(BtrPage *page, IndexInfo *indexInfo)
{
    TupleDesc heapTupleDesc = nullptr;
    const bool shouldCheckDataConsistency = m_options.checkDataConsistency && ShouldSamplePage(page->GetSelfPageId());
    if (m_options.checkDataConsistency) {
        heapTupleDesc = m_pageSource->GetHeapTupleDesc();
        Int32Vector *indexKeyMap = m_pageSource->GetIndexKeyMap();
        if (heapTupleDesc == nullptr || indexKeyMap == nullptr) {
            ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_heap_metadata_missing", 1, 0,
                "Heap tuple descriptor or index key map is unavailable for page (%hu,%u)",
                page->GetFileId(), page->GetBlockNum());
            return DSTORE_FAIL;
        }
    }

    for (OffsetNumber offset = page->GetLinkAndStatus()->GetFirstDataOffset(); offset <= page->GetMaxOffset(); ++offset) {
        const ItemId *itemId = page->GetItemIdPtr(offset);
        if (!IsComparableItem(itemId)) {
            continue;
        }

        IndexTuple *tuple = page->GetIndexTuple(offset);
        if (VerifySingleLeafTuple(page, tuple, offset, indexInfo, shouldCheckDataConsistency ? heapTupleDesc : nullptr)
            != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

RetStatus BtreeVerifier::VerifySingleLeafTuple(
    BtrPage *page, IndexTuple *tuple, OffsetNumber offset, IndexInfo *indexInfo, TupleDesc heapTupleDesc)
{
    const ItemPointerData heapCtid = tuple->GetHeapCtid();
    if (heapCtid == INVALID_ITEM_POINTER) {
        ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_heap_ctid_missing", 1, 0,
            "Leaf tuple at offset %hu on page (%hu,%u) has invalid heap ctid", offset,
            page->GetFileId(), page->GetBlockNum());
        return DSTORE_FAIL;
    }

    HeapTuple *heapTuple = m_pageSource->FetchHeapTuple(heapCtid, m_context->IsOnline());
    if (heapTuple == nullptr && m_context->IsOnline()) {
        HeapTuple *invisibleTuple = m_pageSource->FetchHeapTuple(heapCtid, false);
        if (invisibleTuple != nullptr) {
            m_pageSource->ReleaseHeapTuple(invisibleTuple);
            ReportResult(VerifySeverity::INFO_LEVEL, page->GetSelfPageId(), "btree_heap_visibility_skipped", 0, 1,
                "Skipped leaf tuple at offset %hu on page (%hu,%u) because heap tuple (%hu,%u,%hu) is not visible online",
                offset, page->GetFileId(), page->GetBlockNum(), heapCtid.GetFileId(), heapCtid.GetBlockNum(),
                heapCtid.GetOffset());
            return DSTORE_SUCC;
        }
    }

    if (heapTuple == nullptr) {
        ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_heap_tuple_missing", 1, 0,
            "Leaf tuple at offset %hu on page (%hu,%u) points to missing heap tuple (%hu,%u,%hu)",
            offset, page->GetFileId(), page->GetBlockNum(), heapCtid.GetFileId(), heapCtid.GetBlockNum(),
            heapCtid.GetOffset());
        return DSTORE_FAIL;
    }

    RetStatus status = DSTORE_SUCC;
    if (m_options.checkDataConsistency && heapTupleDesc != nullptr) {
        status = VerifyLeafTupleDataConsistency(tuple, heapTuple, page->GetSelfPageId(), offset,
            indexInfo, heapTupleDesc, m_pageSource->GetIndexKeyMap());
    }

    m_pageSource->ReleaseHeapTuple(heapTuple);
    return status;
}

RetStatus BtreeVerifier::VerifyLeafTupleDataConsistency(IndexTuple *indexTuple, HeapTuple *heapTuple,
    const PageId &pageId, OffsetNumber offset, IndexInfo *indexInfo, TupleDesc heapTupleDesc, Int32Vector *indexKeyMap)
{
    StorageAssert(indexInfo != nullptr);
    StorageAssert(indexKeyMap != nullptr);
    StorageAssert(heapTupleDesc != nullptr);

    for (uint16 attr = 0; attr < indexInfo->indexKeyAttrsNum; ++attr) {
        if (attr >= static_cast<uint16>(indexKeyMap->dim1)) {
            ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "btree_index_key_map_short", indexInfo->indexKeyAttrsNum,
                indexKeyMap->dim1, "Index key map for page (%hu,%u) is shorter than index key count",
                pageId.m_fileId, pageId.m_blockId);
            return DSTORE_FAIL;
        }

        const int16 heapAttNum = indexKeyMap->values[attr];
        if (heapAttNum <= 0) {
            ReportResult(VerifySeverity::INFO_LEVEL, pageId, "btree_index_expression_skipped", 0, 1,
                "Skipped data consistency for expression-based index attr %hu on page (%hu,%u)",
                static_cast<uint16>(attr + 1), pageId.m_fileId, pageId.m_blockId);
            continue;
        }

        bool isIndexNull = false;
        bool isHeapNull = false;
        const Datum indexDatum = indexTuple->GetAttr(attr + 1, indexInfo->attributes, &isIndexNull);
        const Datum heapDatum = heapTuple->GetAttr(heapAttNum, heapTupleDesc, &isHeapNull);
        if (CompareIndexDatum(indexDatum, isIndexNull, heapDatum, isHeapNull, indexInfo, attr) != 0) {
            ReportResult(VerifySeverity::ERROR_LEVEL, pageId, "btree_index_heap_value_mismatch", 1, 0,
                "Leaf tuple at offset %hu on page (%hu,%u) mismatches heap tuple attr %hd for index attr %hu",
                offset, pageId.m_fileId, pageId.m_blockId, heapAttNum, static_cast<uint16>(attr + 1));
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

RetStatus BtreeVerifier::VerifyPageStructure(BtrPage *page, uint32 expectedLevel, const PageId &previousPageId)
{
    VerifyReport *report = m_context->GetReport();
    if (VerifyPage(page, VerifyLevel::HEAVYWEIGHT, report) != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }

    BtrPageLinkAndStatus *link = page->GetLinkAndStatus();
    if (link == nullptr) {
        ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_link_header_missing", 1, 0,
            "Page (%hu,%u) is missing btree link metadata", page->GetFileId(), page->GetBlockNum());
        return DSTORE_FAIL;
    }

    if (page->GetLevel() != expectedLevel) {
        ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_level_mismatch", expectedLevel,
            page->GetLevel(), "Page (%hu,%u) level %u != expected level %u", page->GetFileId(), page->GetBlockNum(),
            page->GetLevel(), expectedLevel);
        return DSTORE_FAIL;
    }

    const BtrPageType expectedType = expectedLevel == 0 ? BtrPageType::LEAF_PAGE : BtrPageType::INTERNAL_PAGE;
    if (!link->TestType(expectedType)) {
        ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_page_type_mismatch",
            static_cast<uint64>(expectedType), link->GetType(),
            "Page (%hu,%u) type %hu does not match expected btree type for level %u",
            page->GetFileId(), page->GetBlockNum(), link->GetType(), expectedLevel);
        return DSTORE_FAIL;
    }

    if (previousPageId.IsValid()) {
        if (page->GetLeft() != previousPageId) {
            ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_left_sibling_mismatch", 1, 0,
                "Page (%hu,%u) left sibling (%hu,%u) does not match previous page (%hu,%u)",
                page->GetFileId(), page->GetBlockNum(), page->GetLeft().m_fileId, page->GetLeft().m_blockId,
                previousPageId.m_fileId, previousPageId.m_blockId);
            return DSTORE_FAIL;
        }
    } else if (!page->IsLeftmost()) {
        ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_leftmost_invariant_broken", 1, 0,
            "Level leftmost page (%hu,%u) unexpectedly has a left sibling (%hu,%u)", page->GetFileId(),
            page->GetBlockNum(), page->GetLeft().m_fileId, page->GetLeft().m_blockId);
        return DSTORE_FAIL;
    }

    if (!link->IsSplitComplete()) {
        ReportResult(VerifySeverity::WARNING_LEVEL, page->GetSelfPageId(), "btree_split_incomplete", 0, 1,
            "Page (%hu,%u) is marked split-incomplete during btree verification", page->GetFileId(),
            page->GetBlockNum());
    }

    return DSTORE_SUCC;
}

RetStatus BtreeVerifier::VerifyPageKeyOrdering(
    BtrPage *page, IndexInfo *indexInfo, IndexTuple **firstTuple, IndexTuple **lastTuple)
{
    IndexTuple *previousTuple = nullptr;
    IndexTuple *highKey = nullptr;
    if (!page->IsRightmost()) {
        const ItemId *highKeyItemId = page->GetItemIdPtr(BTREE_PAGE_HIKEY);
        if (!IsComparableItem(highKeyItemId)) {
            ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_high_key_missing", 1, 0,
                "Non-rightmost page (%hu,%u) is missing a valid high key at offset %u",
                page->GetFileId(), page->GetBlockNum(), BTREE_PAGE_HIKEY);
            return DSTORE_FAIL;
        }
        highKey = page->GetIndexTuple(BTREE_PAGE_HIKEY);
    }

    for (OffsetNumber offset = page->GetLinkAndStatus()->GetFirstDataOffset(); offset <= page->GetMaxOffset(); ++offset) {
        const ItemId *itemId = page->GetItemIdPtr(offset);
        if (!IsComparableItem(itemId)) {
            continue;
        }

        IndexTuple *tuple = page->GetIndexTuple(offset);
        if (*firstTuple == nullptr) {
            *firstTuple = tuple;
        }
        *lastTuple = tuple;

        if (previousTuple != nullptr && CompareTupleKeys(previousTuple, tuple, indexInfo, true) > 0) {
            ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_intra_page_order_violation", 1, 0,
                "Tuple ordering is not monotonic on page (%hu,%u) between offsets %hu and %hu", page->GetFileId(),
                page->GetBlockNum(), static_cast<uint16>(offset - 1), offset);
            return DSTORE_FAIL;
        }

        if (highKey != nullptr && CompareTupleKeys(tuple, highKey, indexInfo, false) > 0) {
            ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_high_key_violation", 1, 0,
                "Tuple at offset %hu exceeds high key on page (%hu,%u)", offset, page->GetFileId(),
                page->GetBlockNum());
            return DSTORE_FAIL;
        }

        previousTuple = tuple;
    }

    return DSTORE_SUCC;
}

RetStatus BtreeVerifier::VerifyCrossPageOrdering(const PageId &leftPageId, IndexTuple *leftLastTuple,
    const PageId &rightPageId, IndexTuple *rightFirstTuple, IndexInfo *indexInfo)
{
    if (leftLastTuple == nullptr || rightFirstTuple == nullptr) {
        return DSTORE_SUCC;
    }

    if (CompareTupleKeys(leftLastTuple, rightFirstTuple, indexInfo, true) > 0) {
        ReportResult(VerifySeverity::ERROR_LEVEL, rightPageId, "btree_cross_page_order_violation", 1, 0,
            "Sibling pages (%hu,%u) and (%hu,%u) are out of order at the page boundary",
            leftPageId.m_fileId, leftPageId.m_blockId, rightPageId.m_fileId, rightPageId.m_blockId);
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus BtreeVerifier::VerifyParentChildConsistency(BtrPage *page, IndexInfo *indexInfo)
{
    for (OffsetNumber offset = page->GetLinkAndStatus()->GetFirstDataOffset(); offset <= page->GetMaxOffset(); ++offset) {
        const ItemId *itemId = page->GetItemIdPtr(offset);
        if (!IsComparableItem(itemId)) {
            continue;
        }

        IndexTuple *parentTuple = page->GetIndexTuple(offset);
        const PageId childPageId = parentTuple->GetLowlevelIndexpageLink();
        BufferDesc *childBuffer = INVALID_BUFFER_DESC;
        BtrPage *childPage = m_pageSource->ReadBtreePage(childPageId, &childBuffer);
        if (childPage == nullptr) {
            ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_child_read_failed", 1, 0,
                "Failed to read child page (%hu,%u) referenced from page (%hu,%u) offset %hu",
                childPageId.m_fileId, childPageId.m_blockId, page->GetFileId(), page->GetBlockNum(), offset);
            return DSTORE_FAIL;
        }

        if (childPage->GetLevel() + 1 != page->GetLevel()) {
            m_pageSource->ReleaseBtreePage(childBuffer);
            ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_parent_child_level_mismatch",
                page->GetLevel() - 1, childPage->GetLevel(),
                "Child page (%hu,%u) level %u does not match parent (%hu,%u) level %u",
                childPageId.m_fileId, childPageId.m_blockId, childPage->GetLevel(), page->GetFileId(),
                page->GetBlockNum(), page->GetLevel());
            return DSTORE_FAIL;
        }

        IndexTuple *childBoundary = nullptr;
        if (!childPage->IsRightmost()) {
            const ItemId *highKeyItemId = childPage->GetItemIdPtr(BTREE_PAGE_HIKEY);
            if (IsComparableItem(highKeyItemId)) {
                childBoundary = childPage->GetIndexTuple(BTREE_PAGE_HIKEY);
            }
        }
        if (childBoundary == nullptr) {
            const OffsetNumber firstDataOffset = childPage->GetLinkAndStatus()->GetFirstDataOffset();
            const ItemId *firstItemId = firstDataOffset <= childPage->GetMaxOffset() ?
                childPage->GetItemIdPtr(firstDataOffset) : nullptr;
            if (IsComparableItem(firstItemId)) {
                childBoundary = childPage->GetIndexTuple(firstDataOffset);
            }
        }

        if (childBoundary == nullptr || CompareTupleKeys(parentTuple, childBoundary, indexInfo, false) != 0) {
            m_pageSource->ReleaseBtreePage(childBuffer);
            ReportResult(VerifySeverity::ERROR_LEVEL, page->GetSelfPageId(), "btree_parent_child_key_mismatch", 1, 0,
                "Parent page (%hu,%u) offset %hu does not match child page (%hu,%u) boundary tuple",
                page->GetFileId(), page->GetBlockNum(), offset, childPageId.m_fileId, childPageId.m_blockId);
            return DSTORE_FAIL;
        }

        m_pageSource->ReleaseBtreePage(childBuffer);
    }

    return DSTORE_SUCC;
}

PageId BtreeVerifier::DescendToLevel(const PageId &rootPageId, uint32 rootLevel, uint32 targetLevel)
{
    PageId currentPageId = rootPageId;
    uint32 currentLevel = rootLevel;

    while (currentPageId.IsValid() && currentLevel > targetLevel) {
        BufferDesc *bufferDesc = INVALID_BUFFER_DESC;
        BtrPage *page = m_pageSource->ReadBtreePage(currentPageId, &bufferDesc);
        if (page == nullptr) {
            return INVALID_PAGE_ID;
        }

        const OffsetNumber childOffset = page->GetLinkAndStatus()->GetFirstDataOffset();
        const ItemId *itemId = childOffset <= page->GetMaxOffset() ? page->GetItemIdPtr(childOffset) : nullptr;
        if (!IsComparableItem(itemId)) {
            m_pageSource->ReleaseBtreePage(bufferDesc);
            return INVALID_PAGE_ID;
        }

        currentPageId = page->GetIndexTuple(childOffset)->GetLowlevelIndexpageLink();
        --currentLevel;
        m_pageSource->ReleaseBtreePage(bufferDesc);
    }

    return currentLevel == targetLevel ? currentPageId : INVALID_PAGE_ID;
}

void BtreeVerifier::ReportResult(VerifySeverity severity, const PageId &pageId, const char *checkName, uint64 expected,
    uint64 actual, const char *format, ...)
{
    if (m_context == nullptr || m_context->GetReport() == nullptr) {
        return;
    }

    char detail[256] = {0};
    va_list args;
    va_start(args, format);
    (void)vsnprintf(detail, sizeof(detail), format, args);
    va_end(args);

    m_context->GetReport()->AddResult(severity, BTREE_VERIFY_TARGET, pageId, checkName, expected, actual, "%s", detail);
}

bool BtreeVerifier::IsComparableItem(const ItemId *itemId)
{
    return itemId != nullptr && itemId->IsNormal();
}

int BtreeVerifier::CompareTupleKeys(IndexTuple *left, IndexTuple *right, IndexInfo *indexInfo, bool compareHeapTids)
{
    StorageAssert(left != nullptr);
    StorageAssert(right != nullptr);
    StorageAssert(indexInfo != nullptr);

    const uint16 leftKeyNum = left->GetKeyNum(indexInfo->indexKeyAttrsNum);
    const uint16 rightKeyNum = right->GetKeyNum(indexInfo->indexKeyAttrsNum);
    const uint16 minKeyNum = DstoreMin(leftKeyNum, rightKeyNum);

    for (uint16 attr = 0; attr < minKeyNum; ++attr) {
        bool isLeftNull = false;
        bool isRightNull = false;
        const Datum leftDatum = left->GetAttr(attr + 1, indexInfo->attributes, &isLeftNull);
        const Datum rightDatum = right->GetAttr(attr + 1, indexInfo->attributes, &isRightNull);
        const int result = CompareIndexDatum(leftDatum, isLeftNull, rightDatum, isRightNull, indexInfo, attr);

        if (result != 0) {
            return result;
        }
    }

    if (leftKeyNum != rightKeyNum) {
        return leftKeyNum < rightKeyNum ? -1 : 1;
    }

    if (!compareHeapTids) {
        return 0;
    }

    ItemPointerData leftCtid = left->GetHeapCtid();
    ItemPointerData rightCtid = right->GetHeapCtid();
    if (leftCtid == INVALID_ITEM_POINTER || rightCtid == INVALID_ITEM_POINTER) {
        return 0;
    }

    return ItemPointerData::Compare(&leftCtid, &rightCtid);
}

bool BtreeVerifier::ShouldSamplePage(const PageId &pageId) const
{
    const float sampleRatio = m_context == nullptr ? 1.0F : m_context->GetSampleRatio();
    if (sampleRatio >= 1.0F) {
        return true;
    }
    if (sampleRatio <= 0.0F) {
        return false;
    }

    const uint64 hashValue = (static_cast<uint64>(pageId.m_fileId) << 32) ^
        (static_cast<uint64>(pageId.m_blockId) * 2654435761U);
    constexpr uint64 HASH_SCALE = 1000000;
    const double normalized = static_cast<double>(hashValue % HASH_SCALE) / static_cast<double>(HASH_SCALE);
    return normalized < static_cast<double>(sampleRatio);
}

RetStatus VerifyBtreeIndex(StorageRelation indexRel, StorageRelation heapRel, const BtreeVerifyOptions &options,
    VerifyReport *report)
{
    if (indexRel == nullptr || report == nullptr) {
        return DSTORE_FAIL;
    }

    VerifyContext context(report, options.snapshot, options.sampleRatio, options.isOnline, options.maxErrors);
    RelationBtreeVerifyPageSource pageSource(indexRel, heapRel, options);
    BtreeVerifier verifier(&pageSource, options, &context);
    return verifier.Verify();
}

}  // namespace DSTORE

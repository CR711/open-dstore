#include "dfx/dstore_segment_verify.h"

#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <unordered_set>

#include "buffer/dstore_buf_mgr.h"
#include "dfx/dstore_page_verify.h"
#include "framework/dstore_instance.h"
#include "page/dstore_btr_recycle_root_meta_page.h"
#include "tablespace/dstore_index_segment.h"
#include "tablespace/dstore_tablespace_internal.h"
#include "tablespace/dstore_segment.h"
#include "page/dstore_undo_segment_meta_page.h"

namespace DSTORE {

namespace {

class BufferSegmentVerifyPageSource : public SegmentVerifyPageSource {
public:
    explicit BufferSegmentVerifyPageSource(BufMgrInterface *bufMgr) : m_bufMgr(bufMgr) {}

    SegmentMetaPage *ReadSegmentMetaPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return static_cast<SegmentMetaPage *>(ReadTypedPage(pageId, bufferDesc));
    }

    SegExtentMetaPage *ReadExtentMetaPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return static_cast<SegExtentMetaPage *>(ReadTypedPage(pageId, bufferDesc));
    }

    Page *ReadPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return ReadTypedPage(pageId, bufferDesc);
    }

    TbsBitmapMetaPage *ReadBitmapMetaPage(FileId fileId, BufferDesc **bufferDesc) override
    {
        return static_cast<TbsBitmapMetaPage *>(ReadTypedPage({fileId, TBS_BITMAP_META_PAGE}, bufferDesc));
    }

    TbsBitmapPage *ReadBitmapPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return static_cast<TbsBitmapPage *>(ReadTypedPage(pageId, bufferDesc));
    }

    bool GetIndexRootInfo(const PageId &segmentMetaPageId, PageId *rootPageId, uint32 *rootLevel) override
    {
        if (rootPageId == nullptr || rootLevel == nullptr || m_bufMgr == nullptr) {
            return false;
        }

        BufferDesc *bufferDesc = INVALID_BUFFER_DESC;
        BtrPage *metaPage =
            static_cast<BtrPage *>(ReadTypedPage({segmentMetaPageId.m_fileId, segmentMetaPageId.m_blockId + 1}, &bufferDesc));
        if (metaPage == nullptr) {
            return false;
        }

        BtrMeta *meta = static_cast<BtrMeta *>(static_cast<void *>(metaPage->GetData()));
        *rootPageId = meta->GetRootPageId();
        *rootLevel = meta->GetRootLevel();
        ReleasePage(bufferDesc);
        return rootPageId->IsValid();
    }

    void ReleasePage(BufferDesc *bufferDesc) override
    {
        if (bufferDesc != INVALID_BUFFER_DESC && m_bufMgr != nullptr) {
            m_bufMgr->UnlockAndRelease(bufferDesc);
        }
    }

private:
    Page *ReadTypedPage(const PageId &pageId, BufferDesc **bufferDesc)
    {
        if (bufferDesc == nullptr || m_bufMgr == nullptr) {
            return nullptr;
        }

        *bufferDesc = m_bufMgr->Read(g_defaultPdbId, pageId, LW_SHARED);
        if (*bufferDesc == INVALID_BUFFER_DESC) {
            return nullptr;
        }
        return static_cast<Page *>((*bufferDesc)->GetPage());
    }

    BufMgrInterface *m_bufMgr{nullptr};
};

constexpr const char *SEGMENT_VERIFY_TARGET = "segment";

bool IsDataSegmentType(SegmentType segmentType)
{
    return segmentType == SegmentType::HEAP_SEGMENT_TYPE || segmentType == SegmentType::INDEX_SEGMENT_TYPE ||
        segmentType == SegmentType::HEAP_TEMP_SEGMENT_TYPE || segmentType == SegmentType::INDEX_TEMP_SEGMENT_TYPE;
}

bool IsIndexSegmentType(SegmentType segmentType)
{
    return segmentType == SegmentType::INDEX_SEGMENT_TYPE || segmentType == SegmentType::INDEX_TEMP_SEGMENT_TYPE;
}

uint64 PageIdToUint64(const PageId &pageId)
{
    return (static_cast<uint64>(pageId.m_fileId) << 32) | pageId.m_blockId;
}

}  // namespace

SegmentVerifier::SegmentVerifier(SegmentVerifyPageSource *pageSource, const PageId &segmentMetaPageId,
    const SegmentVerifyOptions &options, VerifyContext *context)
    : m_pageSource(pageSource), m_segmentMetaPageId(segmentMetaPageId), m_options(options), m_context(context)
{}

RetStatus SegmentVerifier::Verify()
{
    if (m_pageSource == nullptr || m_context == nullptr || m_context->GetReport() == nullptr ||
        !m_segmentMetaPageId.IsValid()) {
        return DSTORE_FAIL;
    }

    BufferDesc *bufferDesc = INVALID_BUFFER_DESC;
    SegmentMetaPage *segmentMetaPage = m_pageSource->ReadSegmentMetaPage(m_segmentMetaPageId, &bufferDesc);
    if (segmentMetaPage == nullptr) {
        ReportResult(VerifySeverity::ERROR_LEVEL, m_segmentMetaPageId, "segment_meta_read_failed", 1, 0,
            "Failed to read segment meta page (%hu,%u)", m_segmentMetaPageId.m_fileId, m_segmentMetaPageId.m_blockId);
        return DSTORE_FAIL;
    }

    std::vector<ExtentInfo> extents;
    uint64 walkedTotalBlocks = 0;
    RetStatus ret = WalkExtentChain(segmentMetaPage, &extents, &walkedTotalBlocks);
    if (ret == DSTORE_SUCC) {
        ret = VerifySegmentMetadata(segmentMetaPage, extents, walkedTotalBlocks);
    }
    if (ret == DSTORE_SUCC && m_options.checkExtentBitmap) {
        ret = VerifyExtentBitmapConsistency(extents);
    }
    if (ret == DSTORE_SUCC && m_options.checkPageCounts) {
        ret = VerifyIndexLeafPageCount(segmentMetaPage, extents);
    }

    m_pageSource->ReleasePage(bufferDesc);
    return ret == DSTORE_SUCC ? m_context->GetReport()->GetRetStatus() : DSTORE_FAIL;
}

RetStatus SegmentVerifier::WalkExtentChain(
    SegmentMetaPage *segmentMetaPage, std::vector<ExtentInfo> *extents, uint64 *totalBlocks)
{
    if (segmentMetaPage == nullptr || extents == nullptr || totalBlocks == nullptr) {
        return DSTORE_FAIL;
    }

    if (VerifyPage(segmentMetaPage, VerifyLevel::HEAVYWEIGHT, m_context->GetReport()) != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }

    extents->push_back({segmentMetaPage->GetSelfPageId(), segmentMetaPage->GetSelfExtentSize()});
    *totalBlocks += segmentMetaPage->GetSelfExtentSize();

    if (m_options.checkExtentChain) {
        const uint16 expectedFirstSize = ResolveExpectedExtentSize(
            segmentMetaPage->segmentHeader.segmentType, 0, segmentMetaPage->GetSelfExtentSize());
        if (segmentMetaPage->GetSelfExtentSize() != expectedFirstSize) {
            ReportResult(VerifySeverity::ERROR_LEVEL, segmentMetaPage->GetSelfPageId(), "extent_size_invalid",
                expectedFirstSize, segmentMetaPage->GetSelfExtentSize(),
                "First extent (%hu,%u) has size %hu but expected %hu", segmentMetaPage->GetFileId(),
                segmentMetaPage->GetBlockNum(), segmentMetaPage->GetSelfExtentSize(), expectedFirstSize);
            return DSTORE_FAIL;
        }
    }

    PageId currentExtentId = segmentMetaPage->GetNextExtentMetaPageId();
    uint64 extentIndex = 1;
    while (currentExtentId.IsValid()) {
        if (!m_context->VisitPage(currentExtentId)) {
            ReportResult(VerifySeverity::ERROR_LEVEL, currentExtentId, "extent_chain_cycle", 1, 0,
                "Detected extent chain cycle at (%hu,%u)", currentExtentId.m_fileId, currentExtentId.m_blockId);
            return DSTORE_FAIL;
        }

        BufferDesc *extentBuffer = INVALID_BUFFER_DESC;
        SegExtentMetaPage *extentPage = m_pageSource->ReadExtentMetaPage(currentExtentId, &extentBuffer);
        if (extentPage == nullptr) {
            ReportResult(VerifySeverity::ERROR_LEVEL, currentExtentId, "extent_chain_broken", 1, 0,
                "Failed to read extent meta page (%hu,%u)", currentExtentId.m_fileId, currentExtentId.m_blockId);
            return DSTORE_FAIL;
        }

        if (VerifyPage(extentPage, VerifyLevel::HEAVYWEIGHT, m_context->GetReport()) != DSTORE_SUCC) {
            m_pageSource->ReleasePage(extentBuffer);
            return DSTORE_FAIL;
        }

        if (extentPage->extentMeta.magic != EXTENT_META_MAGIC) {
            ReportResult(VerifySeverity::ERROR_LEVEL, currentExtentId, "extent_magic_invalid", EXTENT_META_MAGIC,
                extentPage->extentMeta.magic, "Extent (%hu,%u) magic is invalid", currentExtentId.m_fileId,
                currentExtentId.m_blockId);
            m_pageSource->ReleasePage(extentBuffer);
            return DSTORE_FAIL;
        }

        const uint16 actualSize = extentPage->GetSelfExtentSize();
        const uint16 expectedSize =
            ResolveExpectedExtentSize(segmentMetaPage->segmentHeader.segmentType, extentIndex, actualSize);
        if (actualSize != expectedSize) {
            ReportResult(VerifySeverity::ERROR_LEVEL, currentExtentId, "extent_size_invalid", expectedSize, actualSize,
                "Extent (%hu,%u) has size %hu but expected %hu for extent index %lu", currentExtentId.m_fileId,
                currentExtentId.m_blockId, actualSize, expectedSize, extentIndex);
            m_pageSource->ReleasePage(extentBuffer);
            return DSTORE_FAIL;
        }

        extents->push_back({currentExtentId, actualSize});
        *totalBlocks += actualSize;
        currentExtentId = extentPage->GetNextExtentMetaPageId();
        ++extentIndex;
        m_pageSource->ReleasePage(extentBuffer);
    }

    return DSTORE_SUCC;
}

RetStatus SegmentVerifier::VerifySegmentMetadata(
    SegmentMetaPage *segmentMetaPage, const std::vector<ExtentInfo> &extents, uint64 walkedTotalBlocks)
{
    if (segmentMetaPage->extentMeta.magic != SEGMENT_META_MAGIC) {
        ReportResult(VerifySeverity::ERROR_LEVEL, segmentMetaPage->GetSelfPageId(), "segment_magic_invalid",
            SEGMENT_META_MAGIC, segmentMetaPage->extentMeta.magic, "Segment meta page magic is invalid");
        return DSTORE_FAIL;
    }

    const SegmentType segmentType = segmentMetaPage->segmentHeader.segmentType;
    if (!IsDataSegmentType(segmentType) && segmentType != SegmentType::UNDO_SEGMENT_TYPE) {
        ReportResult(VerifySeverity::ERROR_LEVEL, segmentMetaPage->GetSelfPageId(), "segment_type_invalid", 1,
            static_cast<uint64>(segmentType), "Segment (%hu,%u) has unsupported type %u", segmentMetaPage->GetFileId(),
            segmentMetaPage->GetBlockNum(), static_cast<uint8>(segmentType));
        return DSTORE_FAIL;
    }

    if (m_options.checkPageCounts && segmentMetaPage->GetTotalBlockCount() != walkedTotalBlocks) {
        ReportResult(VerifySeverity::ERROR_LEVEL, segmentMetaPage->GetSelfPageId(), "block_count_mismatch",
            segmentMetaPage->GetTotalBlockCount(), walkedTotalBlocks,
            "Segment (%hu,%u) totalBlockCount %lu mismatches walked extent blocks %lu", segmentMetaPage->GetFileId(),
            segmentMetaPage->GetBlockNum(), segmentMetaPage->GetTotalBlockCount(), walkedTotalBlocks);
        return DSTORE_FAIL;
    }

    if (segmentMetaPage->GetExtentCount() != extents.size()) {
        ReportResult(VerifySeverity::ERROR_LEVEL, segmentMetaPage->GetSelfPageId(), "extent_count_mismatch",
            segmentMetaPage->GetExtentCount(), extents.size(),
            "Segment (%hu,%u) extent count %lu mismatches walked extent count %lu", segmentMetaPage->GetFileId(),
            segmentMetaPage->GetBlockNum(), segmentMetaPage->GetExtentCount(), extents.size());
        return DSTORE_FAIL;
    }

    if (IsDataSegmentType(segmentType)) {
        auto *dataMetaPage = static_cast<DataSegmentMetaPage *>(static_cast<void *>(segmentMetaPage));
        const bool rangeUnset = dataMetaPage->dataFirst.IsInvalid() || dataMetaPage->dataLast.IsInvalid();
        if ((dataMetaPage->GetDataBlockCount() == 0) != rangeUnset) {
            ReportResult(VerifySeverity::ERROR_LEVEL, segmentMetaPage->GetSelfPageId(), "data_range_mismatch",
                dataMetaPage->GetDataBlockCount() == 0, rangeUnset,
                "Segment (%hu,%u) dataBlockCount/dataFirst/dataLast are inconsistent", segmentMetaPage->GetFileId(),
                segmentMetaPage->GetBlockNum());
            return DSTORE_FAIL;
        }

        if (!rangeUnset &&
            (!IsPageInExtents(dataMetaPage->dataFirst, extents) || !IsPageInExtents(dataMetaPage->dataLast, extents) ||
                PageIdToUint64(dataMetaPage->dataFirst) > PageIdToUint64(dataMetaPage->dataLast))) {
            ReportResult(VerifySeverity::ERROR_LEVEL, segmentMetaPage->GetSelfPageId(), "data_range_mismatch", 1, 0,
                "Segment (%hu,%u) dataFirst/dataLast fall outside extent ranges", segmentMetaPage->GetFileId(),
                segmentMetaPage->GetBlockNum());
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

RetStatus SegmentVerifier::VerifyExtentBitmapConsistency(const std::vector<ExtentInfo> &extents)
{
    std::vector<std::pair<uint64, uint64>> ranges;
    std::unordered_set<uint64> validatedBitmapPages;

    for (const ExtentInfo &extent : extents) {
        ranges.emplace_back(extent.pageId.m_blockId, extent.pageId.m_blockId + extent.extentSize - 1);

        BufferDesc *bitmapMetaBuffer = INVALID_BUFFER_DESC;
        TbsBitmapMetaPage *bitmapMetaPage = m_pageSource->ReadBitmapMetaPage(extent.pageId.m_fileId, &bitmapMetaBuffer);
        if (bitmapMetaPage == nullptr) {
            ReportResult(VerifySeverity::ERROR_LEVEL, extent.pageId, "bitmap_meta_read_failed", 1, 0,
                "Failed to read bitmap meta page for file %hu", extent.pageId.m_fileId);
            return DSTORE_FAIL;
        }

        PageId bitmapPageId = INVALID_PAGE_ID;
        uint16 bitNo = 0;
        if (!LocateBitmapBit(bitmapMetaPage, extent.pageId, &bitmapPageId, &bitNo)) {
            ReportResult(VerifySeverity::ERROR_LEVEL, extent.pageId, "extent_not_in_bitmap", 1, 0,
                "Extent (%hu,%u) cannot be located in tablespace bitmap", extent.pageId.m_fileId,
                extent.pageId.m_blockId);
            m_pageSource->ReleasePage(bitmapMetaBuffer);
            return DSTORE_FAIL;
        }

        BufferDesc *bitmapBuffer = INVALID_BUFFER_DESC;
        TbsBitmapPage *bitmapPage = m_pageSource->ReadBitmapPage(bitmapPageId, &bitmapBuffer);
        if (bitmapPage == nullptr) {
            ReportResult(VerifySeverity::ERROR_LEVEL, extent.pageId, "bitmap_page_read_failed", 1, 0,
                "Failed to read bitmap page (%hu,%u)", bitmapPageId.m_fileId, bitmapPageId.m_blockId);
            m_pageSource->ReleasePage(bitmapMetaBuffer);
            return DSTORE_FAIL;
        }

        if (bitmapPage->TestBitZero(bitNo)) {
            ReportResult(VerifySeverity::ERROR_LEVEL, extent.pageId, "extent_not_in_bitmap", 1, 0,
                "Extent (%hu,%u) is not marked allocated in bitmap page (%hu,%u) bit %hu", extent.pageId.m_fileId,
                extent.pageId.m_blockId, bitmapPageId.m_fileId, bitmapPageId.m_blockId, bitNo);
            m_pageSource->ReleasePage(bitmapBuffer);
            m_pageSource->ReleasePage(bitmapMetaBuffer);
            return DSTORE_FAIL;
        }

        const uint64 bitmapKey = PageIdToUint64(bitmapPageId);
        if (validatedBitmapPages.insert(bitmapKey).second) {
            const uint16 popCount = CountSetBits(bitmapPage);
            if (popCount != bitmapPage->allocatedExtentCount) {
                ReportResult(VerifySeverity::ERROR_LEVEL, bitmapPageId, "bitmap_count_mismatch",
                    bitmapPage->allocatedExtentCount, popCount,
                    "Bitmap page (%hu,%u) allocatedExtentCount %hu mismatches popcount %hu",
                    bitmapPageId.m_fileId, bitmapPageId.m_blockId, bitmapPage->allocatedExtentCount, popCount);
                m_pageSource->ReleasePage(bitmapBuffer);
                m_pageSource->ReleasePage(bitmapMetaBuffer);
                return DSTORE_FAIL;
            }
        }

        m_pageSource->ReleasePage(bitmapBuffer);
        m_pageSource->ReleasePage(bitmapMetaBuffer);
    }

    std::sort(ranges.begin(), ranges.end());
    for (size_t i = 1; i < ranges.size(); ++i) {
        if (ranges[i - 1].second >= ranges[i].first) {
            ReportResult(VerifySeverity::ERROR_LEVEL, m_segmentMetaPageId, "extent_overlap", ranges[i - 1].second,
                ranges[i].first, "Segment (%hu,%u) has overlapping extent ranges [%lu,%lu] and [%lu,%lu]",
                m_segmentMetaPageId.m_fileId, m_segmentMetaPageId.m_blockId, ranges[i - 1].first, ranges[i - 1].second,
                ranges[i].first, ranges[i].second);
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

RetStatus SegmentVerifier::VerifyIndexLeafPageCount(
    SegmentMetaPage *segmentMetaPage, const std::vector<ExtentInfo> &extents)
{
    if (!IsIndexSegmentType(segmentMetaPage->segmentHeader.segmentType)) {
        return DSTORE_SUCC;
    }

    PageId rootPageId = INVALID_PAGE_ID;
    uint32 rootLevel = 0;
    if (!m_pageSource->GetIndexRootInfo(segmentMetaPage->GetSelfPageId(), &rootPageId, &rootLevel)) {
        ReportResult(VerifySeverity::ERROR_LEVEL, segmentMetaPage->GetSelfPageId(), "btree_root_missing", 1, 0,
            "Failed to resolve btree root for index segment (%hu,%u)", segmentMetaPage->GetFileId(),
            segmentMetaPage->GetBlockNum());
        return DSTORE_FAIL;
    }

    uint64 actualLeafCount = 0;
    if (CountLeafPagesBySiblingTraversal(rootPageId, rootLevel, &actualLeafCount) != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }

    const uint64 expectedLeafCount = CountPhysicalLeafPages(segmentMetaPage->GetSelfPageId(), extents);
    if (actualLeafCount != expectedLeafCount) {
        ReportResult(VerifySeverity::WARNING_LEVEL, segmentMetaPage->GetSelfPageId(), "leaf_page_count_mismatch",
            expectedLeafCount, actualLeafCount,
            "Index segment (%hu,%u) physical leaf page count %lu mismatches traversed level-0 leaf count %lu",
            segmentMetaPage->GetFileId(), segmentMetaPage->GetBlockNum(), expectedLeafCount, actualLeafCount);
    }

    return DSTORE_SUCC;
}

uint16 SegmentVerifier::ResolveExpectedExtentSize(
    SegmentType segmentType, uint64 extentIndex, uint16 actualExtentSize) const
{
    if (segmentType == SegmentType::UNDO_SEGMENT_TYPE) {
        return actualExtentSize;
    }

    for (int i = 0; i < EXTENT_SIZE_COUNT - 1; ++i) {
        if (extentIndex < EXT_NUM_LINE[i + 1]) {
            return static_cast<uint16>(EXT_SIZE_LIST[i]);
        }
    }
    return static_cast<uint16>(EXT_SIZE_LIST[EXTENT_SIZE_COUNT - 1]);
}

bool SegmentVerifier::LocateBitmapBit(
    TbsBitmapMetaPage *bitmapMetaPage, const PageId &extentMetaPageId, PageId *bitmapPageId, uint16 *bitNo) const
{
    if (bitmapMetaPage == nullptr || bitmapPageId == nullptr || bitNo == nullptr) {
        return false;
    }

    for (uint16 groupNo = 0; groupNo < bitmapMetaPage->groupCount && groupNo < MAX_BITMAP_GROUP_CNT; ++groupNo) {
        const TbsBitMapGroup &group = bitmapMetaPage->bitmapGroups[groupNo];
        if (extentMetaPageId.m_fileId != group.firstBitmapPageId.m_fileId) {
            continue;
        }

        const uint64 pageStart = group.firstBitmapPageId.m_blockId + BITMAP_PAGES_PER_GROUP;
        const uint64 pageEnd = pageStart + static_cast<uint64>(DF_BITMAP_BIT_CNT) *
            static_cast<uint64>(bitmapMetaPage->extentSize) * static_cast<uint64>(BITMAP_PAGES_PER_GROUP);
        if (extentMetaPageId.m_blockId < pageStart || extentMetaPageId.m_blockId >= pageEnd) {
            continue;
        }

        const uint64 relativeBlock = extentMetaPageId.m_blockId - pageStart;
        if (relativeBlock % bitmapMetaPage->extentSize != 0) {
            return false;
        }

        const uint64 bitIndex = relativeBlock / bitmapMetaPage->extentSize;
        *bitmapPageId = {group.firstBitmapPageId.m_fileId,
            static_cast<BlockNumber>(group.firstBitmapPageId.m_blockId + (bitIndex / DF_BITMAP_BIT_CNT))};
        *bitNo = static_cast<uint16>(bitIndex % DF_BITMAP_BIT_CNT);
        return true;
    }

    return false;
}

uint16 SegmentVerifier::CountSetBits(const TbsBitmapPage *bitmapPage) const
{
    if (bitmapPage == nullptr) {
        return 0;
    }

    uint16 count = 0;
    for (uint16 byteNo = 0; byteNo < DF_BITMAP_BYTE_CNT; ++byteNo) {
        count += static_cast<uint16>(__builtin_popcount(bitmapPage->bitmap[byteNo]));
    }
    return count;
}

bool SegmentVerifier::IsPageInExtents(const PageId &pageId, const std::vector<ExtentInfo> &extents) const
{
    for (const ExtentInfo &extent : extents) {
        if (extent.pageId.m_fileId != pageId.m_fileId) {
            continue;
        }
        if (pageId.m_blockId >= extent.pageId.m_blockId &&
            pageId.m_blockId < extent.pageId.m_blockId + extent.extentSize) {
            return true;
        }
    }
    return false;
}

PageId SegmentVerifier::DescendToLeafLevel(const PageId &rootPageId, uint32 rootLevel)
{
    PageId currentPageId = rootPageId;
    uint32 currentLevel = rootLevel;
    while (currentLevel > 0 && currentPageId.IsValid()) {
        BufferDesc *bufferDesc = INVALID_BUFFER_DESC;
        BtrPage *page = static_cast<BtrPage *>(m_pageSource->ReadPage(currentPageId, &bufferDesc));
        if (page == nullptr || page->GetType() != PageType::INDEX_PAGE_TYPE || page->GetMaxOffset() < BTREE_PAGE_FIRSTKEY) {
            m_pageSource->ReleasePage(bufferDesc);
            return INVALID_PAGE_ID;
        }

        IndexTuple *downlink = page->GetIndexTuple(BTREE_PAGE_FIRSTKEY);
        currentPageId = downlink == nullptr ? INVALID_PAGE_ID : downlink->GetLowlevelIndexpageLink();
        --currentLevel;
        m_pageSource->ReleasePage(bufferDesc);
    }

    return currentPageId;
}

RetStatus SegmentVerifier::CountLeafPagesBySiblingTraversal(const PageId &rootPageId, uint32 rootLevel, uint64 *leafCount)
{
    if (leafCount == nullptr) {
        return DSTORE_FAIL;
    }

    PageId currentPageId = rootLevel == 0 ? rootPageId : DescendToLeafLevel(rootPageId, rootLevel);
    if (!currentPageId.IsValid()) {
        ReportResult(VerifySeverity::ERROR_LEVEL, rootPageId, "leaf_level_resolution_failed", rootLevel, 0,
            "Failed to descend from root (%hu,%u) to btree leaf level", rootPageId.m_fileId, rootPageId.m_blockId);
        return DSTORE_FAIL;
    }

    std::unordered_set<uint64> visitedPages;
    *leafCount = 0;
    while (currentPageId.IsValid()) {
        if (!visitedPages.insert(PageIdToUint64(currentPageId)).second) {
            ReportResult(VerifySeverity::ERROR_LEVEL, currentPageId, "btree_sibling_cycle", 1, 0,
                "Detected sibling cycle while counting index leaf pages at (%hu,%u)", currentPageId.m_fileId,
                currentPageId.m_blockId);
            return DSTORE_FAIL;
        }

        BufferDesc *bufferDesc = INVALID_BUFFER_DESC;
        BtrPage *page = static_cast<BtrPage *>(m_pageSource->ReadPage(currentPageId, &bufferDesc));
        if (page == nullptr || page->GetType() != PageType::INDEX_PAGE_TYPE) {
            ReportResult(VerifySeverity::ERROR_LEVEL, currentPageId, "btree_page_read_failed", 1, 0,
                "Failed to read btree leaf page (%hu,%u)", currentPageId.m_fileId, currentPageId.m_blockId);
            m_pageSource->ReleasePage(bufferDesc);
            return DSTORE_FAIL;
        }

        if (!page->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE) || page->GetLinkAndStatus()->GetLevel() != 0) {
            ReportResult(VerifySeverity::ERROR_LEVEL, currentPageId, "leaf_page_level_invalid", 0,
                page->GetLinkAndStatus()->GetLevel(),
                "Expected leaf page at (%hu,%u) but found type %u level %u", currentPageId.m_fileId,
                currentPageId.m_blockId, page->GetLinkAndStatus()->GetType(), page->GetLinkAndStatus()->GetLevel());
            m_pageSource->ReleasePage(bufferDesc);
            return DSTORE_FAIL;
        }

        ++(*leafCount);
        currentPageId = page->GetRight();
        m_pageSource->ReleasePage(bufferDesc);
    }

    return DSTORE_SUCC;
}

uint64 SegmentVerifier::CountPhysicalLeafPages(const PageId &segmentMetaPageId, const std::vector<ExtentInfo> &extents)
{
    const PageId btrMetaPageId{segmentMetaPageId.m_fileId, segmentMetaPageId.m_blockId + NUM_BTR_META_PAGE};
    const PageId recycleRootMetaPageId{
        segmentMetaPageId.m_fileId, segmentMetaPageId.m_blockId + NUM_BTR_META_PAGE + NUM_RECYCLE_ROOT_META_PAGE};

    uint64 leafCount = 0;
    for (const ExtentInfo &extent : extents) {
        for (uint16 offset = 0; offset < extent.extentSize; ++offset) {
            const PageId currentPageId{extent.pageId.m_fileId, extent.pageId.m_blockId + offset};
            if (currentPageId == segmentMetaPageId || currentPageId == btrMetaPageId || currentPageId == recycleRootMetaPageId) {
                continue;
            }

            BufferDesc *bufferDesc = INVALID_BUFFER_DESC;
            Page *page = m_pageSource->ReadPage(currentPageId, &bufferDesc);
            if (page == nullptr) {
                continue;
            }

            if (page->GetType() == PageType::INDEX_PAGE_TYPE) {
                BtrPage *btrPage = static_cast<BtrPage *>(static_cast<void *>(page));
                if (btrPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE) &&
                    btrPage->GetLinkAndStatus()->GetLevel() == 0 &&
                    !btrPage->GetLinkAndStatus()->IsUnlinked() &&
                    btrPage->GetBtrMetaPageId() == btrMetaPageId) {
                    ++leafCount;
                }
            }

            m_pageSource->ReleasePage(bufferDesc);
        }
    }

    return leafCount;
}

void SegmentVerifier::ReportResult(VerifySeverity severity, const PageId &pageId, const char *checkName,
    uint64 expected, uint64 actual, const char *format, ...)
{
    if (m_context == nullptr || m_context->GetReport() == nullptr) {
        return;
    }

    VerifyResult result;
    result.severity = severity;
    result.targetType = SEGMENT_VERIFY_TARGET;
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

RetStatus VerifySegment(
    BufMgrInterface *bufMgr, const PageId &segmentMetaPageId, const SegmentVerifyOptions &options, VerifyReport *report)
{
    if (bufMgr == nullptr || report == nullptr || !segmentMetaPageId.IsValid()) {
        return DSTORE_FAIL;
    }

    VerifyContext context(report, nullptr, 1.0F, false, options.maxErrors);
    BufferSegmentVerifyPageSource pageSource(bufMgr);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);
    return verifier.Verify();
}

}  // namespace DSTORE

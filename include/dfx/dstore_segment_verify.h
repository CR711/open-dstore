#ifndef DSTORE_SEGMENT_VERIFY_H
#define DSTORE_SEGMENT_VERIFY_H

#include <vector>

#include "dfx/dstore_verify_context.h"
#include "page/dstore_bitmap_meta_page.h"
#include "page/dstore_bitmap_page.h"
#include "page/dstore_extent_meta_page.h"
#include "page/dstore_index_page.h"
#include "page/dstore_segment_meta_page.h"

namespace DSTORE {

class BufferDesc;
class BufMgrInterface;

struct SegmentVerifyOptions {
    bool checkExtentChain{true};
    bool checkExtentBitmap{true};
    bool checkPageCounts{true};
    uint32 maxErrors{1000};
};

class SegmentVerifyPageSource {
public:
    virtual ~SegmentVerifyPageSource() = default;

    virtual SegmentMetaPage *ReadSegmentMetaPage(const PageId &pageId, BufferDesc **bufferDesc) = 0;
    virtual SegExtentMetaPage *ReadExtentMetaPage(const PageId &pageId, BufferDesc **bufferDesc) = 0;
    virtual Page *ReadPage(const PageId &pageId, BufferDesc **bufferDesc) = 0;
    virtual TbsBitmapMetaPage *ReadBitmapMetaPage(FileId fileId, BufferDesc **bufferDesc) = 0;
    virtual TbsBitmapPage *ReadBitmapPage(const PageId &pageId, BufferDesc **bufferDesc) = 0;
    virtual bool GetIndexRootInfo(const PageId &segmentMetaPageId, PageId *rootPageId, uint32 *rootLevel) = 0;
    virtual void ReleasePage(BufferDesc *bufferDesc) = 0;
};

class SegmentVerifier {
public:
    SegmentVerifier(SegmentVerifyPageSource *pageSource, const PageId &segmentMetaPageId,
        const SegmentVerifyOptions &options, VerifyContext *context);

    RetStatus Verify();

private:
    struct ExtentInfo {
        PageId pageId;
        uint16 extentSize;
    };

    RetStatus WalkExtentChain(SegmentMetaPage *segmentMetaPage, std::vector<ExtentInfo> *extents, uint64 *totalBlocks);
    RetStatus VerifySegmentMetadata(SegmentMetaPage *segmentMetaPage, const std::vector<ExtentInfo> &extents,
        uint64 walkedTotalBlocks);
    RetStatus VerifyExtentBitmapConsistency(const std::vector<ExtentInfo> &extents);
    RetStatus VerifyIndexLeafPageCount(SegmentMetaPage *segmentMetaPage, const std::vector<ExtentInfo> &extents);

    uint16 ResolveExpectedExtentSize(SegmentType segmentType, uint64 extentIndex, uint16 actualExtentSize) const;
    bool LocateBitmapBit(
        TbsBitmapMetaPage *bitmapMetaPage, const PageId &extentMetaPageId, PageId *bitmapPageId, uint16 *bitNo) const;
    uint16 CountSetBits(const TbsBitmapPage *bitmapPage) const;
    bool IsPageInExtents(const PageId &pageId, const std::vector<ExtentInfo> &extents) const;
    PageId DescendToLeafLevel(const PageId &rootPageId, uint32 rootLevel);
    RetStatus CountLeafPagesBySiblingTraversal(const PageId &rootPageId, uint32 rootLevel, uint64 *leafCount);
    uint64 CountPhysicalLeafPages(const PageId &segmentMetaPageId, const std::vector<ExtentInfo> &extents);

    void ReportResult(VerifySeverity severity, const PageId &pageId, const char *checkName, uint64 expected,
        uint64 actual, const char *format, ...) __attribute__((format(printf, 7, 8)));

    SegmentVerifyPageSource *m_pageSource{nullptr};
    PageId m_segmentMetaPageId{INVALID_PAGE_ID};
    SegmentVerifyOptions m_options;
    VerifyContext *m_context{nullptr};
};

RetStatus VerifySegment(BufMgrInterface *bufMgr, const PageId &segmentMetaPageId, const SegmentVerifyOptions &options,
    VerifyReport *report);

}  // namespace DSTORE

#endif

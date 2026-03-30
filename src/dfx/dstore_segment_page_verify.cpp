#include "dfx/dstore_page_verify.h"

#include "page/dstore_data_segment_meta_page.h"
#include "page/dstore_heap_segment_meta_page.h"
#include "page/dstore_undo_segment_meta_page.h"

namespace DSTORE {

namespace {

RetStatus ReportSegmentError(VerifyReport *report, const Page *page, const char *checkName, uint64 expected,
    uint64 actual, const char *message)
{
    if (report != nullptr) {
        report->AddResult(VerifySeverity::ERROR_LEVEL, "page", page->GetSelfPageId(), checkName, expected, actual, "%s",
            message);
    }
    return DSTORE_FAIL;
}

bool IsValidSegmentMetaExtentSize(ExtentSize extentSize)
{
    return extentSize == EXT_SIZE_8 || extentSize == EXT_SIZE_128 || extentSize == EXT_SIZE_1024 || extentSize == EXT_SIZE_8192;
}

RetStatus VerifySegmentMetaLightweightCommon(const SegmentMetaPage *page, SegmentType expectedType, VerifyReport *report)
{
    if (page->extentMeta.magic != SEGMENT_META_MAGIC) {
        return ReportSegmentError(report, page, "segment_meta_magic_invalid", SEGMENT_META_MAGIC, page->extentMeta.magic,
            "Segment meta page magic is invalid");
    }

    if (!IsValidSegmentMetaExtentSize(page->extentMeta.extSize)) {
        return ReportSegmentError(report, page, "segment_meta_extent_size_invalid", EXT_SIZE_8192, page->extentMeta.extSize,
            "Segment meta page extent size is invalid");
    }

    if (page->segmentHeader.segmentType != expectedType) {
        return ReportSegmentError(report, page, "segment_meta_type_invalid", static_cast<uint64>(expectedType),
            static_cast<uint64>(page->segmentHeader.segmentType),
            "Segment meta page segment type does not match the page verifier");
    }

    if (page->GetTotalBlockCount() == 0) {
        return ReportSegmentError(report, page, "segment_meta_total_block_invalid", 1, 0,
            "Segment meta page total block count must be positive");
    }

    return DSTORE_SUCC;
}

RetStatus VerifyDataSegmentMetaLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    return VerifySegmentMetaLightweightCommon(static_cast<const DataSegmentMetaPage *>(page),
        static_cast<const DataSegmentMetaPage *>(page)->segmentHeader.segmentType, report);
}

RetStatus VerifyDataSegmentMetaHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    RetStatus ret = VerifyDataSegmentMetaLightweight(page, level, report);
    const DataSegmentMetaPage *metaPage = static_cast<const DataSegmentMetaPage *>(page);

    if (metaPage->GetExtentCount() == 0) {
        ret = ReportSegmentError(report, metaPage, "segment_meta_extent_count_invalid", 1, 0,
            "Data segment meta page must own at least one extent");
    }

    if (metaPage->segmentHeader.extents.first.IsInvalid() || metaPage->segmentHeader.extents.last.IsInvalid()) {
        ret = ReportSegmentError(report, metaPage, "segment_meta_extent_head_invalid", 1, 0,
            "Data segment meta page extent head/tail page ids must be valid");
    }

    if ((metaPage->dataBlockCount == 0) != (metaPage->dataFirst.IsInvalid() || metaPage->dataLast.IsInvalid())) {
        ret = ReportSegmentError(report, metaPage, "segment_meta_data_range_invalid", metaPage->dataBlockCount == 0 ? 0 : 1,
            metaPage->dataFirst.IsInvalid() || metaPage->dataLast.IsInvalid(),
            "Data segment meta page data page range is inconsistent with data block count");
    }

    return ret;
}

RetStatus VerifyHeapSegmentMetaLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    return VerifySegmentMetaLightweightCommon(static_cast<const HeapSegmentMetaPage *>(page),
        static_cast<const HeapSegmentMetaPage *>(page)->segmentHeader.segmentType, report);
}

RetStatus VerifyHeapSegmentMetaHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    RetStatus ret = VerifyHeapSegmentMetaLightweight(page, level, report);
    const HeapSegmentMetaPage *metaPage = static_cast<const HeapSegmentMetaPage *>(page);

    if (metaPage->numFsms > MAX_FSM_TREE_PER_RELATION) {
        ret = ReportSegmentError(report, metaPage, "heap_segment_meta_num_fsms_invalid", MAX_FSM_TREE_PER_RELATION,
            metaPage->numFsms, "Heap segment meta page FSM count exceeds the supported maximum");
    }

    for (uint16 i = 0; i < metaPage->numFsms && i < MAX_FSM_TREE_PER_RELATION; ++i) {
        if (metaPage->fsmInfos[i].fsmMetaPageId.IsInvalid()) {
            ret = ReportSegmentError(report, metaPage, "heap_segment_meta_fsm_page_invalid", 1, 0,
                "Heap segment meta page contains an invalid FSM meta page id");
            break;
        }
    }

    if (metaPage->GetExtentCount() == 0) {
        ret = ReportSegmentError(report, metaPage, "heap_segment_meta_extent_count_invalid", 1, 0,
            "Heap segment meta page must own at least one extent");
    }

    return ret;
}

RetStatus VerifyUndoSegmentMetaLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    return VerifySegmentMetaLightweightCommon(static_cast<const UndoSegmentMetaPage *>(page),
        SegmentType::UNDO_SEGMENT_TYPE, report);
}

RetStatus VerifyUndoSegmentMetaHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    RetStatus ret = VerifyUndoSegmentMetaLightweight(page, level, report);
    const UndoSegmentMetaPage *metaPage = static_cast<const UndoSegmentMetaPage *>(page);

    if (metaPage->GetExtentCount() == 0) {
        ret = ReportSegmentError(report, metaPage, "undo_segment_meta_extent_count_invalid", 1, 0,
            "Undo segment meta page must own at least one extent");
    }

    if (metaPage->alreadyInitTxnSlotPages && metaPage->firstUndoPageId.IsInvalid()) {
        ret = ReportSegmentError(report, metaPage, "undo_segment_meta_first_page_invalid", 1, 0,
            "Undo segment meta page initialized transaction slot pages without a first undo page id");
    }

    return ret;
}

}  // namespace

void RegisterSegmentPageVerifiers()
{
    (void)RegisterPageVerifier(PageType::DATA_SEGMENT_META_PAGE_TYPE, "DataSegmentMetaPage",
        VerifyDataSegmentMetaLightweight, VerifyDataSegmentMetaHeavyweight);
    (void)RegisterPageVerifier(PageType::HEAP_SEGMENT_META_PAGE_TYPE, "HeapSegmentMetaPage",
        VerifyHeapSegmentMetaLightweight, VerifyHeapSegmentMetaHeavyweight);
    (void)RegisterPageVerifier(PageType::UNDO_SEGMENT_META_PAGE_TYPE, "UndoSegmentMetaPage",
        VerifyUndoSegmentMetaLightweight, VerifyUndoSegmentMetaHeavyweight);
}

}  // namespace DSTORE

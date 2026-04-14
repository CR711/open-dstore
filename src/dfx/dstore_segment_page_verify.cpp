#include "dfx/dstore_page_verify.h"

#include "page/dstore_data_segment_meta_page.h"
#include "page/dstore_heap_segment_meta_page.h"
#include "page/dstore_undo_segment_meta_page.h"

namespace DSTORE {

namespace {

RetStatus ReportSegmentError(VerifyReport *report, const Page *page, const char *checkName, uint64 expected,
    uint64 actual, const char *message, VerifyCode code)
{
    if (report != nullptr) {
        const PageId pageId = (page != nullptr) ? page->GetSelfPageId() : INVALID_PAGE_ID;
        report->AddResultWithCode(
            VerifySeverity::SEVERITY_ERROR, code, "page", pageId, checkName, expected, actual, "%s", message);
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
            "Segment meta page magic is invalid", VerifyCode::SEG_MAGIC_MISMATCH);
    }

    if (!IsValidSegmentMetaExtentSize(page->extentMeta.extSize)) {
        return ReportSegmentError(report, page, "segment_meta_extent_size_invalid", EXT_SIZE_8192, page->extentMeta.extSize,
            "Segment meta page extent size is invalid", VerifyCode::SEG_EXT_SIZE_INVALID);
    }

    if (page->segmentHeader.segmentType != expectedType) {
        return ReportSegmentError(report, page, "segment_meta_type_invalid", static_cast<uint64>(expectedType),
            static_cast<uint64>(page->segmentHeader.segmentType),
            "Segment meta page segment type does not match the page verifier", VerifyCode::SEG_SEGMENT_TYPE_INVALID);
    }

    if (page->GetTotalBlockCount() == 0) {
        return ReportSegmentError(report, page, "segment_meta_total_block_invalid", 1, 0,
            "Segment meta page total block count must be positive", VerifyCode::SEG_EXT_SIZE_INVALID);
    }

    return DSTORE_SUCC;
}

RetStatus VerifyDataSegmentMetaLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const DataSegmentMetaPage *metaPage = static_cast<const DataSegmentMetaPage *>(page);
    SegmentType segType = metaPage->segmentHeader.segmentType;

    /* DataSegmentMetaPage 只接受这 4 种 segment type，其他都是腐蚀 */
    if (segType != SegmentType::HEAP_SEGMENT_TYPE &&
        segType != SegmentType::INDEX_SEGMENT_TYPE &&
        segType != SegmentType::HEAP_TEMP_SEGMENT_TYPE &&
        segType != SegmentType::INDEX_TEMP_SEGMENT_TYPE) {
        return ReportSegmentError(report, metaPage, "segment_meta_type_invalid",
            static_cast<uint64>(SegmentType::HEAP_SEGMENT_TYPE),
            static_cast<uint64>(segType),
            "Data segment meta page segment type is not a valid data segment type",
            VerifyCode::SEG_SEGMENT_TYPE_INVALID);
    }

    return VerifySegmentMetaLightweightCommon(metaPage, segType, report);
}

RetStatus VerifyDataSegmentMetaHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    RetStatus ret = DSTORE_SUCC;
    const DataSegmentMetaPage *metaPage = static_cast<const DataSegmentMetaPage *>(page);

    if (metaPage->GetExtentCount() == 0) {
        ret = ReportSegmentError(report, metaPage, "segment_meta_extent_count_invalid", 1, 0,
            "Data segment meta page must own at least one extent", VerifyCode::SEG_EXT_SIZE_INVALID);
    }

    if (metaPage->segmentHeader.extents.first.IsInvalid() || metaPage->segmentHeader.extents.last.IsInvalid()) {
        ret = ReportSegmentError(report, metaPage, "segment_meta_extent_head_invalid", 1, 0,
            "Data segment meta page extent head/tail page ids must be valid", VerifyCode::PAGE_ID_INVALID);
    }

    if ((metaPage->dataBlockCount == 0) != (metaPage->dataFirst.IsInvalid() || metaPage->dataLast.IsInvalid())) {
        ret = ReportSegmentError(report, metaPage, "segment_meta_data_range_invalid", metaPage->dataBlockCount == 0 ? 0 : 1,
            metaPage->dataFirst.IsInvalid() || metaPage->dataLast.IsInvalid(),
            "Data segment meta page data page range is inconsistent with data block count", VerifyCode::PAGE_ID_INVALID);
    }

    return ret;
}

RetStatus VerifyHeapSegmentMetaLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const HeapSegmentMetaPage *metaPage = static_cast<const HeapSegmentMetaPage *>(page);
    SegmentType segType = metaPage->segmentHeader.segmentType;

    /* HeapSegmentMetaPage 只接受 HEAP_SEGMENT_TYPE 或 HEAP_TEMP_SEGMENT_TYPE */
    if (segType != SegmentType::HEAP_SEGMENT_TYPE &&
        segType != SegmentType::HEAP_TEMP_SEGMENT_TYPE) {
        return ReportSegmentError(report, metaPage, "segment_meta_type_invalid",
            static_cast<uint64>(SegmentType::HEAP_SEGMENT_TYPE),
            static_cast<uint64>(segType),
            "Heap segment meta page segment type is not a valid heap segment type",
            VerifyCode::SEG_SEGMENT_TYPE_INVALID);
    }

    return VerifySegmentMetaLightweightCommon(metaPage, segType, report);
}

RetStatus VerifyHeapSegmentMetaHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    RetStatus ret = DSTORE_SUCC;
    const HeapSegmentMetaPage *metaPage = static_cast<const HeapSegmentMetaPage *>(page);

    if (metaPage->numFsms > MAX_FSM_TREE_PER_RELATION) {
        ret = ReportSegmentError(report, metaPage, "heap_segment_meta_num_fsms_invalid", MAX_FSM_TREE_PER_RELATION,
            metaPage->numFsms, "Heap segment meta page FSM count exceeds the supported maximum",
            VerifyCode::HEAP_FSM_SLOT_INVALID);
    }

    for (uint16 i = 0; i < metaPage->numFsms && i < MAX_FSM_TREE_PER_RELATION; ++i) {
        if (metaPage->fsmInfos[i].fsmMetaPageId.IsInvalid()) {
            ret = ReportSegmentError(report, metaPage, "heap_segment_meta_fsm_page_invalid", 1, 0,
                "Heap segment meta page contains an invalid FSM meta page id", VerifyCode::PAGE_ID_INVALID);
            break;
        }
    }

    if (metaPage->GetExtentCount() == 0) {
        ret = ReportSegmentError(report, metaPage, "heap_segment_meta_extent_count_invalid", 1, 0,
            "Heap segment meta page must own at least one extent", VerifyCode::SEG_EXT_SIZE_INVALID);
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
    (void)level;
    RetStatus ret = DSTORE_SUCC;
    const UndoSegmentMetaPage *metaPage = static_cast<const UndoSegmentMetaPage *>(page);

    if (metaPage->GetExtentCount() == 0) {
        ret = ReportSegmentError(report, metaPage, "undo_segment_meta_extent_count_invalid", 1, 0,
            "Undo segment meta page must own at least one extent", VerifyCode::SEG_EXT_SIZE_INVALID);
    }

    if (metaPage->alreadyInitTxnSlotPages && metaPage->firstUndoPageId.IsInvalid()) {
        ret = ReportSegmentError(report, metaPage, "undo_segment_meta_first_page_invalid", 1, 0,
            "Undo segment meta page initialized transaction slot pages without a first undo page id",
            VerifyCode::UNDO_SEG_FIRST_PAGE_INVALID);
    }

    return ret;
}

}  // namespace

void RegisterSegmentPageVerifiers()
{
    (void)RegisterPageVerifier(PageType::DATA_SEGMENT_META_PAGE_TYPE, "DataSegmentMetaPage",
        VerifyModule::SEGMENT, VerifyDataSegmentMetaLightweight, nullptr, VerifyDataSegmentMetaHeavyweight);
    (void)RegisterPageVerifier(PageType::HEAP_SEGMENT_META_PAGE_TYPE, "HeapSegmentMetaPage",
        VerifyModule::HEAP, VerifyHeapSegmentMetaLightweight, nullptr, VerifyHeapSegmentMetaHeavyweight);
    (void)RegisterPageVerifier(PageType::UNDO_SEGMENT_META_PAGE_TYPE, "UndoSegmentMetaPage",
        VerifyModule::UNDO, VerifyUndoSegmentMetaLightweight, nullptr, VerifyUndoSegmentMetaHeavyweight);
}

}  // namespace DSTORE

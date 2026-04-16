#include "dfx/dstore_page_verify.h"

#include "page/dstore_bitmap_meta_page.h"
#include "page/dstore_bitmap_page.h"
#include "page/dstore_extent_meta_page.h"
#include "page/dstore_tbs_file_meta_page.h"
#include "page/dstore_tbs_space_meta_page.h"

namespace DSTORE {

namespace {

RetStatus ReportTbsError(VerifyReport *report, const Page *page, const char *checkName, uint64 expected, uint64 actual,
    const char *message, VerifyCode code)
{
    if (report != nullptr) {
        const PageId pageId = (page != nullptr) ? page->GetSelfPageId() : INVALID_PAGE_ID;
        report->AddResultWithCode(
            VerifySeverity::SEVERITY_ERROR, code, "page", pageId, checkName, expected, actual, "%s", message);
    }
    return DSTORE_FAIL;
}

bool IsValidExtentSize(ExtentSize extentSize)
{
    return extentSize == EXT_SIZE_8 || extentSize == EXT_SIZE_128 || extentSize == EXT_SIZE_1024 || extentSize == EXT_SIZE_8192;
}

uint16 CountSetBits(const TbsBitmapPage *bitmapPage)
{
    uint16 count = 0;
    for (uint32 i = 0; i < DF_BITMAP_BYTE_CNT; ++i) {
        count += static_cast<uint16>(__builtin_popcount(bitmapPage->bitmap[i]));
    }
    return count;
}

RetStatus VerifyTbsExtentMetaLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const SegExtentMetaPage *extentMetaPage = static_cast<const SegExtentMetaPage *>(page);

    if (extentMetaPage->extentMeta.magic != EXTENT_META_MAGIC) {
        return ReportTbsError(report, extentMetaPage, "tbs_extent_meta_magic_invalid", EXTENT_META_MAGIC,
            extentMetaPage->extentMeta.magic, "Tablespace extent meta page magic is invalid", VerifyCode::PAGE_MAGIC_MISMATCH);
    }

    if (!IsValidExtentSize(extentMetaPage->extentMeta.extSize)) {
        return ReportTbsError(report, extentMetaPage, "tbs_extent_meta_size_invalid", EXT_SIZE_8192,
            extentMetaPage->extentMeta.extSize, "Tablespace extent meta page extent size is invalid",
            VerifyCode::SEG_EXT_SIZE_INVALID);
    }

    return DSTORE_SUCC;
}

RetStatus VerifyTbsExtentMetaMediumweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)page;
    (void)level;
    (void)report;
    return DSTORE_SUCC;
}

RetStatus VerifyTbsExtentMetaHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)page;
    (void)level;
    (void)report;
    return DSTORE_SUCC;
}

RetStatus VerifyTbsBitmapPageLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const TbsBitmapPage *bitmapPage = static_cast<const TbsBitmapPage *>(page);

    if (bitmapPage->allocatedExtentCount > DF_BITMAP_BIT_CNT) {
        return ReportTbsError(report, bitmapPage, "tbs_bitmap_allocated_count_invalid", DF_BITMAP_BIT_CNT,
            bitmapPage->allocatedExtentCount, "Tablespace bitmap page allocated extent count exceeds bitmap capacity",
            VerifyCode::BITMAP_ALLOCATED_CNT_MISMATCH);
    }

    return DSTORE_SUCC;
}

RetStatus VerifyTbsBitmapPageMediumweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)page;
    (void)level;
    (void)report;
    return DSTORE_SUCC;
}

RetStatus VerifyTbsBitmapPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    RetStatus ret = DSTORE_SUCC;
    const TbsBitmapPage *bitmapPage = static_cast<const TbsBitmapPage *>(page);
    const uint16 popCount = CountSetBits(bitmapPage);

    if (popCount != bitmapPage->allocatedExtentCount) {
        ret = ReportTbsError(report, bitmapPage, "tbs_bitmap_popcount_invalid", bitmapPage->allocatedExtentCount, popCount,
            "Tablespace bitmap page allocated extent count does not match the bitmap popcount",
            VerifyCode::BITMAP_ALLOCATED_CNT_MISMATCH);
    }

    return ret;
}

RetStatus VerifyTbsBitmapMetaPageLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const TbsBitmapMetaPage *bitmapMetaPage = static_cast<const TbsBitmapMetaPage *>(page);

    if (!IsValidExtentSize(bitmapMetaPage->extentSize)) {
        return ReportTbsError(report, bitmapMetaPage, "tbs_bitmap_meta_extent_size_invalid", EXT_SIZE_8192,
            bitmapMetaPage->extentSize, "Tablespace bitmap meta page extent size is invalid",
            VerifyCode::BITMAP_META_EXTENT_SIZE_INVALID);
    }

    if (bitmapMetaPage->bitmapPagesPerGroup != BITMAP_PAGES_PER_GROUP) {
        return ReportTbsError(report, bitmapMetaPage, "tbs_bitmap_meta_pages_per_group_invalid", BITMAP_PAGES_PER_GROUP,
            bitmapMetaPage->bitmapPagesPerGroup, "Tablespace bitmap meta page bitmapPagesPerGroup is inconsistent",
            VerifyCode::BITMAP_META_EXTENT_SIZE_INVALID);
    }

    return DSTORE_SUCC;
}

RetStatus VerifyTbsBitmapMetaPageMediumweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const TbsBitmapMetaPage *bitmapMetaPage = static_cast<const TbsBitmapMetaPage *>(page);

    if (bitmapMetaPage->groupCount > MAX_BITMAP_GROUP_CNT) {
        return ReportTbsError(report, bitmapMetaPage, "tbs_bitmap_meta_group_count_invalid", MAX_BITMAP_GROUP_CNT,
            bitmapMetaPage->groupCount, "Tablespace bitmap meta page group count exceeds maximum",
            VerifyCode::BITMAP_META_EXTENT_SIZE_INVALID);
    }

    if (bitmapMetaPage->idleGroupHints > bitmapMetaPage->groupCount) {
        return ReportTbsError(report, bitmapMetaPage, "tbs_bitmap_meta_idle_hint_invalid", bitmapMetaPage->groupCount,
            bitmapMetaPage->idleGroupHints, "Tablespace bitmap meta page idle group hint exceeds group count",
            VerifyCode::BITMAP_META_EXTENT_SIZE_INVALID);
    }

    return DSTORE_SUCC;
}

RetStatus VerifyTbsBitmapMetaPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    RetStatus ret = DSTORE_SUCC;
    const TbsBitmapMetaPage *bitmapMetaPage = static_cast<const TbsBitmapMetaPage *>(page);
    const uint16 expectedValidOffset = static_cast<uint16>(OFFSETOF(TbsBitmapMetaPage, bitmapGroups) +
        bitmapMetaPage->groupCount * sizeof(TbsBitMapGroup));

    if (bitmapMetaPage->groupCount > MAX_BITMAP_GROUP_CNT) {
        ret = ReportTbsError(report, bitmapMetaPage, "tbs_bitmap_meta_group_count_invalid", MAX_BITMAP_GROUP_CNT,
            bitmapMetaPage->groupCount, "Tablespace bitmap meta page group count exceeds maximum",
            VerifyCode::BITMAP_META_EXTENT_SIZE_INVALID);
    }

    if (bitmapMetaPage->validOffset != expectedValidOffset) {
        ret = ReportTbsError(report, bitmapMetaPage, "tbs_bitmap_meta_valid_offset_invalid", expectedValidOffset,
            bitmapMetaPage->validOffset, "Tablespace bitmap meta page validOffset does not match group count",
            VerifyCode::BITMAP_META_EXTENT_SIZE_INVALID);
    }

    if (bitmapMetaPage->idleGroupHints > bitmapMetaPage->groupCount) {
        ret = ReportTbsError(report, bitmapMetaPage, "tbs_bitmap_meta_idle_hint_invalid", bitmapMetaPage->groupCount,
            bitmapMetaPage->idleGroupHints, "Tablespace bitmap meta page idle group hint exceeds group count",
            VerifyCode::BITMAP_META_EXTENT_SIZE_INVALID);
    }

    return ret;
}

RetStatus VerifyTbsFileMetaPageLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const TbsFileMetaPage *fileMetaPage = static_cast<const TbsFileMetaPage *>(page);

    if (fileMetaPage->pageBaseGlsn == UINT64_MAX) {
        return ReportTbsError(report, fileMetaPage, "tbs_file_meta_glsn_invalid", UINT64_MAX - 1,
            fileMetaPage->pageBaseGlsn, "Tablespace file meta page base GLSN is invalid", VerifyCode::PAGE_BOUNDARY_INVALID);
    }

    return DSTORE_SUCC;
}

RetStatus VerifyTbsFileMetaPageMediumweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const TbsFileMetaPage *fileMetaPage = static_cast<const TbsFileMetaPage *>(page);

    if (fileMetaPage->oid < FIRST_BOOTSTRAP_OBJECT_ID) {
        return ReportTbsError(report, fileMetaPage, "tbs_file_meta_oid_invalid", FIRST_BOOTSTRAP_OBJECT_ID,
            fileMetaPage->oid, "Tablespace file meta page oid is below the bootstrap object id",
            VerifyCode::FILE_BLOCK_ID_INVALID);
    }

    return DSTORE_SUCC;
}

RetStatus VerifyTbsFileMetaPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    RetStatus ret = DSTORE_SUCC;
    const TbsFileMetaPage *fileMetaPage = static_cast<const TbsFileMetaPage *>(page);

    if (fileMetaPage->oid < FIRST_BOOTSTRAP_OBJECT_ID) {
        ret = ReportTbsError(report, fileMetaPage, "tbs_file_meta_oid_invalid", FIRST_BOOTSTRAP_OBJECT_ID,
            fileMetaPage->oid, "Tablespace file meta page oid is below the bootstrap object id", VerifyCode::FILE_BLOCK_ID_INVALID);
    }

    return ret;
}

RetStatus VerifyTbsSpaceMetaPageLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const TbsSpaceMetaPage *spaceMetaPage = static_cast<const TbsSpaceMetaPage *>(page);

    if (spaceMetaPage->pageVersion > 1) {
        return ReportTbsError(report, spaceMetaPage, "tbs_space_meta_version_invalid", 1, spaceMetaPage->pageVersion,
            "Tablespace space meta page version is invalid", VerifyCode::SPACE_PAGE_VERSION_INVALID);
    }

    return DSTORE_SUCC;
}

RetStatus VerifyTbsSpaceMetaPageMediumweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)page;
    (void)level;
    (void)report;
    return DSTORE_SUCC;
}

RetStatus VerifyTbsSpaceMetaPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)page;
    (void)level;
    (void)report;
    return DSTORE_SUCC;
}

}  // namespace

void RegisterTablespacePageVerifiers()
{
    (void)RegisterPageVerifier(PageType::TBS_EXTENT_META_PAGE_TYPE, "TbsExtentMetaPage",
        VerifyModule::SEGMENT, VerifyTbsExtentMetaLightweight, VerifyTbsExtentMetaMediumweight,
        VerifyTbsExtentMetaHeavyweight);
    (void)RegisterPageVerifier(PageType::TBS_BITMAP_PAGE_TYPE, "TbsBitmapPage",
        VerifyModule::SEGMENT, VerifyTbsBitmapPageLightweight, VerifyTbsBitmapPageMediumweight,
        VerifyTbsBitmapPageHeavyweight);
    (void)RegisterPageVerifier(PageType::TBS_BITMAP_META_PAGE_TYPE, "TbsBitmapMetaPage",
        VerifyModule::SEGMENT, VerifyTbsBitmapMetaPageLightweight, VerifyTbsBitmapMetaPageMediumweight,
        VerifyTbsBitmapMetaPageHeavyweight);
    (void)RegisterPageVerifier(PageType::TBS_FILE_META_PAGE_TYPE, "TbsFileMetaPage",
        VerifyModule::SEGMENT, VerifyTbsFileMetaPageLightweight, VerifyTbsFileMetaPageMediumweight,
        VerifyTbsFileMetaPageHeavyweight);
    (void)RegisterPageVerifier(PageType::TBS_SPACE_META_PAGE_TYPE, "TbsSpaceMetaPage",
        VerifyModule::SEGMENT, VerifyTbsSpaceMetaPageLightweight, VerifyTbsSpaceMetaPageMediumweight,
        VerifyTbsSpaceMetaPageHeavyweight);
}

}  // namespace DSTORE

/*
 * Unit tests for Segment meta page verification (DataSegmentMetaPage,
 * HeapSegmentMetaPage, UndoSegmentMetaPage).
 *
 * Covers both lightweight and heavyweight verifiers registered by
 * RegisterSegmentPageVerifiers().
 */
#include <gtest/gtest.h>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_data_segment_meta_page.h"
#include "page/dstore_heap_segment_meta_page.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;
using DSTORE::ut_dfx::ScopedVerifyConfig;
using DSTORE::ut_dfx::HasVerifyCode;
using DSTORE::ut_dfx::EnableAllModules;

namespace {

/* Initialize a valid DataSegmentMetaPage */
DataSegmentMetaPage *InitDataSegmentMeta(PageBuffer &buffer, PageId pageId, SegmentType segType)
{
    DataSegmentMetaPage *page = reinterpret_cast<DataSegmentMetaPage *>(buffer.data());
    page->InitDataSegmentMetaPage(segType, pageId, EXT_SIZE_8, 1, 1);
    page->dataBlockCount = 1;
    page->dataFirst = pageId;
    page->dataLast = pageId;
    page->addedPageId = pageId;
    page->extendedPageId = {pageId.m_fileId, pageId.m_blockId + 7};
    page->lastExtentIsReused = false;
    page->SetChecksum();
    return page;
}

/* Initialize a valid HeapSegmentMetaPage */
HeapSegmentMetaPage *InitHeapSegmentMeta(PageBuffer &buffer, PageId pageId)
{
    HeapSegmentMetaPage *page = reinterpret_cast<HeapSegmentMetaPage *>(buffer.data());
    page->InitHeapSegmentMetaPage(SegmentType::HEAP_SEGMENT_TYPE, pageId, EXT_SIZE_8, 1, 1);
    page->dataBlockCount = 1;
    page->dataFirst = pageId;
    page->dataLast = pageId;
    page->addedPageId = pageId;
    page->extendedPageId = {pageId.m_fileId, pageId.m_blockId + 7};
    page->lastExtentIsReused = false;
    page->numFsms = 1;
    page->fsmInfos[0] = {pageId, 0};
    page->SetChecksum();
    return page;
}

/* Initialize a valid UndoSegmentMetaPage */
UndoSegmentMetaPage *InitUndoSegmentMeta(PageBuffer &buffer, PageId pageId)
{
    UndoSegmentMetaPage *page = reinterpret_cast<UndoSegmentMetaPage *>(buffer.data());
    page->InitUndoSegmentMetaPage(pageId, 1, 1);
    page->SetChecksum();
    return page;
}

}  /* anonymous namespace */

/* ========== DataSegmentMeta Lightweight Tests ========== */

TEST(UTSegmentPageVerify, ValidDataSegmentMetaPassesLight)
{
    RegisterSegmentPageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    DataSegmentMetaPage *page = InitDataSegmentMeta(buf, {20, 100}, SegmentType::HEAP_SEGMENT_TYPE);

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST(UTSegmentPageVerify, DataSegmentMetaInvalidTypeFails)
{
    RegisterSegmentPageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    /* Init with valid type first, then corrupt */
    DataSegmentMetaPage *page = InitDataSegmentMeta(buf, {20, 101}, SegmentType::HEAP_SEGMENT_TYPE);
    page->segmentHeader.segmentType = SegmentType::UNDO_SEGMENT_TYPE;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::SEG_SEGMENT_TYPE_INVALID));
}

TEST(UTSegmentPageVerify, DataSegmentMetaInvalidMagicFails)
{
    RegisterSegmentPageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    DataSegmentMetaPage *page = InitDataSegmentMeta(buf, {20, 102}, SegmentType::HEAP_SEGMENT_TYPE);
    page->extentMeta.magic = 0;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::SEG_MAGIC_MISMATCH));
}

TEST(UTSegmentPageVerify, DataSegmentMetaInvalidExtSizeFails)
{
    RegisterSegmentPageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    DataSegmentMetaPage *page = InitDataSegmentMeta(buf, {20, 103}, SegmentType::HEAP_SEGMENT_TYPE);
    page->extentMeta.extSize = static_cast<ExtentSize>(EXT_SIZE_8 + 1);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::SEG_EXT_SIZE_INVALID));
}

TEST(UTSegmentPageVerify, DataSegmentMetaZeroTotalBlockFails)
{
    RegisterSegmentPageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    DataSegmentMetaPage *page = InitDataSegmentMeta(buf, {20, 104}, SegmentType::HEAP_SEGMENT_TYPE);
    page->segmentHeader.totalBlockCount = 0;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::SEG_EXT_SIZE_INVALID));
}

TEST(UTSegmentPageVerify, DataSegmentMetaAllValidTypesPasses)
{
    RegisterSegmentPageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();

    SegmentType validTypes[] = {
        SegmentType::HEAP_SEGMENT_TYPE,
        SegmentType::INDEX_SEGMENT_TYPE,
        SegmentType::HEAP_TEMP_SEGMENT_TYPE,
        SegmentType::INDEX_TEMP_SEGMENT_TYPE,
    };

    for (auto segType : validTypes) {
        PageBuffer buf{};
        DataSegmentMetaPage *page = InitDataSegmentMeta(buf, {20, 105}, segType);

        VerifyReport report;
        EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC)
            << "Failed for SegmentType = " << static_cast<int>(segType);
        EXPECT_FALSE(report.HasError());
    }
}

/* ========== DataSegmentMeta Heavyweight Tests ========== */

TEST(UTSegmentPageVerify, ValidDataSegmentMetaPassesHeavy)
{
    RegisterSegmentPageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    DataSegmentMetaPage *page = InitDataSegmentMeta(buf, {20, 200}, SegmentType::HEAP_SEGMENT_TYPE);

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST(UTSegmentPageVerify, DataSegmentMetaZeroExtentCountFails)
{
    RegisterSegmentPageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    DataSegmentMetaPage *page = InitDataSegmentMeta(buf, {20, 201}, SegmentType::HEAP_SEGMENT_TYPE);
    page->segmentHeader.extents.count = 0;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::SEG_EXT_SIZE_INVALID));
}

TEST(UTSegmentPageVerify, DataSegmentMetaInvalidExtentHeadFails)
{
    RegisterSegmentPageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    DataSegmentMetaPage *page = InitDataSegmentMeta(buf, {20, 202}, SegmentType::HEAP_SEGMENT_TYPE);
    page->segmentHeader.extents.first = INVALID_PAGE_ID;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_ID_INVALID));
}

TEST(UTSegmentPageVerify, DataSegmentMetaDataRangeInconsistentFails)
{
    RegisterSegmentPageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    DataSegmentMetaPage *page = InitDataSegmentMeta(buf, {20, 203}, SegmentType::HEAP_SEGMENT_TYPE);
    /* dataBlockCount = 0 but dataFirst is valid (non-INVALID) -> inconsistent */
    page->dataBlockCount = 0;
    /* dataFirst is still valid from InitDataSegmentMeta */
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_ID_INVALID));
}

/* ========== HeapSegmentMeta Lightweight Tests ========== */

TEST(UTSegmentPageVerify, ValidHeapSegmentMetaPassesLight)
{
    RegisterSegmentPageVerifiers();
    PageBuffer buf{};
    HeapSegmentMetaPage *page = InitHeapSegmentMeta(buf, {20, 300});

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST(UTSegmentPageVerify, HeapSegmentMetaInvalidTypeFails)
{
    RegisterSegmentPageVerifiers();
    PageBuffer buf{};
    /* Init with valid HEAP type, then corrupt to INDEX type */
    HeapSegmentMetaPage *page = InitHeapSegmentMeta(buf, {20, 301});
    page->segmentHeader.segmentType = SegmentType::INDEX_SEGMENT_TYPE;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::SEG_SEGMENT_TYPE_INVALID));
}

TEST(UTSegmentPageVerify, HeapSegmentMetaTempTypePassesLight)
{
    RegisterSegmentPageVerifiers();
    PageBuffer buf{};
    /* Init with HEAP type, then change to HEAP_TEMP (still valid) */
    HeapSegmentMetaPage *page = InitHeapSegmentMeta(buf, {20, 302});
    page->segmentHeader.segmentType = SegmentType::HEAP_TEMP_SEGMENT_TYPE;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

/* ========== HeapSegmentMeta Heavyweight Tests ========== */

TEST(UTSegmentPageVerify, ValidHeapSegmentMetaPassesHeavy)
{
    RegisterSegmentPageVerifiers();
    PageBuffer buf{};
    HeapSegmentMetaPage *page = InitHeapSegmentMeta(buf, {20, 400});

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST(UTSegmentPageVerify, HeapSegmentMetaNumFsmsExceedsFails)
{
    RegisterSegmentPageVerifiers();
    PageBuffer buf{};
    HeapSegmentMetaPage *page = InitHeapSegmentMeta(buf, {20, 401});
    page->numFsms = MAX_FSM_TREE_PER_RELATION + 1;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_FSM_SLOT_INVALID));
}

TEST(UTSegmentPageVerify, HeapSegmentMetaInvalidFsmPageIdFails)
{
    RegisterSegmentPageVerifiers();
    PageBuffer buf{};
    HeapSegmentMetaPage *page = InitHeapSegmentMeta(buf, {20, 402});
    page->numFsms = 1;
    page->fsmInfos[0].fsmMetaPageId = INVALID_PAGE_ID;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_ID_INVALID));
}

TEST(UTSegmentPageVerify, HeapSegmentMetaZeroExtentCountFails)
{
    RegisterSegmentPageVerifiers();
    PageBuffer buf{};
    HeapSegmentMetaPage *page = InitHeapSegmentMeta(buf, {20, 403});
    page->segmentHeader.extents.count = 0;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::SEG_EXT_SIZE_INVALID));
}

/* ========== UndoSegmentMeta Lightweight Tests ========== */

TEST(UTSegmentPageVerify, ValidUndoSegmentMetaPassesLight)
{
    RegisterSegmentPageVerifiers();
    PageBuffer buf{};
    UndoSegmentMetaPage *page = InitUndoSegmentMeta(buf, {20, 500});

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST(UTSegmentPageVerify, UndoSegmentMetaInvalidMagicFails)
{
    RegisterSegmentPageVerifiers();
    PageBuffer buf{};
    UndoSegmentMetaPage *page = InitUndoSegmentMeta(buf, {20, 501});
    page->extentMeta.magic = 0;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::SEG_MAGIC_MISMATCH));
}

/* ========== UndoSegmentMeta Heavyweight Tests ========== */

TEST(UTSegmentPageVerify, ValidUndoSegmentMetaPassesHeavy)
{
    RegisterSegmentPageVerifiers();
    PageBuffer buf{};
    UndoSegmentMetaPage *page = InitUndoSegmentMeta(buf, {20, 600});

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST(UTSegmentPageVerify, UndoSegmentMetaZeroExtentCountFails)
{
    RegisterSegmentPageVerifiers();
    PageBuffer buf{};
    UndoSegmentMetaPage *page = InitUndoSegmentMeta(buf, {20, 601});
    page->segmentHeader.extents.count = 0;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::SEG_EXT_SIZE_INVALID));
}

TEST(UTSegmentPageVerify, UndoSegmentMetaInitWithInvalidFirstPageFails)
{
    RegisterSegmentPageVerifiers();
    PageBuffer buf{};
    UndoSegmentMetaPage *page = InitUndoSegmentMeta(buf, {20, 602});
    page->alreadyInitTxnSlotPages = true;
    page->firstUndoPageId = INVALID_PAGE_ID;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_SEG_FIRST_PAGE_INVALID));
}

TEST(UTSegmentPageVerify, DataSegmentMetaExtentLastInvalidFails)
{
    RegisterSegmentPageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    DataSegmentMetaPage *page = InitDataSegmentMeta(buf, {20, 150}, SegmentType::HEAP_SEGMENT_TYPE);

    /* Keep first valid, only set last to INVALID */
    page->segmentHeader.extents.last = INVALID_PAGE_ID;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_ID_INVALID));
}

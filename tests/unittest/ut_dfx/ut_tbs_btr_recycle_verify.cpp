/*
 * Unit tests for Tablespace and BtrRecycle page verification.
 *
 * Coverage:
 *   Tablespace: TbsExtentMeta (magic, extSize), TbsBitmapPage (allocCount overflow, popcount),
 *               TbsBitmapMeta (extSize, pagesPerGroup, groupCount, validOffset, idleHint),
 *               TbsFileMeta (GLSN, oid), TbsSpaceMeta (version)
 *   BtrRecycle: BtrQueuePage (specialOffset, type, size>capacity),
 *               PartitionMeta (xid, timestamp, self-link),
 *               RootMeta (xid, partition self-link)
 */
#include <cstring>
#include <gtest/gtest.h>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_bitmap_meta_page.h"
#include "page/dstore_bitmap_page.h"
#include "page/dstore_extent_meta_page.h"
#include "page/dstore_tbs_file_meta_page.h"
#include "page/dstore_tbs_space_meta_page.h"
#include "page/dstore_btr_recycle_partition_meta_page.h"
#include "page/dstore_btr_recycle_root_meta_page.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;
using DSTORE::ut_dfx::ScopedVerifyConfig;
using DSTORE::ut_dfx::HasVerifyCode;
using DSTORE::ut_dfx::EnableAllModules;

namespace {

/*
 * Minimal layout of BtrQueuePageMeta for test use only.
 * Avoids including dstore_btr_queue_page.h which pulls in heavy
 * framework dependencies (dstore_instance.h, dstore_csn_mgr.h).
 */
struct TestBtrQueuePageMeta {
    uint32 versionNumber;
    Xid createdXid;
    PageId next;
    uint32 type;  /* BtrRecycleQueueType: RECYCLE=0, FREE=1 */
};

}  /* anonymous namespace */

/* ========== TbsBitmapPage — BITMAP_ALLOCATED_CNT_MISMATCH ========== */

TEST(UTTbsBtrRecycleVerify, BitmapAllocatedCountMismatch)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    TbsBitmapPage *page = reinterpret_cast<TbsBitmapPage *>(buf.data());
    page->InitBitmapPage({50, 100}, {50, 101});
    page->SetLsn(1, 1, 1, false);
    page->allocatedExtentCount = 0;
    memset(page->bitmap, 0, DF_BITMAP_BYTE_CNT);
    /* Set 3 bits in bitmap but claim allocatedExtentCount = 0 */
    page->bitmap[0] = 0x07;  /* 3 bits set */
    page->SetChecksum();

    VerifyReport report;
    /* LIGHT should pass since allocatedExtentCount(0) <= DF_BITMAP_BIT_CNT */
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);

    /* HEAVY should detect popcount mismatch */
    VerifyReport heavyReport;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &heavyReport), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(heavyReport, VerifyCode::BITMAP_ALLOCATED_CNT_MISMATCH));
}

/* ========== TbsBitmapMetaPage — BITMAP_META_EXTENT_SIZE_INVALID ========== */

TEST(UTTbsBtrRecycleVerify, BitmapMetaExtentSizeInvalid)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    TbsBitmapMetaPage *page = reinterpret_cast<TbsBitmapMetaPage *>(buf.data());
    page->InitBitmapMetaPage({50, 101}, 0, EXT_SIZE_8);
    page->SetLsn(1, 1, 1, false);
    page->extentSize = static_cast<ExtentSize>(0xFF);  /* Invalid extent size */
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::BITMAP_META_EXTENT_SIZE_INVALID));
}

/* ========== BtrQueuePage — BTR_QUEUE_INCONSISTENT ========== */

TEST(UTTbsBtrRecycleVerify, BtrQueueTypeInvalid)
{
    RegisterBtrRecyclePageVerifiers();
    PageBuffer buf{};
    Page *page = reinterpret_cast<Page *>(buf.data());
    uint16 specialSize = static_cast<uint16>(MAXALIGN(sizeof(TestBtrQueuePageMeta)));
    page->Init(specialSize, PageType::BTR_QUEUE_PAGE_TYPE, {50, 102});
    page->SetLsn(1, 1, 1, false);

    /* Set up the meta area at the special offset with an invalid queue type */
    uint16 specialOffset = page->GetSpecialOffset();
    TestBtrQueuePageMeta *meta = reinterpret_cast<TestBtrQueuePageMeta *>(
        reinterpret_cast<char *>(page) + specialOffset);
    meta->versionNumber = 0;
    meta->createdXid = Xid(0);
    meta->next = INVALID_PAGE_ID;
    meta->type = 0xFF;  /* invalid BtrRecycleQueueType */
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::BTR_QUEUE_INCONSISTENT));
}

/* ========== TbsFileMetaPage — FILE_BLOCK_ID_INVALID ========== */

TEST(UTTbsBtrRecycleVerify, FileMetaOidBelowBootstrap)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    TbsFileMetaPage *page = reinterpret_cast<TbsFileMetaPage *>(buf.data());
    page->InitTbsFileMetaPage({50, 103}, 0, Xid(0));
    page->SetLsn(1, 1, 1, false);
    page->pageBaseGlsn = 1;  /* valid */
    page->oid = 0;  /* below FIRST_BOOTSTRAP_OBJECT_ID */
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::FILE_BLOCK_ID_INVALID));
}

/* ========== TbsSpaceMetaPage — SPACE_PAGE_VERSION_INVALID ========== */

TEST(UTTbsBtrRecycleVerify, SpaceMetaVersionInvalid)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    TbsSpaceMetaPage *page = reinterpret_cast<TbsSpaceMetaPage *>(buf.data());
    page->InitTbsSpaceMetaPage({50, 104});
    page->SetLsn(1, 1, 1, false);
    page->pageVersion = 2;  /* > 1, invalid */
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::SPACE_PAGE_VERSION_INVALID));
}

/* ======================================================================
 * Tablespace — additional coverage
 * ====================================================================== */

/* ========== TbsExtentMetaPage — valid page passes ========== */

TEST(UTTbsBtrRecycleVerify, ExtentMetaValidPasses)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    SegExtentMetaPage *page = reinterpret_cast<SegExtentMetaPage *>(buf.data());
    page->InitSegExtentMetaPage({60, 1}, EXT_SIZE_8, PageType::TBS_EXTENT_META_PAGE_TYPE);
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

/* ========== TbsExtentMetaPage — magic mismatch ========== */

TEST(UTTbsBtrRecycleVerify, ExtentMetaMagicMismatch)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    SegExtentMetaPage *page = reinterpret_cast<SegExtentMetaPage *>(buf.data());
    page->InitSegExtentMetaPage({60, 2}, EXT_SIZE_8, PageType::TBS_EXTENT_META_PAGE_TYPE);
    page->SetLsn(1, 1, 1, false);
    page->extentMeta.magic = 0xDEADBEEF;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_MAGIC_MISMATCH));
}

/* ========== TbsExtentMetaPage — invalid extent size ========== */

TEST(UTTbsBtrRecycleVerify, ExtentMetaInvalidExtentSize)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    SegExtentMetaPage *page = reinterpret_cast<SegExtentMetaPage *>(buf.data());
    page->InitSegExtentMetaPage({60, 3}, EXT_SIZE_8, PageType::TBS_EXTENT_META_PAGE_TYPE);
    page->SetLsn(1, 1, 1, false);
    page->extentMeta.extSize = static_cast<ExtentSize>(999);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::SEG_EXT_SIZE_INVALID));
}

/* ========== TbsBitmapPage — valid page passes ========== */

TEST(UTTbsBtrRecycleVerify, BitmapPageValidPasses)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    TbsBitmapPage *page = reinterpret_cast<TbsBitmapPage *>(buf.data());
    page->InitBitmapPage({60, 10}, {60, 11});
    page->SetLsn(1, 1, 1, false);
    page->allocatedExtentCount = 3;
    memset(page->bitmap, 0, DF_BITMAP_BYTE_CNT);
    page->bitmap[0] = 0x07;  /* 3 bits = matches allocatedExtentCount */
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

/* ========== TbsBitmapPage — allocatedExtentCount exceeds capacity (LIGHT) ========== */

TEST(UTTbsBtrRecycleVerify, BitmapAllocatedCountExceedsCapacity)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    TbsBitmapPage *page = reinterpret_cast<TbsBitmapPage *>(buf.data());
    page->InitBitmapPage({60, 12}, {60, 13});
    page->SetLsn(1, 1, 1, false);
    page->allocatedExtentCount = DF_BITMAP_BIT_CNT + 1;  /* exceeds capacity */
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::BITMAP_ALLOCATED_CNT_MISMATCH));
}

/* ========== TbsBitmapMetaPage — bitmapPagesPerGroup mismatch (LIGHT) ========== */

TEST(UTTbsBtrRecycleVerify, BitmapMetaPagesPerGroupMismatch)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    TbsBitmapMetaPage *page = reinterpret_cast<TbsBitmapMetaPage *>(buf.data());
    page->InitBitmapMetaPage({60, 20}, 0, EXT_SIZE_8);
    page->SetLsn(1, 1, 1, false);
    page->bitmapPagesPerGroup = BITMAP_PAGES_PER_GROUP + 1;  /* mismatch */
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::BITMAP_META_EXTENT_SIZE_INVALID));
}

/* ========== TbsBitmapMetaPage — groupCount exceeds MAX (HEAVY) ========== */

TEST(UTTbsBtrRecycleVerify, BitmapMetaGroupCountExceedsMax)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    TbsBitmapMetaPage *page = reinterpret_cast<TbsBitmapMetaPage *>(buf.data());
    page->InitBitmapMetaPage({60, 21}, 0, EXT_SIZE_8);
    page->SetLsn(1, 1, 1, false);
    page->groupCount = MAX_BITMAP_GROUP_CNT + 1;
    /* Fix validOffset to match groupCount so it doesn't fail first on offset check */
    page->validOffset = static_cast<uint16>(OFFSETOF(TbsBitmapMetaPage, bitmapGroups) +
        page->groupCount * sizeof(TbsBitMapGroup));
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::BITMAP_META_EXTENT_SIZE_INVALID));
}

/* ========== TbsBitmapMetaPage — validOffset mismatch (HEAVY) ========== */

TEST(UTTbsBtrRecycleVerify, BitmapMetaValidOffsetMismatch)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    TbsBitmapMetaPage *page = reinterpret_cast<TbsBitmapMetaPage *>(buf.data());
    page->InitBitmapMetaPage({60, 22}, 0, EXT_SIZE_8);
    page->SetLsn(1, 1, 1, false);
    page->groupCount = 2;
    page->validOffset = 999;  /* does not match expected from groupCount */
    page->idleGroupHints = 0;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::BITMAP_META_EXTENT_SIZE_INVALID));
}

/* ========== TbsBitmapMetaPage — idleGroupHints exceeds groupCount (HEAVY) ========== */

TEST(UTTbsBtrRecycleVerify, BitmapMetaIdleHintExceedsGroupCount)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    TbsBitmapMetaPage *page = reinterpret_cast<TbsBitmapMetaPage *>(buf.data());
    page->InitBitmapMetaPage({60, 23}, 0, EXT_SIZE_8);
    page->SetLsn(1, 1, 1, false);
    page->groupCount = 2;
    page->validOffset = static_cast<uint16>(OFFSETOF(TbsBitmapMetaPage, bitmapGroups) +
        page->groupCount * sizeof(TbsBitMapGroup));
    page->idleGroupHints = 5;  /* > groupCount(2) */
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::BITMAP_META_EXTENT_SIZE_INVALID));
}

/* ========== TbsFileMetaPage — GLSN == UINT64_MAX (LIGHT) ========== */

TEST(UTTbsBtrRecycleVerify, FileMetaGlsnInvalid)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    TbsFileMetaPage *page = reinterpret_cast<TbsFileMetaPage *>(buf.data());
    page->InitTbsFileMetaPage({60, 30}, 0, Xid(0));
    page->SetLsn(1, 1, 1, false);
    page->pageBaseGlsn = UINT64_MAX;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/* ========== TbsFileMetaPage — valid page passes all levels ========== */

TEST(UTTbsBtrRecycleVerify, FileMetaValidPasses)
{
    RegisterTablespacePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    TbsFileMetaPage *page = reinterpret_cast<TbsFileMetaPage *>(buf.data());
    page->InitTbsFileMetaPage({60, 31}, 0, Xid(0));
    page->SetLsn(1, 1, 1, false);
    page->pageBaseGlsn = 100;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

/* ======================================================================
 * BtrRecycle — additional coverage
 * ====================================================================== */

/* ========== BtrQueuePage — special offset invalid (LIGHT) ========== */

TEST(UTTbsBtrRecycleVerify, BtrQueueSpecialOffsetInvalid)
{
    RegisterBtrRecyclePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    Page *page = reinterpret_cast<Page *>(buf.data());
    /* Init with wrong special size — will produce wrong special offset */
    uint16 wrongSpecialSize = static_cast<uint16>(MAXALIGN(sizeof(TestBtrQueuePageMeta)) + 16);
    page->Init(wrongSpecialSize, PageType::BTR_QUEUE_PAGE_TYPE, {60, 40});
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/* ========== BtrRecyclePartitionMeta — createdXid == INVALID_XID (LIGHT) ========== */

TEST(UTTbsBtrRecycleVerify, BtrRecyclePartitionXidInvalid)
{
    RegisterBtrRecyclePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    BtrRecyclePartitionMetaPage *page = reinterpret_cast<BtrRecyclePartitionMetaPage *>(buf.data());
    page->InitRecyclePartitionMetaPage({60, 50}, Xid(1));
    page->SetLsn(1, 1, 1, false);
    page->createdXid = INVALID_XID;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/* ========== BtrRecyclePartitionMeta — accessTimestamp == 0 (HEAVY) ========== */

TEST(UTTbsBtrRecycleVerify, BtrRecyclePartitionTimestampZero)
{
    RegisterBtrRecyclePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    BtrRecyclePartitionMetaPage *page = reinterpret_cast<BtrRecyclePartitionMetaPage *>(buf.data());
    page->InitRecyclePartitionMetaPage({60, 51}, Xid(1));
    page->SetLsn(1, 1, 1, false);
    page->accessTimestamp = 0;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/* ========== BtrRecyclePartitionMeta — queue head self-link (HEAVY) ========== */

TEST(UTTbsBtrRecycleVerify, BtrRecyclePartitionSelfLink)
{
    RegisterBtrRecyclePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    BtrRecyclePartitionMetaPage *page = reinterpret_cast<BtrRecyclePartitionMetaPage *>(buf.data());
    page->InitRecyclePartitionMetaPage({60, 52}, Xid(1));
    page->SetLsn(1, 1, 1, false);
    page->accessTimestamp = 1;
    page->recycleQueueHead = page->GetSelfPageId();  /* self-reference */
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_ID_INVALID));
}

/* ========== BtrRecycleRootMeta — createdXid == INVALID_XID (LIGHT) ========== */

TEST(UTTbsBtrRecycleVerify, BtrRecycleRootXidInvalid)
{
    RegisterBtrRecyclePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    BtrRecycleRootMetaPage *page = reinterpret_cast<BtrRecycleRootMetaPage *>(buf.data());
    page->InitRecycleRootMetaPage({60, 60}, Xid(1));
    page->SetLsn(1, 1, 1, false);
    page->metaPageHeader.createdXid = INVALID_XID;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/* ========== BtrRecycleRootMeta — partition entry self-reference (HEAVY) ========== */

TEST(UTTbsBtrRecycleVerify, BtrRecycleRootPartitionSelfLink)
{
    RegisterBtrRecyclePageVerifiers();
    ScopedVerifyConfig guard;
    EnableAllModules();
    PageBuffer buf{};
    const PageId selfPageId = {60, 61};
    BtrRecycleRootMetaPage *page = reinterpret_cast<BtrRecycleRootMetaPage *>(buf.data());
    page->InitRecycleRootMetaPage(selfPageId, Xid(1));
    page->SetLsn(1, 1, 1, false);
    page->SetRecyclePartitionMeta(0, selfPageId);  /* self-reference */
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_ID_INVALID));
}

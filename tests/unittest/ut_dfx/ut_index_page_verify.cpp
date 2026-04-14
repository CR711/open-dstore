#include <cstring>
#include <gtest/gtest.h>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_index_page.h"
#include "tuple/dstore_index_tuple.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;

namespace {

BtrPage *InitIndexPage(PageBuffer &buffer, PageId pageId)
{
    BtrPage *page = reinterpret_cast<BtrPage *>(buffer.data());
    page->InitBtrPageInner(pageId);
    page->SetLsn(1, 1, 1, false);
    page->GetLinkAndStatus()->InitPageMeta({1, 1}, 0, false);
    page->SetBtrMetaCreateXid(Xid(0));
    page->AllocateTdSpace();
    page->SetChecksum();
    return page;
}

void AddIndexTuple(BtrPage *page, OffsetNumber offset, uint16 tupleSize, uint8 tdId)
{
    page->SetUpper(static_cast<uint16>(page->GetUpper() - tupleSize));
    ItemId *itemId = page->GetItemIdPtr(offset);
    itemId->SetNormal(page->GetUpper(), tupleSize);
    page->SetLower(static_cast<uint16>(page->GetLower() + sizeof(ItemId)));

    IndexTuple *tuple = page->GetIndexTuple(offset);
    tuple->SetSize(tupleSize);
    tuple->SetTdId(tdId);
    tuple->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    ItemPointerData heapCtid = INVALID_ITEM_POINTER;
    tuple->SetHeapCtid(&heapCtid);
}

class UTIndexPageVerify : public ::testing::Test {
protected:
    void SetUp() override
    {
        RegisterIndexPageVerifier();
    }

    PageBuffer pageBuffer{};
    VerifyReport report;

    BtrPage *InitDefaultPage(const PageId &pageId = {30, 40})
    {
        return InitIndexPage(pageBuffer, pageId);
    }
};

}  // namespace

TEST_F(UTIndexPageVerify, ValidIndexPagePasses)
{
    BtrPage *page = InitDefaultPage({30, 40});

    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTIndexPageVerify, InvalidSpecialOffsetFails)
{
    BtrPage *page = InitDefaultPage({31, 41});

    page->SetSpecialOffset(static_cast<uint16>(page->GetSpecialOffset() - 8));
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, MissingHighKeyFails)
{
    BtrPage *page = InitDefaultPage({32, 42});

    page->GetLinkAndStatus()->SetRight({2, 2});
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, ValidIndexPagePassesMedium)
{
    BtrPage *page = InitDefaultPage({33, 43});

    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTIndexPageVerify, MultiTupleHeavyweightValid)
{
    BtrPage *page = InitDefaultPage({34, 44});

    /* high key + 2 data tuples */
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 1, 24, 0);
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 2, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTIndexPageVerify, MetaPageTdCountNonZeroFails)
{
    BtrPage *page = InitDefaultPage({35, 45});

    /* 标记为 META_PAGE 类型 */
    page->GetLinkAndStatus()->SetType(BtrPageType::META_PAGE);
    page->SetChecksum();

    /* meta page 不应有 TD slots，但 InitIndexPage 已分配了 TD space */
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, TdIdExceedsTdCount)
{
    BtrPage *page = InitDefaultPage({36, 46});

    uint8 tdCount = page->GetTdCount();
    /* 插入 tuple 时 tdId 设为 tdCount，越界 */
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, tdCount);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, TupleOverlapDetected)
{
    BtrPage *page = InitDefaultPage({37, 47});

    /* 先插入一个合法 tuple */
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 48, 0);
    /* 手动构造第二个 tuple 使其与第一个重叠 */
    ItemId *itemId2 = page->GetItemIdPtr(BTREE_PAGE_HIKEY + 1);
    uint16 overlapOffset = page->GetUpper() + 24;
    itemId2->SetNormal(overlapOffset, 24);
    page->SetLower(static_cast<uint16>(page->GetLower() + sizeof(ItemId)));

    IndexTuple *tuple2 = reinterpret_cast<IndexTuple *>(
        reinterpret_cast<char *>(page) + overlapOffset);
    tuple2->SetSize(24);
    tuple2->SetTdId(0);
    tuple2->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    ItemPointerData ctid = INVALID_ITEM_POINTER;
    tuple2->SetHeapCtid(&ctid);

    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, DamagedPageFails)
{
    BtrPage *page = InitDefaultPage({38, 48});

    /* IsDamaged() returns true when lower == 0.
     * lower=0 causes lower > upper to NOT trigger (0 < upper is fine),
     * but the index lightweight verifier catches IsDamaged(). */
    page->SetLower(0);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/* ========== 未初始化页面检测测试 ========== */

TEST_F(UTIndexPageVerify, UninitializedPageDetected)
{
    BtrPage *page = InitDefaultPage({39, 49});

    /* Invalidate btrMetaPageId so IsInitialized() returns false */
    page->GetLinkAndStatus()->btrMetaPageId = INVALID_PAGE_ID;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/* ========== MetaPageId 无效测试 ========== */

TEST_F(UTIndexPageVerify, MetaPageIdInvalid)
{
    BtrPage *page = InitDefaultPage({40, 50});

    /* Invalidate btrMetaPageId; this also makes IsInitialized() false,
     * but the verifier additionally checks BTR_META_PAGE_ID_INVALID */
    page->GetLinkAndStatus()->btrMetaPageId = INVALID_PAGE_ID;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::BTR_META_PAGE_ID_INVALID));
}

/* ========== 活跃区域内 Unused ItemId 测试 ========== */

TEST_F(UTIndexPageVerify, UnusedDataItemInActiveRegion)
{
    BtrPage *page = InitDefaultPage({41, 51});

    /* Add hikey + 2 data tuples */
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 1, 24, 0);
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 2, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    /* Set the 3rd data tuple's ItemId to unused */
    ItemId *itemId3 = page->GetItemIdPtr(BTREE_PAGE_HIKEY + 2);
    itemId3->SetUnused();
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/* ========== ItemId 长度与 Tuple 大小不匹配测试 ========== */

TEST_F(UTIndexPageVerify, ItemSizeMismatch)
{
    BtrPage *page = InitDefaultPage({42, 52});

    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    /* Manually reduce ItemId len to be smaller than tuple size */
    ItemId *itemId = page->GetItemIdPtr(BTREE_PAGE_HIKEY);
    itemId->SetNormal(itemId->GetOffset(), 8); /* len=8 but tuple->GetSize()=24 */
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::INDEX_TUPLE_SIZE_MISMATCH));
}

/* ========== MEDIUM 校验器测试 ========== */

TEST_F(UTIndexPageVerify, MediumLevelValid)
{
    BtrPage *page = InitDefaultPage({43, 53});

    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTIndexPageVerify, MediumDetectsLeafLevelNonZero)
{
    BtrPage *page = InitDefaultPage({44, 54});

    /* Default type is LEAF_PAGE from InitIndexPage. Set level > 0 */
    page->GetLinkAndStatus()->SetLevel(3);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, MediumDetectsRightSibSelfReference)
{
    BtrPage *page = InitDefaultPage({45, 55});

    /* Set right sibling to point to self */
    page->GetLinkAndStatus()->SetRight(page->GetSelfPageId());
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::PAGE_ID_INVALID));
}

TEST_F(UTIndexPageVerify, MediumDetectsLeftSibSelfReference)
{
    BtrPage *page = InitDefaultPage({46, 56});

    /* Set left sibling to point to self */
    page->GetLinkAndStatus()->SetLeft(page->GetSelfPageId());
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::PAGE_ID_INVALID));
}

TEST_F(UTIndexPageVerify, MediumDetectsInternalPageLevelZero)
{
    BtrPage *page = InitDefaultPage({47, 57});

    /* Set type to INTERNAL_PAGE but keep level at 0 */
    page->GetLinkAndStatus()->SetType(BtrPageType::INTERNAL_PAGE);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, MediumDetectsItemOutOfBounds)
{
    BtrPage *page = InitDefaultPage({48, 58});

    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    /* Move ItemId offset below upper boundary */
    ItemId *itemId = page->GetItemIdPtr(BTREE_PAGE_HIKEY);
    itemId->SetNormal(0, 24);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/* ========== splitStat / liveStat 枚举范围校验测试 ========== */

TEST_F(UTIndexPageVerify, LightPassesMaxValidLiveStat)
{
    BtrPage *page = InitDefaultPage({49, 59});

    /* Set liveStat to the maximum valid enum value.
     * liveStat is a 2-bit field (max representable = 3 = EMPTY_NO_PARENT_HAS_SIB),
     * so all 2-bit values (0-3) are in the valid enum range.
     * This test verifies that the boundary value passes. */
    BtrPageLinkAndStatus *link = page->GetLinkAndStatus();
    link->SetLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

/* ========== BTR_PAGE_TYPE_INVALID UT-only 断言测试 ========== */

TEST_F(UTIndexPageVerify, LightDetectsInvalidBtrPageType)
{
    BtrPage *page = InitDefaultPage({50, 60});

    /* Set btree page type to INVALID_BTR_PAGE (0) which is out of valid range */
    page->GetLinkAndStatus()->SetType(BtrPageType::INVALID_BTR_PAGE);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::BTR_PAGE_TYPE_INVALID))
        << "Expected BTR_PAGE_TYPE_INVALID error code for INVALID_BTR_PAGE type";
}

TEST_F(UTIndexPageVerify, LightDetectsOutOfRangeBtrPageType)
{
    BtrPage *page = InitDefaultPage({51, 61});

    /* type is a 2-bit field: META_PAGE(3)+1 = 4 wraps to 0 = INVALID_BTR_PAGE.
     * This test verifies that corrupted bit patterns that wrap around are
     * still caught by IsValidBtrPageType(). */
    BtrPageLinkAndStatus *link = page->GetLinkAndStatus();
    link->status.bitVal.type = static_cast<uint16>(BtrPageType::META_PAGE) + 1;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::BTR_PAGE_TYPE_INVALID))
        << "Expected BTR_PAGE_TYPE_INVALID error code for out-of-range type";
}

/* ========== BTR_SPLIT_STAT_INVALID 校验测试 ========== */

TEST_F(UTIndexPageVerify, LightDetectsInvalidSplitStat)
{
    BtrPage *page = InitDefaultPage({52, 62});

    /* splitStat is a 2-bit field; valid values are SPLIT_COMPLETE(0) and
     * SPLIT_INCOMPLETE(1). Set splitStat=2 which is out of range.
     * IsSplitComplete() returns false when splitStat != 0, so the guard
     * condition !IsSplitComplete() is satisfied. */
    BtrPageLinkAndStatus *link = page->GetLinkAndStatus();
    link->status.bitVal.splitStat = 2;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::BTR_SPLIT_STAT_INVALID))
        << "Expected BTR_SPLIT_STAT_INVALID for out-of-range splitStat=2";
}

TEST_F(UTIndexPageVerify, LightPassesValidSplitIncomplete)
{
    BtrPage *page = InitDefaultPage({53, 63});

    /* SPLIT_INCOMPLETE(1) is the maximum valid splitStat value.
     * The page should pass lightweight verification. */
    BtrPageLinkAndStatus *link = page->GetLinkAndStatus();
    link->SetSplitStatus(BtrPageSplitStatus::SPLIT_INCOMPLETE);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

/* ========== Intra-page key ordering tests (HEAVY) ========== */

TEST_F(UTIndexPageVerify, HeavyweightKeyOrderValid)
{
    BtrPage *page = InitDefaultPage({54, 64});

    uint16 tupleSize = 32; /* INDEX_TUPLE_SIZE(16) + 16 bytes key data */
    /* high key */
    AddIndexTuple(page, BTREE_PAGE_HIKEY, tupleSize, 0);
    /* data tuple 1: key bytes = 0x10 */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 1, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 1);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x10, keyLen);
    }
    /* data tuple 2: key bytes = 0x20 (ascending order) */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 2, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 2);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x20, keyLen);
    }

    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTIndexPageVerify, HeavyweightKeyOrderInvalid)
{
    BtrPage *page = InitDefaultPage({55, 65});

    uint16 tupleSize = 32; /* INDEX_TUPLE_SIZE(16) + 16 bytes key data */
    /* high key */
    AddIndexTuple(page, BTREE_PAGE_HIKEY, tupleSize, 0);
    /* data tuple 1: key bytes = 0x30 (higher value) */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 1, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 1);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x30, keyLen);
    }
    /* data tuple 2: key bytes = 0x10 (lower value — out of order) */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 2, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 2);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x10, keyLen);
    }

    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/* ========== 5+ tuple key ordering tests (HEAVY level) ========== */

TEST_F(UTIndexPageVerify, FiveTupleKeyOrderValid)
{
    BtrPage *page = InitDefaultPage({56, 66});

    uint16 tupleSize = 32; /* INDEX_TUPLE_SIZE(16) + 16 bytes key data */
    /* high key */
    AddIndexTuple(page, BTREE_PAGE_HIKEY, tupleSize, 0);
    /* data tuple 1: key = 0x10 */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 1, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 1);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x10, keyLen);
    }
    /* data tuple 2: key = 0x20 */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 2, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 2);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x20, keyLen);
    }
    /* data tuple 3: key = 0x30 */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 3, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 3);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x30, keyLen);
    }
    /* data tuple 4: key = 0x40 */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 4, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 4);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x40, keyLen);
    }
    /* data tuple 5: key = 0x50 */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 5, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 5);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x50, keyLen);
    }

    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTIndexPageVerify, FiveTupleKeyOrderInvalid_MiddleSwap)
{
    BtrPage *page = InitDefaultPage({57, 67});

    uint16 tupleSize = 32; /* INDEX_TUPLE_SIZE(16) + 16 bytes key data */
    /* high key */
    AddIndexTuple(page, BTREE_PAGE_HIKEY, tupleSize, 0);
    /* data tuple 1: key = 0x10 */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 1, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 1);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x10, keyLen);
    }
    /* data tuple 2: key = 0x20 */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 2, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 2);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x20, keyLen);
    }
    /* data tuple 3: key = 0x40 (swapped with tuple 4) */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 3, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 3);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x40, keyLen);
    }
    /* data tuple 4: key = 0x30 (swapped with tuple 3 — out of order) */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 4, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 4);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x30, keyLen);
    }
    /* data tuple 5: key = 0x50 */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 5, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 5);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x50, keyLen);
    }

    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, DuplicateKeysAllowed)
{
    BtrPage *page = InitDefaultPage({58, 68});

    uint16 tupleSize = 32; /* INDEX_TUPLE_SIZE(16) + 16 bytes key data */
    /* high key */
    AddIndexTuple(page, BTREE_PAGE_HIKEY, tupleSize, 0);
    /* data tuple 1: key = 0x20 */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 1, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 1);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x20, keyLen);
    }
    /* data tuple 2: key = 0x20 (duplicate) */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 2, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 2);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x20, keyLen);
    }
    /* data tuple 3: key = 0x20 (duplicate) */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 3, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 3);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x20, keyLen);
    }

    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTIndexPageVerify, SingleDataTupleKeyOrderValid)
{
    BtrPage *page = InitDefaultPage({59, 69});

    uint16 tupleSize = 32; /* INDEX_TUPLE_SIZE(16) + 16 bytes key data */
    /* high key */
    AddIndexTuple(page, BTREE_PAGE_HIKEY, tupleSize, 0);
    /* single data tuple: key = 0x42 */
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 1, tupleSize, 0);
    {
        const IndexTuple *tuple = page->GetIndexTuple(BTREE_PAGE_HIKEY + 1);
        char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
        uint16 keyLen = tupleSize - INDEX_TUPLE_SIZE;
        memset(keyData, 0x42, keyLen);
    }

    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

/* ========== Alignment violation test (MEDIUM level) ========== */

TEST_F(UTIndexPageVerify, ItemIdOffsetMisaligned)
{
    BtrPage *page = InitDefaultPage({60, 70});

    /* Add two tuples so the first tuple's offset is well above GetUpper() */
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    AddIndexTuple(page, BTREE_PAGE_FIRSTKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);

    /* Corrupt the FIRST tuple's ItemId offset to an odd (non-MAXALIGN'd) value.
     * Since this tuple was added first, its offset is higher in the page
     * (further from GetUpper()), so subtracting 1 keeps it within bounds. */
    ItemId *itemId = page->GetItemIdPtr(BTREE_PAGE_HIKEY);
    uint16 originalOffset = itemId->GetOffset();
    uint16 misaligned = originalOffset - 1;  /* guaranteed misaligned: 8-byte aligned - 1 = odd */
    itemId->SetNormal(misaligned, 24);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::INDEX_ITEMID_OFFSET_MISALIGNED));
}

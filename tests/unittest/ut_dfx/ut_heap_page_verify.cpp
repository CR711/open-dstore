#include <gtest/gtest.h>

#include <chrono>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_heap_page.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;
using DSTORE::ut_dfx::ScopedVerifyConfig;

namespace {

HeapPage *InitHeapPage(PageBuffer &buffer, PageId pageId)
{
    HeapPage *page = reinterpret_cast<HeapPage *>(buffer.data());
    page->Init(0, PageType::HEAP_PAGE_TYPE, pageId);
    page->SetLsn(1, 1, 1, false);
    page->SetDataHeaderSize(HEAP_PAGE_DATA_OFFSET);
    page->m_header.m_lower = HEAP_PAGE_DATA_OFFSET;  /* 设置正确的 lower 值 */
    page->AllocateTdSpace();
    page->SetFsmIndex({INVALID_PAGE_ID, 0});
    page->SetPotentialDelSize(0);
    page->SetChecksum();
    return page;
}

void AddHeapTuple(HeapPage *page, OffsetNumber offset, uint16 tupleSize, uint8 tdId)
{
    page->SetUpper(static_cast<uint16>(page->GetUpper() - tupleSize));
    ItemId *itemId = page->GetItemIdPtr(offset);
    itemId->SetNormal(page->GetUpper(), tupleSize);
    page->SetLower(static_cast<uint16>(page->GetLower() + sizeof(ItemId)));

    HeapDiskTuple *tuple = page->GetDiskTuple(offset);
    tuple->SetTupleSize(tupleSize);
    tuple->SetTdId(tdId);
    tuple->SetLockerTdId(INVALID_TD_SLOT);
    tuple->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    tuple->SetLiveMode(HeapDiskTupLiveMode::TUPLE_BY_NORMAL_INSERT);
    tuple->SetNumColumn(1);
}

/* ========== Test Fixture ========== */

class UTHeapPageVerify : public ::testing::Test {
protected:
    void SetUp() override
    {
        RegisterHeapPageVerifier();
    }

    HeapPage *InitDefaultPage(const PageId &pageId = {10, 20})
    {
        return InitHeapPage(pageBuffer, pageId);
    }

    PageBuffer pageBuffer{};
    VerifyReport report;
};

}  // namespace

/* ========== 正常页面校验测试 ========== */

TEST_F(UTHeapPageVerify, ValidHeapPagePassesLight)
{
    HeapPage *page = InitDefaultPage({20, 30});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTHeapPageVerify, ValidHeapPagePassesMedium)
{
    HeapPage *page = InitDefaultPage({21, 31});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTHeapPageVerify, ValidHeapPagePassesHeavy)
{
    HeapPage *page = InitDefaultPage({22, 32});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

/* ========== TD Count 错误测试 ========== */

TEST_F(UTHeapPageVerify, InvalidTdCountFails)
{
    HeapPage *page = InitDefaultPage({23, 33});
    page->dataHeader.tdCount = static_cast<uint8>(MIN_TD_COUNT - 1);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/* ========== FSM Slot 错误测试 ========== */

TEST_F(UTHeapPageVerify, InvalidFsmSlotFailsLight)
{
    HeapPage *page = InitDefaultPage({24, 34});
    page->SetFsmIndex({INVALID_PAGE_ID, FSM_MAX_HWM});
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/* ========== CRC 错误测试 ========== */

TEST_F(UTHeapPageVerify, CrcMismatchFails)
{
    HeapPage *page = InitDefaultPage({25, 35});
    /* 篡改页面内容但不更新 CRC */
    page->dataHeader.tdCount = 5;

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    /* 应该报告 CRC 错误 */
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::PAGE_CRC_MISMATCH));
}

/* ========== 边界错误测试 ========== */

TEST_F(UTHeapPageVerify, LowerExceedsUpperFails)
{
    HeapPage *page = InitDefaultPage({26, 36});
    /* 设置 lower > upper */
    page->SetLower(8000);
    page->SetUpper(1000);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetResults()[0].code, VerifyCode::PAGE_BOUNDARY_INVALID);
}

/* ========== 页面类型错误测试 ========== */

TEST_F(UTHeapPageVerify, UnregisteredPageTypeFails)
{
    Page *page = reinterpret_cast<Page *>(pageBuffer.data());
    page->Init(0, PageType::HEAP_PAGE_TYPE, {27, 37});
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();

    /* 不注册校验器，直接验证 */
    PageVerifyRegistry registry;
    EXPECT_EQ(registry.Verify(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/* ========== 校验级别测试 ========== */

TEST_F(UTHeapPageVerify, NoneLevelSkipsAllChecks)
{
    HeapPage *page = InitDefaultPage({28, 38});
    /* 即使有错误，NONE 级别也应该跳过 */
    page->dataHeader.tdCount = static_cast<uint8>(MIN_TD_COUNT - 1);

    EXPECT_EQ(VerifyPage(page, VerifyLevel::NONE, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 0U);
}

/* ========== 模块过滤测试 ========== */

TEST_F(UTHeapPageVerify, ModuleFilterWorks)
{
    HeapPage *page = InitDefaultPage({29, 39});
    /* 破坏页面以产生错误 */
    page->SetLower(8000);
    page->SetUpper(1000);
    page->SetChecksum();

    /* RAII guard restores GUC on scope exit (safe against assertion failures) */
    ScopedVerifyConfig guard;

    /* 只启用 INDEX 模块，HEAP 页面应跳过 */
    SetDfxVerifyLevel(VerifyLevel::LIGHT);
    SetDfxVerifyModules(1ULL << static_cast<int>(VerifyModule::INDEX));

    VerifyReport skippedReport;
    EXPECT_EQ(VerifyPageInlineWithReport(page, &skippedReport), DSTORE_SUCC);
    EXPECT_EQ(skippedReport.GetTotalChecks(), 0U);

    /* 启用 HEAP 模块 */
    SetDfxVerifyModules(1ULL << static_cast<int>(VerifyModule::HEAP));
    VerifyReport enabledReport;
    EXPECT_EQ(VerifyPageInlineWithReport(page, &enabledReport), DSTORE_FAIL);
    EXPECT_GT(enabledReport.GetTotalChecks(), 0U);
}

/* ========== 错误码验证测试 ========== */

TEST_F(UTHeapPageVerify, CorrectErrorCodeReported)
{
    HeapPage *page = InitDefaultPage({30, 40});
    page->dataHeader.tdCount = static_cast<uint8>(MIN_TD_COUNT - 1);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);

    /* 验证错误码在 Heap 模块范围内 (0x0100-0x012F) */
    bool hasHeapError = false;
    for (const auto &result : report.GetResults()) {
        uint32 code = static_cast<uint32>(result.code);
        if (code >= 0x0100 && code < 0x0130) {
            hasHeapError = true;
            break;
        }
    }
    EXPECT_TRUE(hasHeapError);
}

TEST_F(UTHeapPageVerify, CrExtendPageSkipped)
{
    HeapPage *page = InitDefaultPage({31, 41});

    page->SetIsCrExtend(true);
    page->dataHeader.tdCount = 0; /* 破坏页面使模块级校验会失败 */
    page->SetChecksum();           /* CRC 必须正确（CR 跳过在 CRC 校验之后） */

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 0U);
}

/* ========== 三场景入口测试 ========== */

TEST_F(UTHeapPageVerify, VerifyPageOnReadBehavior)
{
    HeapPage *page = InitDefaultPage({32, 42});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    /* 正常页面：VerifyPageOnRead 返回成功 */
    VerifyReport readReport;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &readReport), DSTORE_SUCC);
    EXPECT_FALSE(readReport.HasError());

    /* 错误页面：VerifyPageOnRead 返回失败，不 PANIC */
    PageBuffer errorBuffer{};
    HeapPage *errorPage = InitHeapPage(errorBuffer, {33, 43});
    errorPage->SetLower(8000);
    errorPage->SetUpper(1000);
    errorPage->SetChecksum();

    VerifyReport errorReport;
    EXPECT_EQ(VerifyPageOnRead(errorPage, VerifyLevel::LIGHT, &errorReport), DSTORE_FAIL);
    EXPECT_TRUE(errorReport.HasError());
}

TEST_F(UTHeapPageVerify, VerifyPageFullBehavior)
{
    HeapPage *page = InitDefaultPage({34, 44});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    /* 正常页面：VerifyPageFull 返回成功 */
    VerifyReport fullReport;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &fullReport), DSTORE_SUCC);

    /* 错误页面：VerifyPageFull 返回成功（收集问题不 PANIC） */
    PageBuffer errorBuffer{};
    HeapPage *errorPage = InitHeapPage(errorBuffer, {35, 45});
    errorPage->SetLower(8000);
    errorPage->SetUpper(1000);
    errorPage->SetChecksum();

    VerifyReport errorReport;
    /* VerifyPageFull 始终返回成功，只收集问题 */
    EXPECT_EQ(VerifyPageFull(errorPage, VerifyLevel::LIGHT, &errorReport), DSTORE_SUCC);
    /* 但报告中应有错误 */
    EXPECT_TRUE(errorReport.HasError());
}

TEST_F(UTHeapPageVerify, VerifyPageOnWriteNormalPage)
{
    HeapPage *page = InitDefaultPage({36, 46});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    /* 正常页面：VerifyPageOnWrite 返回成功 */
    EXPECT_EQ(VerifyPageOnWrite(page, VerifyLevel::LIGHT), DSTORE_SUCC);
}

/* ========== Heavyweight 多 Tuple 复杂场景测试 ========== */

TEST_F(UTHeapPageVerify, MultiTupleHeavyweightValid)
{
    HeapPage *page = InitDefaultPage({50, 60});
    /* 依次插入 3 个 tuple，从高地址向低地址排列，无重叠 */
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 40, 0);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER + 1, 40, 0);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER + 2, 40, 0);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTHeapPageVerify, TupleOverlapDetected)
{
    HeapPage *page = InitDefaultPage({51, 61});
    /* 先插入一个合法 tuple */
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 64, 0);
    /* 第二个 tuple：手动构造 offset 使其与第一个重叠 */
    ItemId *itemId2 = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER + 1);
    uint16 overlapOffset = page->GetUpper() + 32; /* 与第一个 tuple 重叠 32 字节 */
    itemId2->SetNormal(overlapOffset, 32);
    page->SetLower(static_cast<uint16>(page->GetLower() + sizeof(ItemId)));

    HeapDiskTuple *tuple2 = reinterpret_cast<HeapDiskTuple *>(
        reinterpret_cast<char *>(page) + overlapOffset);
    tuple2->SetTupleSize(32);
    tuple2->SetTdId(0);
    tuple2->SetLockerTdId(INVALID_TD_SLOT);
    tuple2->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    tuple2->SetNumColumn(1);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_TUPLE_OVERLAP));
}

TEST_F(UTHeapPageVerify, TdStatusInvalid)
{
    HeapPage *page = InitDefaultPage({52, 62});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    /* 设置非法 TD status */
    HeapDiskTuple *tuple = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tuple->SetTdStatus(static_cast<TupleTdStatus>(0xFF));
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_TD_SANITY_FAIL));
}

TEST_F(UTHeapPageVerify, TdIdExceedsTdCount)
{
    HeapPage *page = InitDefaultPage({53, 63});
    /* TD count 由 AllocateTdSpace 设定，通常为 MIN_TD_COUNT */
    uint8 tdCount = page->GetTdCount();
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, tdCount); /* tdId == tdCount，越界 */
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_TD_SANITY_FAIL));
}

TEST_F(UTHeapPageVerify, UnusedItemIdWithNonZeroLen)
{
    HeapPage *page = InitDefaultPage({54, 64});
    /* 构造一个 unused 但 len != 0 的 ItemId */
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    ItemId *itemId = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER);
    itemId->SetUnused();
    /* SetUnused() 会将 len 清零，手动写回非零 len 以模拟腐蚀 */
    itemId->direct.m_len = 32;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTHeapPageVerify, ZeroColumnTupleDetected)
{
    HeapPage *page = InitDefaultPage({55, 65});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    /* 将 numColumn 设为 0 */
    HeapDiskTuple *tuple = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tuple->SetNumColumn(0);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_TUPLE_NUM_COLUMN_INVALID));
}

/* ========== NoStorage ItemId TD 校验测试 ========== */

TEST_F(UTHeapPageVerify, NoStorageItem_TdIdExceeds)
{
    HeapPage *page = InitDefaultPage({60, 70});

    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    ItemId *itemId = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER);
    uint8 tdCount = page->GetTdCount();
    itemId->SetNoStorage();
    itemId->SetTdId(tdCount); /* out of bounds */
    itemId->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_TD_SANITY_FAIL));
}

TEST_F(UTHeapPageVerify, NoStorageItem_TdStatusInvalid)
{
    HeapPage *page = InitDefaultPage({61, 71});

    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    ItemId *itemId = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER);
    itemId->SetNoStorage();
    itemId->SetTdId(0);
    itemId->SetTdStatus(static_cast<TupleTdStatus>(0xFF));
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_TD_SANITY_FAIL));
}

/* ========== RangePlaceholder 零长度测试 ========== */

TEST_F(UTHeapPageVerify, RangePlaceholder_ZeroLen)
{
    HeapPage *page = InitDefaultPage({62, 72});

    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    ItemId *itemId = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER);
    itemId->MarkUnreadableAndRangeholder();
    itemId->direct.m_len = 0;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_TUPLE_SIZE_MISMATCH));
}

/* ========== LockerTdId 越界测试 ========== */

TEST_F(UTHeapPageVerify, LockerTdId_ExceedsTdCount)
{
    HeapPage *page = InitDefaultPage({63, 73});

    uint8 tdCount = page->GetTdCount();
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    HeapDiskTuple *tuple = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tuple->SetLockerTdId(tdCount); /* exceeds tdCount */
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_TD_SANITY_FAIL));
}

/* ========== PotentialDelSize 越界测试 ========== */

TEST_F(UTHeapPageVerify, PotentialDelSize_ExceedsBlcksz)
{
    HeapPage *page = InitDefaultPage({64, 74});

    page->SetPotentialDelSize(BLCKSZ + 1);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/* ========== FSM Page Invalid Slot 测试 ========== */

TEST_F(UTHeapPageVerify, FsmPageInvalid_SlotNonZero)
{
    HeapPage *page = InitDefaultPage({65, 75});

    page->SetFsmIndex({INVALID_PAGE_ID, 5});
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_FSM_SLOT_INVALID));
}

/* ========== 跨级别检测一致性测试 ========== */

TEST_F(UTHeapPageVerify, CrossLevel_SameDamage_AllLevelsDetect)
{
    /* Use TD count overflow - detectable at all levels */
    VerifyLevel levels[] = {VerifyLevel::LIGHT, VerifyLevel::MEDIUM, VerifyLevel::HEAVY};
    for (VerifyLevel lvl : levels) {
        PageBuffer localBuffer{};
        HeapPage *page = InitHeapPage(localBuffer, {66, 76});
        page->dataHeader.tdCount = static_cast<uint8>(MIN_TD_COUNT - 1);
        page->SetChecksum();

        VerifyReport localReport;
        EXPECT_EQ(VerifyPage(page, lvl, &localReport), DSTORE_FAIL)
            << "Should fail at level " << static_cast<int>(lvl);
        EXPECT_TRUE(localReport.HasError());
    }
}

/* ========== MEDIUM 校验器测试 ========== */

TEST_F(UTHeapPageVerify, ValidHeapPagePassesMediumWithTuples)
{
    HeapPage *page = InitDefaultPage({70, 80});

    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER + 1, 32, 0);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTHeapPageVerify, MediumDetectsItemOutOfBounds)
{
    HeapPage *page = InitDefaultPage({71, 81});

    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    /* Move ItemId offset to point outside the tuple storage area */
    ItemId *itemId = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER);
    itemId->SetNormal(0, 32);  /* offset=0 is below upper */
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTHeapPageVerify, MediumDetectsTupleTooSmall)
{
    HeapPage *page = InitDefaultPage({72, 82});

    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    /* Shrink ItemId len to below HEAP_DISK_TUP_HEADER_SIZE */
    ItemId *itemId = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER);
    itemId->SetNormal(itemId->GetOffset(), 4);  /* 4 bytes < header size */
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_TUPLE_HEADER_SIZE_INVALID));
}

/* ========== LIGHT dataHeaderSize 校验测试 ========== */

TEST_F(UTHeapPageVerify, LightDetectsDataHeaderSizeInvalid)
{
    HeapPage *page = InitDefaultPage({73, 83});

    /* 篡改 dataHeaderSize 为非法值 */
    page->SetDataHeaderSize(HEAP_PAGE_DATA_OFFSET + 8);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_HEADER_OFFSET_INVALID));
}

/* ========== MEDIUM dataHeaderSize 边界校验测试 ========== */

TEST_F(UTHeapPageVerify, MediumDetectsDataHeaderSizeTooSmall)
{
    HeapPage *page = InitDefaultPage({74, 84});

    /* 设置 dataHeaderSize 小于最小值 */
    page->SetDataHeaderSize(HEAP_PAGE_DATA_OFFSET - 1);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTHeapPageVerify, MediumDetectsDataHeaderSizeExceedsLower)
{
    HeapPage *page = InitDefaultPage({75, 85});

    /* 设置 dataHeaderSize 大于 lower */
    uint16 originalLower = page->GetLower();
    page->SetDataHeaderSize(originalLower + 1);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/* ========== LIGHT specialOffset 校验测试 ========== */

TEST_F(UTHeapPageVerify, LightDetectsSpecialOffsetMismatch)
{
    HeapPage *page = InitDefaultPage({76, 86});

    /* 先添加 tuple 使 upper 降低，再设 specialOffset 到 upper 和 BLCKSZ 之间，
     * 使其通过通用 LIGHT 边界校验（upper <= special <= BLCKSZ）但被
     * Heap 特有校验捕获（specialOffset != BLCKSZ） */
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    uint16 upperVal = page->GetUpper();
    page->m_header.m_special.m_offset = static_cast<uint16>((upperVal + BLCKSZ) / 2);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_SPECIAL_OFFSET_MISMATCH));
}

/* ========== MEDIUM ItemId 对齐校验测试 ========== */

TEST_F(UTHeapPageVerify, MediumDetectsItemIdAlignmentInvalid)
{
    HeapPage *page = InitDefaultPage({77, 87});

    /* 篡改 lower 使 ItemId 区域不对齐到 sizeof(ItemId) */
    uint16 goodLower = page->GetLower();
    page->SetLower(goodLower + 1);  /* +1 byte 打破对齐 */
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_ITEMID_ALIGNMENT_INVALID));
}

/* ========== HEAVY flag 一致性校验测试 ========== */

TEST_F(UTHeapPageVerify, HeavyDetectsFlagInconsistent)
{
    HeapPage *page = InitDefaultPage({78, 88});

    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    /* 设置 hasExternal=true 但 hasVarwidth=false：
     * ResetAttrInfo() 清除 hasNull 和 hasVarwidth，再单独设 hasExternal */
    HeapDiskTuple *tuple = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tuple->ResetAttrInfo();
    tuple->SetHasExternal();
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_TUPLE_FLAG_INCONSISTENT));
}

/* ========== MEDIUM 负面校验测试 ========== */

TEST_F(UTHeapPageVerify, MediumDetectsDataHeaderSizeInvalid)
{
    HeapPage *page = InitDefaultPage({80, 90});

    /* 篡改 dataHeader.headerOffset 小于 HEAP_PAGE_DATA_OFFSET，
     * LIGHT 级别先检测到 DataHeaderSize() != HEAP_PAGE_DATA_OFFSET，报 HEAP_HEADER_OFFSET_INVALID */
    page->SetDataHeaderSize(HEAP_PAGE_DATA_OFFSET - 2);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_HEADER_OFFSET_INVALID));
}

TEST_F(UTHeapPageVerify, MediumDetectsLowerTooSmall)
{
    HeapPage *page = InitDefaultPage({81, 91});

    /* 将 lower 设置为小于 DataHeaderSize() + TdDataSize()，触发 line 90-95 */
    uint16 minLower = page->DataHeaderSize() + page->TdDataSize();
    page->SetLower(static_cast<uint16>(minLower - 1));
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

TEST_F(UTHeapPageVerify, MediumDetectsItemIdAlignmentInvalidByOneByte)
{
    HeapPage *page = InitDefaultPage({82, 92});

    /* 给 lower 加 1 字节使 (lower - DataHeaderSize - TdDataSize) % sizeof(ItemId) != 0 */
    page->SetLower(static_cast<uint16>(page->GetLower() + 1));
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_ITEMID_ALIGNMENT_INVALID));
}

TEST_F(UTHeapPageVerify, MediumDetectsItemBoundsInvalid)
{
    HeapPage *page = InitDefaultPage({83, 93});

    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    /* 手动将 ItemId 的 offset 设为低于 GetUpper() 的值，触发 line 108-113 */
    ItemId *itemId = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER);
    itemId->SetNormal(static_cast<uint16>(page->GetUpper() - 1), 32);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

TEST_F(UTHeapPageVerify, MediumDetectsTupleTooSmallExplicit)
{
    HeapPage *page = InitDefaultPage({84, 94});

    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    /* 将 ItemId 的 len 设为 2，小于 HEAP_DISK_TUP_HEADER_SIZE，触发 line 115-120 */
    ItemId *itemId = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER);
    itemId->SetNormal(itemId->GetOffset(), 2);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_TUPLE_HEADER_SIZE_INVALID));
}

/* ========== LIGHT 校验 O(1) 性能测试 ========== */

TEST_F(UTHeapPageVerify, LightVerifyIsConstantTime)
{
    /* 空页面（0 tuples） */
    PageBuffer emptyBuffer{};
    HeapPage *emptyPage = InitHeapPage(emptyBuffer, {85, 95});
    emptyPage->SetChecksum();

    /* 满页面（尽可能多 tuples） */
    PageBuffer fullBuffer{};
    HeapPage *fullPage = InitHeapPage(fullBuffer, {86, 96});
    OffsetNumber off = FIRST_ITEM_OFFSET_NUMBER;
    const uint16 tupleSize = 32;
    /* 持续添加 tuple 直到空间不足 */
    while (fullPage->GetUpper() - fullPage->GetLower() >= static_cast<int>(sizeof(ItemId) + tupleSize)) {
        AddHeapTuple(fullPage, off, tupleSize, 0);
        ++off;
    }
    uint32 tupleCount = off - FIRST_ITEM_OFFSET_NUMBER;
    ASSERT_GT(tupleCount, 50U); /* 确保页面上有足够多的 tuple */
    fullPage->SetChecksum();

    const int iterations = 10000;

    /* 测量空页面 LIGHT 校验耗时 */
    auto startEmpty = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i) {
        VerifyReport localReport;
        (void)VerifyPage(emptyPage, VerifyLevel::LIGHT, &localReport);
    }
    auto endEmpty = std::chrono::high_resolution_clock::now();
    double emptyNs = static_cast<double>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(endEmpty - startEmpty).count());

    /* 测量满页面 LIGHT 校验耗时 */
    auto startFull = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i) {
        VerifyReport localReport;
        (void)VerifyPage(fullPage, VerifyLevel::LIGHT, &localReport);
    }
    auto endFull = std::chrono::high_resolution_clock::now();
    double fullNs = static_cast<double>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(endFull - startFull).count());

    /* LIGHT 校验是 O(1)，耗时比应接近 1.0，使用宽松阈值避免 CI 抖动 */
    double ratio = (emptyNs > 0.0) ? (fullNs / emptyNs) : 1.0;
    EXPECT_LT(ratio, 3.0)
        << "LIGHT verify ratio (full/empty) = " << ratio
        << " (tupleCount=" << tupleCount
        << ", emptyNs=" << emptyNs
        << ", fullNs=" << fullNs << ")";
}

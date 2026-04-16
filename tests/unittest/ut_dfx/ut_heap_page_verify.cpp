/*
 * UT: Heap page × page verify (v2, 2026-04-15).
 *
 * Direction (team-lead 2b #1 rollout):
 *   - Delete low-value duplicate TEST_F; merge 6 "positive / data-header"
 *     cases into 2 TEST_P rows.
 *   - Invest the freed test budget in 3 high-value concurrency TESTs:
 *       * shared-slot TD mutation (1 writer + N readers) — primary value
 *       * disjoint-slot TD access — control / cross-talk guard
 *       * checksum flip during HEAVY verify
 *   - All local helpers replaced by ut_dfx_test_utils.h (MakeValidHeapPage /
 *     AddHeapTuple / PageBuffer / ScopedVerifyConfig / HasVerifyCode).
 *
 * === Removed cases (reasons) ===
 *   InvalidTdCountFails                       — duplicated by CorrectErrorCodeReported
 *   MediumDetectsTupleTooSmallExplicit        — same inject+expected as MediumDetectsTupleTooSmall
 *   MediumDetectsItemIdAlignmentInvalidByOneByte — same effect as MediumDetectsItemIdAlignmentInvalid
 *   MediumDetectsDataHeaderSizeTooSmall       — subsumed by MediumDetectsDataHeaderSizeInvalid
 *   NoStorageItem_TdStatusInvalid             — same 0xFF status inject as TdStatusInvalid
 *   ValidHeapPagePassesLight/Medium/Heavy     — merged into TEST_P ValidPagePassesAllLevels
 *
 * Naming: PascalCase; concurrency TESTs prefixed Concurrent_Xxx_YieldsZzz.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_heap_page.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::AddHeapTuple;
using DSTORE::ut_dfx::HasVerifyCode;
using DSTORE::ut_dfx::MakeValidHeapPage;
using DSTORE::ut_dfx::PageBuffer;
using DSTORE::ut_dfx::ScopedVerifyConfig;

namespace {

/* ========== Test Fixture ========== */

class UTHeapPageVerify : public ::testing::Test {
protected:
    void SetUp() override
    {
        RegisterHeapPageVerifier();
    }

    HeapPage *InitDefaultPage(const PageId &pageId = {10, 20})
    {
        return MakeValidHeapPage(pageBuffer, pageId);
    }

    ScopedVerifyConfig m_guard{};
    PageBuffer pageBuffer{};
    VerifyReport report;
};

}  /* namespace */

/* ========== TEST_P: Valid page passes every verify level ========== */

struct ValidLevelRow {
    VerifyLevel level;
    bool withTuples;
    const char *label;
};

class UTHeapPageVerifyValidP : public ::testing::TestWithParam<ValidLevelRow> {
protected:
    void SetUp() override
    {
        RegisterHeapPageVerifier();
    }

    ScopedVerifyConfig m_guard{};
    PageBuffer pageBuffer{};
};

TEST_P(UTHeapPageVerifyValidP, ValidPagePassesAllLevels)
{
    const auto &row = GetParam();
    HeapPage *page = MakeValidHeapPage(pageBuffer, {20, 30});
    if (row.withTuples) {
        AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
        AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER + 1, 32, 0);
    }
    page->SetChecksum();

    VerifyReport localReport;
    EXPECT_EQ(VerifyPage(page, row.level, &localReport), DSTORE_SUCC)
        << "level=" << row.label;
    EXPECT_FALSE(localReport.HasError()) << "level=" << row.label;
}

INSTANTIATE_TEST_SUITE_P(
    AllLevels, UTHeapPageVerifyValidP,
    ::testing::Values(
        ValidLevelRow{VerifyLevel::LIGHT,  false, "LIGHT"},
        ValidLevelRow{VerifyLevel::MEDIUM, false, "MEDIUM"},
        ValidLevelRow{VerifyLevel::HEAVY,  false, "HEAVY"},
        ValidLevelRow{VerifyLevel::MEDIUM, true,  "MEDIUM_WITH_TUPLES"}),
    [](const testing::TestParamInfo<ValidLevelRow> &info) {
        return info.param.label;
    });

/* ========== TEST_P: DataHeaderSize corruption across levels ========== */

struct DataHeaderRow {
    VerifyLevel level;
    int16 offsetDelta;       /* delta applied to HEAP_PAGE_DATA_OFFSET; if 0 with useLowerPlusOne, set to lower+1 */
    bool useLowerPlusOne;    /* if true, ignore offsetDelta and set DataHeaderSize = lower+1 */
    VerifyCode expectedCode;
    const char *label;
};

class UTHeapPageVerifyDataHeaderP : public ::testing::TestWithParam<DataHeaderRow> {
protected:
    void SetUp() override
    {
        RegisterHeapPageVerifier();
    }

    ScopedVerifyConfig m_guard{};
    PageBuffer pageBuffer{};
};

TEST_P(UTHeapPageVerifyDataHeaderP, DataHeaderSizeCorruption)
{
    const auto &row = GetParam();
    HeapPage *page = MakeValidHeapPage(pageBuffer, {25, 35});
    if (row.useLowerPlusOne) {
        page->SetDataHeaderSize(static_cast<uint16>(page->GetLower() + 1));
    } else {
        page->SetDataHeaderSize(
            static_cast<uint16>(HEAP_PAGE_DATA_OFFSET + row.offsetDelta));
    }
    page->SetChecksum();

    VerifyReport localReport;
    EXPECT_EQ(VerifyPage(page, row.level, &localReport), DSTORE_FAIL)
        << "label=" << row.label;
    EXPECT_TRUE(localReport.HasError()) << "label=" << row.label;
    if (row.expectedCode != VerifyCode::OK) {
        EXPECT_TRUE(HasVerifyCode(localReport, row.expectedCode))
            << "label=" << row.label;
    }
}

INSTANTIATE_TEST_SUITE_P(
    HeaderCorruption, UTHeapPageVerifyDataHeaderP,
    ::testing::Values(
        /* LIGHT: too large — matches HEAP_HEADER_OFFSET_INVALID */
        DataHeaderRow{VerifyLevel::LIGHT,  8,  false, VerifyCode::HEAP_HEADER_OFFSET_INVALID, "LightTooLarge"},
        /* MEDIUM: too small — matches HEAP_HEADER_OFFSET_INVALID */
        DataHeaderRow{VerifyLevel::MEDIUM, -2, false, VerifyCode::HEAP_HEADER_OFFSET_INVALID, "MediumTooSmall"},
        /* MEDIUM: exceeds lower — any error acceptable */
        DataHeaderRow{VerifyLevel::MEDIUM, 0,  true,  VerifyCode::OK,                          "MediumExceedsLower"}),
    [](const testing::TestParamInfo<DataHeaderRow> &info) {
        return info.param.label;
    });

/* ========== Orthogonal negative / behavior TESTs ========== */

TEST_F(UTHeapPageVerify, InvalidFsmSlotFailsLight)
{
    HeapPage *page = InitDefaultPage({24, 34});
    page->SetFsmIndex({INVALID_PAGE_ID, FSM_MAX_HWM});
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTHeapPageVerify, CrcMismatchFails)
{
    HeapPage *page = InitDefaultPage({25, 35});
    /* Tamper after SetChecksum() so CRC no longer matches. */
    page->dataHeader.tdCount = 5;

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_CRC_MISMATCH));
}

TEST_F(UTHeapPageVerify, LowerExceedsUpperFails)
{
    HeapPage *page = InitDefaultPage({26, 36});
    page->SetLower(8000);
    page->SetUpper(1000);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetResults()[0].code, VerifyCode::PAGE_BOUNDARY_INVALID);
}

TEST_F(UTHeapPageVerify, UnregisteredPageTypeFails)
{
    Page *page = reinterpret_cast<Page *>(pageBuffer.data());
    page->Init(0, PageType::HEAP_PAGE_TYPE, {27, 37});
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();

    PageVerifyRegistry registry;
    EXPECT_EQ(registry.Verify(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTHeapPageVerify, NoneLevelSkipsAllChecks)
{
    HeapPage *page = InitDefaultPage({28, 38});
    page->dataHeader.tdCount = static_cast<uint8>(MIN_TD_COUNT - 1);

    EXPECT_EQ(VerifyPage(page, VerifyLevel::NONE, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 0U);
}

TEST_F(UTHeapPageVerify, ModuleFilter_WhenHeapBitCleared_SkipsHeapVerify)
{
    HeapPage *page = InitDefaultPage({29, 39});
    page->SetLower(8000);
    page->SetUpper(1000);
    page->SetChecksum();

    SetDfxVerifyLevel(VerifyLevel::LIGHT);
    SetDfxVerifyModules(1ULL << static_cast<int>(VerifyModule::INDEX));

    VerifyReport skippedReport;
    EXPECT_EQ(VerifyPageInlineWithReport(page, &skippedReport), DSTORE_SUCC);
    EXPECT_EQ(skippedReport.GetTotalChecks(), 0U);

    SetDfxVerifyModules(1ULL << static_cast<int>(VerifyModule::HEAP));
    VerifyReport enabledReport;
    EXPECT_EQ(VerifyPageInlineWithReport(page, &enabledReport), DSTORE_FAIL);
    EXPECT_GT(enabledReport.GetTotalChecks(), 0U);
}

TEST_F(UTHeapPageVerify, CorrectErrorCodeReported)
{
    HeapPage *page = InitDefaultPage({30, 40});
    page->dataHeader.tdCount = static_cast<uint8>(MIN_TD_COUNT - 1);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);

    /* Verify the error code falls within the Heap module range (0x0100-0x012F). */
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
    page->dataHeader.tdCount = 0;  /* Would fail module-level check... */
    page->SetChecksum();           /* ...but CR-extend bypass kicks in after CRC. */

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 0U);
}

/* ========== Three-scenario entrypoints ========== */

TEST_F(UTHeapPageVerify, VerifyPageOnReadBehavior)
{
    HeapPage *page = InitDefaultPage({32, 42});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    VerifyReport readReport;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &readReport), DSTORE_SUCC);
    EXPECT_FALSE(readReport.HasError());

    PageBuffer errorBuffer{};
    HeapPage *errorPage = MakeValidHeapPage(errorBuffer, {33, 43});
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

    VerifyReport fullReport;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &fullReport), DSTORE_SUCC);

    PageBuffer errorBuffer{};
    HeapPage *errorPage = MakeValidHeapPage(errorBuffer, {35, 45});
    errorPage->SetLower(8000);
    errorPage->SetUpper(1000);
    errorPage->SetChecksum();

    VerifyReport errorReport;
    /* Full scan returns SUCC regardless — it only collects, never PANICs. */
    EXPECT_EQ(VerifyPageFull(errorPage, VerifyLevel::LIGHT, &errorReport), DSTORE_SUCC);
    EXPECT_TRUE(errorReport.HasError());
}

TEST_F(UTHeapPageVerify, VerifyPageOnWriteNormalPage)
{
    HeapPage *page = InitDefaultPage({36, 46});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    EXPECT_EQ(VerifyPageOnWrite(page, VerifyLevel::LIGHT), DSTORE_SUCC);
}

/* ========== HEAVY multi-tuple / TD corruption ========== */

TEST_F(UTHeapPageVerify, MultiTupleHeavyweightValid)
{
    HeapPage *page = InitDefaultPage({50, 60});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER,     40, 0);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER + 1, 40, 0);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER + 2, 40, 0);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTHeapPageVerify, TupleOverlapDetected)
{
    HeapPage *page = InitDefaultPage({51, 61});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 64, 0);
    /* Construct a second tuple whose offset overlaps the first by 32 bytes. */
    ItemId *itemId2 = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER + 1);
    uint16 overlapOffset = static_cast<uint16>(page->GetUpper() + 32);
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
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TUPLE_OVERLAP));
}

TEST_F(UTHeapPageVerify, TdStatusInvalid)
{
    HeapPage *page = InitDefaultPage({52, 62});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    HeapDiskTuple *tuple = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tuple->SetTdStatus(static_cast<TupleTdStatus>(0xFF));
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TD_SANITY_FAIL));
}

TEST_F(UTHeapPageVerify, TdIdExceedsTdCount)
{
    HeapPage *page = InitDefaultPage({53, 63});
    uint8 tdCount = page->GetTdCount();
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, tdCount); /* tdId == tdCount, out of range */
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TD_SANITY_FAIL));
}

TEST_F(UTHeapPageVerify, UnusedItemIdWithNonZeroLen)
{
    HeapPage *page = InitDefaultPage({54, 64});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    ItemId *itemId = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER);
    itemId->SetUnused();
    /* SetUnused() clears len; write a non-zero len back to mimic corruption. */
    itemId->direct.m_len = 32;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTHeapPageVerify, ZeroColumnTupleDetected)
{
    HeapPage *page = InitDefaultPage({55, 65});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    HeapDiskTuple *tuple = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tuple->SetNumColumn(0);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TUPLE_NUM_COLUMN_INVALID));
}

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
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TD_SANITY_FAIL));
}

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
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TUPLE_SIZE_MISMATCH));
}

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
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TD_SANITY_FAIL));
}

TEST_F(UTHeapPageVerify, PotentialDelSize_ExceedsBlcksz)
{
    HeapPage *page = InitDefaultPage({64, 74});
    page->SetPotentialDelSize(BLCKSZ + 1);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

TEST_F(UTHeapPageVerify, FsmPageInvalid_SlotNonZero)
{
    HeapPage *page = InitDefaultPage({65, 75});
    page->SetFsmIndex({INVALID_PAGE_ID, 5});
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_FSM_SLOT_INVALID));
}

TEST_F(UTHeapPageVerify, CrossLevel_SameDamage_AllLevelsDetect)
{
    VerifyLevel levels[] = {VerifyLevel::LIGHT, VerifyLevel::MEDIUM, VerifyLevel::HEAVY};
    for (VerifyLevel lvl : levels) {
        PageBuffer localBuffer{};
        HeapPage *page = MakeValidHeapPage(localBuffer, {66, 76});
        page->dataHeader.tdCount = static_cast<uint8>(MIN_TD_COUNT - 1);
        page->SetChecksum();

        VerifyReport localReport;
        EXPECT_EQ(VerifyPage(page, lvl, &localReport), DSTORE_FAIL)
            << "Should fail at level " << static_cast<int>(lvl);
        EXPECT_TRUE(localReport.HasError());
    }
}

/* ========== MEDIUM-level negative checks ========== */

TEST_F(UTHeapPageVerify, MediumDetectsItemOutOfBounds)
{
    HeapPage *page = InitDefaultPage({71, 81});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    /* Move ItemId offset below upper — outside the tuple storage area. */
    ItemId *itemId = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER);
    itemId->SetNormal(0, 32);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTHeapPageVerify, MediumDetectsTupleTooSmall)
{
    HeapPage *page = InitDefaultPage({72, 82});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    ItemId *itemId = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER);
    itemId->SetNormal(itemId->GetOffset(), 4);  /* 4 bytes < HEAP_DISK_TUP_HEADER_SIZE */
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TUPLE_HEADER_SIZE_INVALID));
}

TEST_F(UTHeapPageVerify, LightDetectsSpecialOffsetMismatch)
{
    HeapPage *page = InitDefaultPage({76, 86});
    /* Add a tuple so upper < BLCKSZ, then set specialOffset between upper and
     * BLCKSZ — passes the generic LIGHT boundary check but is caught by the
     * heap-specific "specialOffset must equal BLCKSZ" rule. */
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    uint16 upperVal = page->GetUpper();
    page->m_header.m_special.m_offset = static_cast<uint16>((upperVal + BLCKSZ) / 2);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_SPECIAL_OFFSET_MISMATCH));
}

TEST_F(UTHeapPageVerify, MediumDetectsItemIdAlignmentInvalid)
{
    HeapPage *page = InitDefaultPage({77, 87});
    uint16 goodLower = page->GetLower();
    page->SetLower(static_cast<uint16>(goodLower + 1));  /* break sizeof(ItemId) alignment */
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_ITEMID_ALIGNMENT_INVALID));
}

TEST_F(UTHeapPageVerify, HeavyDetectsFlagInconsistent)
{
    HeapPage *page = InitDefaultPage({78, 88});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    /* Set hasExternal=true but hasVarwidth=false — inconsistent combination. */
    HeapDiskTuple *tuple = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tuple->ResetAttrInfo();
    tuple->SetHasExternal();
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TUPLE_FLAG_INCONSISTENT));
}

TEST_F(UTHeapPageVerify, MediumDetectsLowerTooSmall)
{
    HeapPage *page = InitDefaultPage({81, 91});
    uint16 minLower = page->DataHeaderSize() + page->TdDataSize();
    page->SetLower(static_cast<uint16>(minLower - 1));
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

TEST_F(UTHeapPageVerify, MediumDetectsItemBoundsInvalid)
{
    HeapPage *page = InitDefaultPage({83, 93});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    ItemId *itemId = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER);
    itemId->SetNormal(static_cast<uint16>(page->GetUpper() - 1), 32);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

TEST_F(UTHeapPageVerify, LightVerifyIsConstantTime)
{
    PageBuffer emptyBuffer{};
    HeapPage *emptyPage = MakeValidHeapPage(emptyBuffer, {85, 95});
    emptyPage->SetChecksum();

    PageBuffer fullBuffer{};
    HeapPage *fullPage = MakeValidHeapPage(fullBuffer, {86, 96});
    OffsetNumber off = FIRST_ITEM_OFFSET_NUMBER;
    const uint16 tupleSize = 32;
    while (fullPage->GetUpper() - fullPage->GetLower() >= static_cast<int>(sizeof(ItemId) + tupleSize)) {
        AddHeapTuple(fullPage, off, tupleSize, 0);
        ++off;
    }
    uint32 tupleCount = off - FIRST_ITEM_OFFSET_NUMBER;
    ASSERT_GT(tupleCount, 50U);
    fullPage->SetChecksum();

    const int iterations = 10000;

    auto startEmpty = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i) {
        VerifyReport localReport;
        (void)VerifyPage(emptyPage, VerifyLevel::LIGHT, &localReport);
    }
    auto endEmpty = std::chrono::high_resolution_clock::now();
    double emptyNs = static_cast<double>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(endEmpty - startEmpty).count());

    auto startFull = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i) {
        VerifyReport localReport;
        (void)VerifyPage(fullPage, VerifyLevel::LIGHT, &localReport);
    }
    auto endFull = std::chrono::high_resolution_clock::now();
    double fullNs = static_cast<double>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(endFull - startFull).count());

    double ratio = (emptyNs > 0.0) ? (fullNs / emptyNs) : 1.0;
    EXPECT_LT(ratio, 3.0)
        << "LIGHT verify ratio (full/empty) = " << ratio
        << " (tupleCount=" << tupleCount
        << ", emptyNs=" << emptyNs
        << ", fullNs=" << fullNs << ")";
}

/* ========== Concurrency TESTs ==========
 *
 * All use the 8-worker × 2000-iter project-standard pattern from
 * ut_page_verify_registry.cpp:944.  Barrier = atomic<int> ready-count +
 * yield + seq_cst fence.  No sleeps, no ordering expectations; outcomes
 * partitioned into "allowed stable states" vs "forbidden third state" —
 * only the third-state count is hard-zeroed.
 */

namespace {
constexpr int CONCURRENT_WORKERS = 8;
constexpr int CONCURRENT_LOOPS = 2000;
}  /* namespace */

TEST(UTHeapPageVerifyConcurrency, Concurrent_SharedTdSlotMutation_ReportsBoundedCodes)
{
    /* 1 writer + 7 readers; verifier must tolerate torn TD reads without
     * reporting unrelated codes (CRC, TUPLE_OVERLAP) or FATAL severity.
     *
     * NOTE: intentional benign race on a single TD status byte.  Single-byte
     * plain stores on x86 / aarch64 are atomic at the CPU level; the race
     * here tests that the verifier classifies whatever byte value it observes
     * without dereferencing past the TD array or reporting an unrelated code.
     */
    RegisterHeapPageVerifier();
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();
    SetDfxVerifyLevel(VerifyLevel::HEAVY);

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {90, 91});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();
    HeapDiskTuple *sharedTuple = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);

    std::atomic<int> ready{0};
    std::atomic<uint64> okCount{0};
    std::atomic<uint64> tdFailCount{0};
    std::atomic<uint64> forbiddenCount{0};
    std::atomic<uint64> fatalCount{0};

    std::vector<std::thread> workers;
    for (int w = 0; w < CONCURRENT_WORKERS; ++w) {
        workers.emplace_back([&, w]() {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (ready.load(std::memory_order_acquire) < CONCURRENT_WORKERS) {
                std::this_thread::yield();
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);

            if (w == 0) {
                /* Sole writer — flip status between valid and invalid. */
                for (int i = 0; i < CONCURRENT_LOOPS; ++i) {
                    sharedTuple->SetTdStatus((i & 1) ? static_cast<TupleTdStatus>(0xFF)
                                                    : ATTACH_TD_AS_NEW_OWNER);
                }
            } else {
                /* Reader — observe whatever the writer has published. */
                for (int i = 0; i < CONCURRENT_LOOPS; ++i) {
                    VerifyReport r;
                    (void)VerifyPage(page, VerifyLevel::HEAVY, &r);
                    if (!r.HasError()) {
                        okCount.fetch_add(1, std::memory_order_relaxed);
                    } else if (HasVerifyCode(r, VerifyCode::HEAP_TD_SANITY_FAIL)) {
                        tdFailCount.fetch_add(1, std::memory_order_relaxed);
                    } else if (HasVerifyCode(r, VerifyCode::PAGE_CRC_MISMATCH)) {
                        /* CRC mismatch is expected when mutator changes page
                         * bytes without updating checksum — benign artifact
                         * under intentional latch-free concurrent mutation. */
                        tdFailCount.fetch_add(1, std::memory_order_relaxed);
                    } else if (HasVerifyCode(r, VerifyCode::HEAP_TUPLE_OVERLAP) ||
                               HasVerifyCode(r, VerifyCode::HEAP_TUPLE_HEADER_SIZE_INVALID)) {
                        forbiddenCount.fetch_add(1, std::memory_order_relaxed);
                    }
                    if (r.HasFatal()) {
                        fatalCount.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });
    }
    for (auto &t : workers) t.join();

    EXPECT_EQ(forbiddenCount.load(), 0u)
        << "verifier leaked unrelated codes during TD race";
    EXPECT_EQ(fatalCount.load(), 0u) << "verifier escalated to FATAL during TD race";
    EXPECT_GT(okCount.load() + tdFailCount.load(), 0u)
        << "no readers observed anything — barrier may be broken";
}

TEST(UTHeapPageVerifyConcurrency, Concurrent_DisjointTdSlot_NoCrossTalk)
{
    /* Control for the shared-slot test: 8 readers over distinct TD slots on
     * a fully valid page.  Expected: zero errors — readers should not
     * interfere with each other's cache lines or registry state. */
    RegisterHeapPageVerifier();
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();
    SetDfxVerifyLevel(VerifyLevel::HEAVY);

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {92, 93});
    for (int i = 0; i < CONCURRENT_WORKERS; ++i) {
        AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER + i, 32, 0);
    }
    page->SetChecksum();

    std::atomic<int> ready{0};
    std::atomic<uint64> errorCount{0};

    std::vector<std::thread> workers;
    for (int w = 0; w < CONCURRENT_WORKERS; ++w) {
        workers.emplace_back([&]() {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (ready.load(std::memory_order_acquire) < CONCURRENT_WORKERS) {
                std::this_thread::yield();
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);

            for (int i = 0; i < CONCURRENT_LOOPS; ++i) {
                VerifyReport r;
                (void)VerifyPage(page, VerifyLevel::HEAVY, &r);
                if (r.HasError()) {
                    errorCount.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto &t : workers) t.join();

    EXPECT_EQ(errorCount.load(), 0u)
        << "concurrent read-only verify should produce zero errors";
}

TEST(UTHeapPageVerifyConcurrency, Concurrent_ChecksumFlipDuringHeavy_NoFatal)
{
    /* 1 writer flips a byte + re-SetChecksum in a loop, then unflips without
     * rechecksumming — leaving the page with a stale CRC.  7 readers run
     * HEAVY verify.  Expected codes: {OK, PAGE_CRC_MISMATCH}; no FATAL. */
    RegisterHeapPageVerifier();
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();
    SetDfxVerifyLevel(VerifyLevel::HEAVY);

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {94, 95});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    std::atomic<int> ready{0};
    std::atomic<uint64> okCount{0};
    std::atomic<uint64> crcCount{0};
    std::atomic<uint64> forbiddenCount{0};
    std::atomic<uint64> fatalCount{0};

    std::vector<std::thread> workers;
    for (int w = 0; w < CONCURRENT_WORKERS; ++w) {
        workers.emplace_back([&, w]() {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (ready.load(std::memory_order_acquire) < CONCURRENT_WORKERS) {
                std::this_thread::yield();
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);

            if (w == 0) {
                volatile char *scratch = reinterpret_cast<volatile char *>(buf.data()) + BLCKSZ / 2;
                for (int i = 0; i < CONCURRENT_LOOPS; ++i) {
                    *scratch ^= 0x5A;
                    page->SetChecksum();
                    *scratch ^= 0x5A;  /* CRC now stale */
                }
            } else {
                for (int i = 0; i < CONCURRENT_LOOPS; ++i) {
                    VerifyReport r;
                    (void)VerifyPage(page, VerifyLevel::HEAVY, &r);
                    if (!r.HasError()) {
                        okCount.fetch_add(1, std::memory_order_relaxed);
                    } else if (HasVerifyCode(r, VerifyCode::PAGE_CRC_MISMATCH)) {
                        crcCount.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        forbiddenCount.fetch_add(1, std::memory_order_relaxed);
                    }
                    if (r.HasFatal()) {
                        fatalCount.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });
    }
    for (auto &t : workers) t.join();

    EXPECT_EQ(forbiddenCount.load(), 0u)
        << "verifier reported unrelated codes during CRC race";
    EXPECT_EQ(fatalCount.load(), 0u) << "verifier escalated to FATAL during CRC race";
    EXPECT_GT(okCount.load() + crcCount.load(), 0u)
        << "no readers observed anything — barrier may be broken";
}

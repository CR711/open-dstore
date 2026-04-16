/*
 * UT for index page verify (BtrPage).  v2 delta vs v1:
 *   - Param-merged 3 TEST_P families (-10 TEST_F, +3 TEST_P)
 *   - Deleted 2 redundant TEST_F (UninitializedPageDetected /
 *     SingleDataTupleKeyOrderValid — both fully covered by retained cases)
 *   - Added 3 concurrency TEST (B-link specific: splitStat flip, right
 *     sibling flip, high-key disjoint control)
 *   - Switched all local helpers to ut_dfx::MakeValidIndexPage /
 *     AddIndexTuple; removed anonymous-namespace duplicates
 *   - All tests use ScopedVerifyConfig RAII and EnableAllModules per
 *     team-lead red line
 *
 * === Removed cases (reasons) ===
 *   ValidIndexPagePasses / ValidIndexPagePassesMedium /
 *     MultiTupleHeavyweightValid / MediumLevelValid
 *     → folded into ValidPagePassesAllLevels (4 rows).
 *   UninitializedPageDetected → equivalent to MetaPageIdInvalid (same
 *     corruption, MetaPageIdInvalid holds the stronger code assertion).
 *   LightDetectsInvalidBtrPageType / LightDetectsOutOfRangeBtrPageType
 *     → folded into InvalidPageTypeCorruption (2 rows).
 *   HeavyweightKeyOrderValid / HeavyweightKeyOrderInvalid /
 *     FiveTupleKeyOrderValid / FiveTupleKeyOrderInvalid_MiddleSwap
 *     → folded into KeyOrderCorruption (4 rows).
 *   SingleDataTupleKeyOrderValid → strict subset of FiveTupleKeyOrderValid.
 */
#include <atomic>
#include <cstring>
#include <thread>
#include <vector>
#include <gtest/gtest.h>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_index_page.h"
#include "tuple/dstore_index_tuple.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;
using DSTORE::ut_dfx::MakeValidIndexPage;
using DSTORE::ut_dfx::AddIndexTuple;
using DSTORE::ut_dfx::ScopedVerifyConfig;

namespace {

constexpr int CONCURRENT_WORKERS = 8;
constexpr int CONCURRENT_LOOPS = 2000;

/* Write `byteVal` into the key payload region of an existing tuple. */
inline void SetTupleKeyBytes(BtrPage *page, OffsetNumber offset, uint16 tupleSize, unsigned char byteVal)
{
    const IndexTuple *tuple = page->GetIndexTuple(offset);
    char *keyData = reinterpret_cast<char *>(const_cast<IndexTuple *>(tuple)) + INDEX_TUPLE_SIZE;
    uint16 keyLen = static_cast<uint16>(tupleSize - INDEX_TUPLE_SIZE);
    std::memset(keyData, byteVal, keyLen);
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
        return MakeValidIndexPage(pageBuffer, pageId);
    }
};

}  // namespace

/* ========== Parameterized valid-page family ========== */

struct ValidIndexPageCase {
    VerifyLevel level;
    bool multiTuple;  /* if true, add 3 data tuples instead of 1 high-key */
    const char *label;
};

class UTIndexPageValid : public ::testing::TestWithParam<ValidIndexPageCase> {
protected:
    void SetUp() override
    {
        RegisterIndexPageVerifier();
    }
    PageBuffer pageBuffer{};
    VerifyReport report;
};

TEST_P(UTIndexPageValid, ValidPagePassesAllLevels)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    const auto &p = GetParam();
    BtrPage *page = MakeValidIndexPage(pageBuffer, {30, 40});

    if (p.multiTuple) {
        AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
        AddIndexTuple(page, BTREE_PAGE_HIKEY + 1, 24, 0);
        AddIndexTuple(page, BTREE_PAGE_HIKEY + 2, 24, 0);
    } else {
        AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    }
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, p.level, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

INSTANTIATE_TEST_SUITE_P(AllLevels, UTIndexPageValid,
    ::testing::Values(
        ValidIndexPageCase{VerifyLevel::LIGHT, false, "light_singletuple"},
        ValidIndexPageCase{VerifyLevel::MEDIUM, false, "medium_singletuple"},
        ValidIndexPageCase{VerifyLevel::HEAVY, false, "heavy_singletuple"},
        ValidIndexPageCase{VerifyLevel::HEAVY, true, "heavy_multituple"}),
    [](const ::testing::TestParamInfo<ValidIndexPageCase> &info) {
        return info.param.label;
    });

/* ========== Parameterized invalid btree page type family ========== */

struct InvalidPageTypeCase {
    uint16 rawType;  /* raw bit-pattern to write into status.bitVal.type */
    const char *label;
};

class UTIndexPageTypeCorruption : public ::testing::TestWithParam<InvalidPageTypeCase> {
protected:
    void SetUp() override
    {
        RegisterIndexPageVerifier();
    }
    PageBuffer pageBuffer{};
    VerifyReport report;
};

TEST_P(UTIndexPageTypeCorruption, InvalidPageTypeDetected)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    const auto &p = GetParam();
    BtrPage *page = MakeValidIndexPage(pageBuffer, {50, 60});
    page->GetLinkAndStatus()->status.bitVal.type = p.rawType;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::BTR_PAGE_TYPE_INVALID))
        << "Expected BTR_PAGE_TYPE_INVALID for raw type " << p.rawType;
}

INSTANTIATE_TEST_SUITE_P(Types, UTIndexPageTypeCorruption,
    ::testing::Values(
        /* 0 = INVALID_BTR_PAGE (explicit invalid sentinel) */
        InvalidPageTypeCase{static_cast<uint16>(BtrPageType::INVALID_BTR_PAGE), "invalid_sentinel"},
        /* META_PAGE(3)+1 = 4 wraps to 0 in 2-bit field — same invalid */
        InvalidPageTypeCase{static_cast<uint16>(static_cast<uint16>(BtrPageType::META_PAGE) + 1), "wraparound"}),
    [](const ::testing::TestParamInfo<InvalidPageTypeCase> &info) {
        return info.param.label;
    });

/* ========== Parameterized key-ordering family ========== */

struct KeyOrderCase {
    int dataTupleCount;         /* 2 or 5 data tuples after hikey */
    std::array<unsigned char, 5> keyBytes;  /* key bytes per data tuple (index 0..count-1) */
    RetStatus expectedStatus;
    bool expectError;
    const char *label;
};

class UTIndexPageKeyOrder : public ::testing::TestWithParam<KeyOrderCase> {
protected:
    void SetUp() override
    {
        RegisterIndexPageVerifier();
    }
    PageBuffer pageBuffer{};
    VerifyReport report;
};

TEST_P(UTIndexPageKeyOrder, KeyOrderCorruption)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    const auto &p = GetParam();
    const uint16 tupleSize = 32;  /* INDEX_TUPLE_SIZE(16) + 16 key bytes */
    BtrPage *page = MakeValidIndexPage(pageBuffer, {55, 65});

    AddIndexTuple(page, BTREE_PAGE_HIKEY, tupleSize, 0);  /* hikey */
    for (int i = 0; i < p.dataTupleCount; ++i) {
        OffsetNumber off = static_cast<OffsetNumber>(BTREE_PAGE_HIKEY + 1 + i);
        AddIndexTuple(page, off, tupleSize, 0);
        SetTupleKeyBytes(page, off, tupleSize, p.keyBytes[i]);
    }

    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), p.expectedStatus);
    EXPECT_EQ(report.HasError(), p.expectError);
}

INSTANTIATE_TEST_SUITE_P(Orderings, UTIndexPageKeyOrder,
    ::testing::Values(
        /* 2-tuple ascending: 0x10 < 0x20 */
        KeyOrderCase{2, {0x10, 0x20, 0, 0, 0}, DSTORE_SUCC, false, "two_ascending"},
        /* 2-tuple descending (invalid): 0x30 > 0x10 */
        KeyOrderCase{2, {0x30, 0x10, 0, 0, 0}, DSTORE_FAIL, true, "two_descending_invalid"},
        /* 5-tuple strict ascending */
        KeyOrderCase{5, {0x10, 0x20, 0x30, 0x40, 0x50}, DSTORE_SUCC, false, "five_ascending"},
        /* 5-tuple with middle swap (tuple 3 & 4 swapped): 0x10,0x20,0x40,0x30,0x50 */
        KeyOrderCase{5, {0x10, 0x20, 0x40, 0x30, 0x50}, DSTORE_FAIL, true, "five_middle_swap_invalid"}),
    [](const ::testing::TestParamInfo<KeyOrderCase> &info) {
        return info.param.label;
    });

/* ========== Remaining single-shot TEST_F (each exercises a distinct field) ========== */

TEST_F(UTIndexPageVerify, InvalidSpecialOffsetFails)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({31, 41});
    page->SetSpecialOffset(static_cast<uint16>(page->GetSpecialOffset() - 8));
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, MissingHighKeyFails)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({32, 42});
    page->GetLinkAndStatus()->SetRight({2, 2});
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, MetaPageTdCountNonZeroFails)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({35, 45});
    page->GetLinkAndStatus()->SetType(BtrPageType::META_PAGE);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, TdIdExceedsTdCount)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({36, 46});
    uint8 tdCount = page->GetTdCount();
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, tdCount);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, TupleOverlapDetected)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({37, 47});

    AddIndexTuple(page, BTREE_PAGE_HIKEY, 48, 0);
    ItemId *itemId2 = page->GetItemIdPtr(BTREE_PAGE_HIKEY + 1);
    uint16 overlapOffset = static_cast<uint16>(page->GetUpper() + 24);
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
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({38, 48});
    page->SetLower(0);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, MetaPageIdInvalid)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({40, 50});
    /* Invalidating btrMetaPageId also makes IsInitialized() false — the
     * verifier additionally reports BTR_META_PAGE_ID_INVALID, which is
     * the strictly stronger assertion compared to the deleted
     * UninitializedPageDetected (which only asserted PAGE_BOUNDARY_INVALID). */
    page->GetLinkAndStatus()->btrMetaPageId = INVALID_PAGE_ID;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::BTR_META_PAGE_ID_INVALID));
}

TEST_F(UTIndexPageVerify, UnusedDataItemInActiveRegion)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({41, 51});
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 1, 24, 0);
    AddIndexTuple(page, BTREE_PAGE_HIKEY + 2, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    ItemId *itemId3 = page->GetItemIdPtr(BTREE_PAGE_HIKEY + 2);
    itemId3->SetUnused();
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, ItemSizeMismatch)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({42, 52});
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    ItemId *itemId = page->GetItemIdPtr(BTREE_PAGE_HIKEY);
    itemId->SetNormal(itemId->GetOffset(), 8);  /* len=8 but tuple size=24 */
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::INDEX_TUPLE_SIZE_MISMATCH));
}

TEST_F(UTIndexPageVerify, MediumDetectsLeafLevelNonZero)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({44, 54});
    page->GetLinkAndStatus()->SetLevel(3);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, MediumDetectsRightSibSelfReference)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({45, 55});
    page->GetLinkAndStatus()->SetRight(page->GetSelfPageId());
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::PAGE_ID_INVALID));
}

TEST_F(UTIndexPageVerify, MediumDetectsLeftSibSelfReference)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({46, 56});
    page->GetLinkAndStatus()->SetLeft(page->GetSelfPageId());
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::PAGE_ID_INVALID));
}

TEST_F(UTIndexPageVerify, MediumDetectsInternalPageLevelZero)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({47, 57});
    page->GetLinkAndStatus()->SetType(BtrPageType::INTERNAL_PAGE);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, MediumDetectsItemOutOfBounds)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({48, 58});
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    ItemId *itemId = page->GetItemIdPtr(BTREE_PAGE_HIKEY);
    itemId->SetNormal(0, 24);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTIndexPageVerify, LightPassesMaxValidLiveStat)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({49, 59});
    page->GetLinkAndStatus()->SetLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTIndexPageVerify, LightDetectsInvalidSplitStat)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({52, 62});
    page->GetLinkAndStatus()->status.bitVal.splitStat = 2;  /* out of {0,1} */
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::BTR_SPLIT_STAT_INVALID));
}

TEST_F(UTIndexPageVerify, LightPassesValidSplitIncomplete)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({53, 63});
    page->GetLinkAndStatus()->SetSplitStatus(BtrPageSplitStatus::SPLIT_INCOMPLETE);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTIndexPageVerify, DuplicateKeysAllowed)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    const uint16 tupleSize = 32;
    BtrPage *page = InitDefaultPage({58, 68});

    AddIndexTuple(page, BTREE_PAGE_HIKEY, tupleSize, 0);
    for (int i = 0; i < 3; ++i) {
        OffsetNumber off = static_cast<OffsetNumber>(BTREE_PAGE_HIKEY + 1 + i);
        AddIndexTuple(page, off, tupleSize, 0);
        SetTupleKeyBytes(page, off, tupleSize, 0x20);  /* all duplicates */
    }
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTIndexPageVerify, ItemIdOffsetMisaligned)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    BtrPage *page = InitDefaultPage({60, 70});
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    AddIndexTuple(page, BTREE_PAGE_FIRSTKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);

    ItemId *itemId = page->GetItemIdPtr(BTREE_PAGE_HIKEY);
    uint16 originalOffset = itemId->GetOffset();
    uint16 misaligned = static_cast<uint16>(originalOffset - 1);
    itemId->SetNormal(misaligned, 24);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::INDEX_ITEMID_OFFSET_MISALIGNED));
}

/* ========== Concurrency tests (B-link specific) ==========
 *
 * Model: 1 writer worker (w==0) flips a single field on a shared valid
 * page; 7 readers run verifier in parallel.  Contract: verifier must
 * report ONLY the expected bounded set of codes (stable state or the
 * known transition code), never FATAL, never unrelated codes.
 *
 * Torn-read guardrail (team-lead 2026-04-15): readers track
 * `forbiddenCount` for codes outside the expected set — if the verifier
 * implementation regresses (e.g. starts re-computing CRC when it
 * shouldn't), these UTs catch it.
 *
 * Shared-slot Y scheme rationale: single-byte plain stores are
 * hardware-atomic on x86 and aarch64; torn-read tolerance is the
 * contract under test, so we intentionally allow the benign race
 * rather than introducing atomic_ref (C++20) or CAS (noise).
 *
 * No independent mutator/stopper thread exists in these TESTs — writer
 * and readers run a fixed CONCURRENT_LOOPS, so new C2 ordering rule
 * (verifiers.join → stopMutator.store → mutator.join) does not apply.
 */

TEST(UTIndexPageVerifyConcurrency, Concurrent_SplitIncompleteFlip_ReportsBoundedCodes)
{
    /* Single writer flips splitStat between SPLIT_COMPLETE(0) and
     * SPLIT_INCOMPLETE(1); 7 readers run LIGHT verify.  Both values are
     * individually valid, so every read must return OK. */
    RegisterIndexPageVerifier();
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();
    SetDfxVerifyLevel(VerifyLevel::LIGHT);

    PageBuffer buf{};
    BtrPage *page = MakeValidIndexPage(buf, {200, 201});
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();
    BtrPageLinkAndStatus *link = page->GetLinkAndStatus();

    std::atomic<int> ready{0};
    std::atomic<uint64> okCount{0};
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
                for (int i = 0; i < CONCURRENT_LOOPS; ++i) {
                    link->SetSplitStatus((i & 1) ? BtrPageSplitStatus::SPLIT_INCOMPLETE
                                                 : BtrPageSplitStatus::SPLIT_COMPLETE);
                }
            } else {
                for (int i = 0; i < CONCURRENT_LOOPS; ++i) {
                    VerifyReport r;
                    (void)VerifyPage(page, VerifyLevel::LIGHT, &r);
                    if (!r.HasError()) {
                        okCount.fetch_add(1, std::memory_order_relaxed);
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
        << "verifier flagged valid splitStat transitions as errors";
    EXPECT_EQ(fatalCount.load(), 0u) << "verifier escalated to FATAL during splitStat race";
    EXPECT_GT(okCount.load(), 0u) << "no readers observed anything — barrier may be broken";
}

TEST(UTIndexPageVerifyConcurrency, Concurrent_RightSiblingFlip_NoFatal)
{
    /* Writer alternates right-sibling between INVALID_PAGE_ID (valid:
     * rightmost page) and self-PageId (invalid: PAGE_ID_INVALID).
     * Readers run MEDIUM verify, which checks sibling self-reference.
     * Expected bounded set: { OK, PAGE_ID_INVALID }. */
    RegisterIndexPageVerifier();
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();
    SetDfxVerifyLevel(VerifyLevel::MEDIUM);

    PageBuffer buf{};
    BtrPage *page = MakeValidIndexPage(buf, {202, 203});
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();
    BtrPageLinkAndStatus *link = page->GetLinkAndStatus();
    const PageId selfId = page->GetSelfPageId();

    std::atomic<int> ready{0};
    std::atomic<uint64> okCount{0};
    std::atomic<uint64> pageIdInvalidCount{0};
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
                for (int i = 0; i < CONCURRENT_LOOPS; ++i) {
                    link->SetRight((i & 1) ? selfId : INVALID_PAGE_ID);
                }
            } else {
                for (int i = 0; i < CONCURRENT_LOOPS; ++i) {
                    VerifyReport r;
                    (void)VerifyPage(page, VerifyLevel::MEDIUM, &r);
                    if (!r.HasError()) {
                        okCount.fetch_add(1, std::memory_order_relaxed);
                    } else if (DSTORE::ut_dfx::HasVerifyCode(r, VerifyCode::PAGE_ID_INVALID)) {
                        pageIdInvalidCount.fetch_add(1, std::memory_order_relaxed);
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
        << "verifier reported unrelated codes during right-sibling race";
    EXPECT_EQ(fatalCount.load(), 0u) << "verifier escalated to FATAL during sibling race";
    EXPECT_GT(okCount.load() + pageIdInvalidCount.load(), 0u)
        << "no readers observed anything — barrier may be broken";
}

TEST(UTIndexPageVerifyConcurrency, Concurrent_HighKeyDisjointReaders_NoCrossTalk)
{
    /* Control test (disjoint reader model): 8 readers run HEAVY verify
     * against a page with hikey + 5 ascending-key data tuples.  No
     * writer.  Expected: zero errors — readers must not interfere with
     * each other's cache lines, registry state, or verifier scratch. */
    RegisterIndexPageVerifier();
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();
    SetDfxVerifyLevel(VerifyLevel::HEAVY);

    const uint16 tupleSize = 32;
    PageBuffer buf{};
    BtrPage *page = MakeValidIndexPage(buf, {204, 205});
    AddIndexTuple(page, BTREE_PAGE_HIKEY, tupleSize, 0);
    for (int i = 0; i < 5; ++i) {
        OffsetNumber off = static_cast<OffsetNumber>(BTREE_PAGE_HIKEY + 1 + i);
        AddIndexTuple(page, off, tupleSize, 0);
        SetTupleKeyBytes(page, off, tupleSize, static_cast<unsigned char>(0x10 * (i + 1)));
    }
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
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
        << "concurrent read-only HEAVY verify should produce zero errors";
}

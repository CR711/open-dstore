/*
 * UT: Fault injection × page verify (v2.1).
 *
 * Direction (team-lead 2026-04-15):
 *   - Low-value copy-paste TEST_F deleted; category representatives kept
 *     via TEST_P; freed budget spent on concurrency / dynamic-state cases
 *     that mirror real production races (not more field flips).
 *
 * Red lines (team-lead 2026-04-15, reviewer round 1):
 *   1. GUC always guarded by ScopedVerifyConfig (no naked Set*).
 *   2. Shared helpers from ut_dfx_test_utils.h — no private copies.
 *   3. Concurrency: std::thread + std::atomic, ≥4 workers, assertions
 *      must actually trap a race (two-valued stable-state check).
 *   4. Assert precision = VerifyCode + VerifySeverity + count
 *      (not HasError()).
 *   5. PascalCase test names retained (team-lead 10:xx 松绑 TestXxx_...).
 *
 * Reviewer round-1 fixes rolled in:
 *   - Added concurrency suite (阻塞 #1): SameCorruption / GucLevelToggle
 *     / MixedPages / DynamicCorruption / LightAndHeavy (B 裁决).
 *   - Fixture-level ScopedVerifyConfig + EnableAllModules (阻塞 #2).
 *   - Removed weak "NONE sentinel" cases; every row now has a concrete
 *     VerifyCode expectation (阻塞 #3).
 *   - MultipleFaults_FirstCaught now EXPECT_EQ(errorCount,1U) +
 *     EXPECT_FALSE(HasVerifyCode(...FSM...)) (阻塞 #4).
 *   - MultipleFaults_HeavyLevel_CollectsAll pins EXPECT_EQ(errorCount,3U)
 *     (阻塞 #5).
 *   - IndexSpecialOffset no longer falls back silently to boundary code
 *     (次要 #6): primary must hit; boundary-fallback moved to its own test.
 *   - Label ↔ injector aligned (次要 #7): IndexMetaPageTypeMismatch.
 *   - HeapTupleOverlap bounds-check the uint16 arithmetic (次要 #8).
 *   - Helpers all from utils.h (次要 #9).
 *   - ThreeScenarios renamed ReadAndFullScenarios (次要 #10).
 *   - Register* moved to fixture SetUp (次要 #11).
 *
 * Layout:
 *   (A) 5 category-representative TEST_P rows (dedup)
 *   (B) 5 orthogonal behavioural TEST_F cases
 *   (C) 5 concurrency TEST cases
 */

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_heap_page.h"
#include "page/dstore_index_page.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;
using DSTORE::ut_dfx::ScopedVerifyConfig;
using DSTORE::ut_dfx::HasVerifyCode;
using DSTORE::ut_dfx::CountVerifyCode;
using DSTORE::ut_dfx::CountSeverity;
using DSTORE::ut_dfx::FaultInjectCase;
using DSTORE::ut_dfx::FaultPageKind;
using DSTORE::ut_dfx::MakeValidHeapPage;
using DSTORE::ut_dfx::AddHeapTuple;
using DSTORE::ut_dfx::MakeValidIndexPage;
using DSTORE::ut_dfx::AddIndexTuple;
using DSTORE::ut_dfx::DEFAULT_VERIFY_MODULES;
using DSTORE::ut_dfx::ALL_VERIFY_MODULES;
using DSTORE::ut_dfx::EnableAllModules;

namespace {

/* Aligned with ut_page_verify_registry.cpp's concurrency knobs. */
constexpr int CONCURRENT_WORKERS = 8;
constexpr int CONCURRENT_LOOPS   = 2000;

/* ========================================================================
 * Corruption injectors
 *   signature  void (*)(char *page, size_t pageSize)
 *   contract   idempotent / deterministic, no global state,
 *              pageSize bounds-checked before any write.
 * ======================================================================== */

void InjectHeapCrc(char *page, size_t pageSize)
{
    ASSERT_GE(pageSize, sizeof(HeapPage));
    HeapPage *hp = reinterpret_cast<HeapPage *>(page);
    AddHeapTuple(hp, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    hp->SetChecksum();
    /* Flip one body byte AFTER SetChecksum — CRC stays stale. */
    page[pageSize / 2] ^= 0x5A;
}

void InjectHeapBoundary(char *page, size_t pageSize)
{
    ASSERT_GE(pageSize, sizeof(HeapPage));
    HeapPage *hp = reinterpret_cast<HeapPage *>(page);
    AddHeapTuple(hp, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    hp->SetLower(static_cast<uint16>(hp->GetUpper() + 100));
    hp->SetChecksum();
}

void InjectHeapTupleOverlap(char *page, size_t pageSize)
{
    ASSERT_GE(pageSize, sizeof(HeapPage));
    HeapPage *hp = reinterpret_cast<HeapPage *>(page);
    AddHeapTuple(hp, FIRST_ITEM_OFFSET_NUMBER, 64, 0);

    /* Reviewer 次要 #8: compute in uint32 and bounds-check before
     * narrowing to uint16, prevents silent wrap-around if callers grow
     * tupleSize or BLCKSZ shrinks. */
    uint32 overlap32 = static_cast<uint32>(hp->GetUpper()) + 32u;
    ASSERT_LT(overlap32, static_cast<uint32>(pageSize))
        << "overlap offset would exceed page, test setup broken";
    uint16 overlapOffset = static_cast<uint16>(overlap32);

    ItemId *itemId2 = hp->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER + 1);
    itemId2->SetNormal(overlapOffset, 32);
    hp->SetLower(static_cast<uint16>(hp->GetLower() + sizeof(ItemId)));

    HeapDiskTuple *tuple2 = reinterpret_cast<HeapDiskTuple *>(page + overlapOffset);
    tuple2->SetTupleSize(32);
    tuple2->SetTdId(0);
    tuple2->SetLockerTdId(INVALID_TD_SLOT);
    tuple2->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    tuple2->SetNumColumn(1);
    hp->SetChecksum();
}

void InjectHeapTdStatusInvalid(char *page, size_t pageSize)
{
    ASSERT_GE(pageSize, sizeof(HeapPage));
    HeapPage *hp = reinterpret_cast<HeapPage *>(page);
    AddHeapTuple(hp, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    HeapDiskTuple *tp = hp->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tp->SetTdStatus(static_cast<TupleTdStatus>(0xFF));
    hp->SetChecksum();
}

void InjectIndexCrc(char *page, size_t pageSize)
{
    ASSERT_GE(pageSize, sizeof(BtrPage));
    BtrPage *bp = reinterpret_cast<BtrPage *>(page);
    bp->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    bp->SetChecksum();
    page[pageSize / 2] ^= 0xA5;
}

/* ========================================================================
 * (A) Parameterized representative set — one row per CorruptionKind.
 * Every row has a concrete primary VerifyCode.  The old "NONE sentinel"
 * weak-contract rows were deleted (reviewer 阻塞 #3).
 * ======================================================================== */

const FaultInjectCase kRepresentativeCases[] = {
    { FaultPageKind::HEAP_KIND,  InjectHeapCrc,             VerifyLevel::LIGHT,
      VerifyCode::PAGE_CRC_MISMATCH,          VerifyCode::OK,
      VerifySeverity::SEVERITY_ERROR,         "HeapCrc" },
    { FaultPageKind::HEAP_KIND,  InjectHeapBoundary,        VerifyLevel::LIGHT,
      VerifyCode::PAGE_BOUNDARY_INVALID,      VerifyCode::OK,
      VerifySeverity::SEVERITY_ERROR,         "HeapBoundary" },
    { FaultPageKind::HEAP_KIND,  InjectHeapTupleOverlap,    VerifyLevel::HEAVY,
      VerifyCode::HEAP_TUPLE_OVERLAP,         VerifyCode::OK,
      VerifySeverity::SEVERITY_ERROR,         "HeapTupleOverlap" },
    { FaultPageKind::HEAP_KIND,  InjectHeapTdStatusInvalid, VerifyLevel::HEAVY,
      VerifyCode::HEAP_TD_SANITY_FAIL,        VerifyCode::OK,
      VerifySeverity::SEVERITY_ERROR,         "HeapTdStatusInvalid" },
    { FaultPageKind::INDEX_KIND, InjectIndexCrc,            VerifyLevel::LIGHT,
      VerifyCode::PAGE_CRC_MISMATCH,          VerifyCode::OK,
      VerifySeverity::SEVERITY_ERROR,         "IndexCrc" },
};

void *BuildAndCorrupt(PageBuffer &buf, const FaultInjectCase &c)
{
    void *page = nullptr;
    if (c.pageKind == FaultPageKind::HEAP_KIND) {
        page = MakeValidHeapPage(buf, {100, 1});
    } else {
        page = MakeValidIndexPage(buf, {100, 1});
    }
    c.injector(reinterpret_cast<char *>(buf.data()), buf.size());
    return page;
}

class UTFaultInjectVerifyP : public ::testing::TestWithParam<FaultInjectCase> {
protected:
    void SetUp() override
    {
        RegisterHeapPageVerifier();
        RegisterIndexPageVerifier();
        EnableAllModules();
    }

    /* Reviewer 阻塞 #2: RAII GUC guard covers every parameterized case. */
    ScopedVerifyConfig m_guard{};
};

TEST_P(UTFaultInjectVerifyP, CorruptionDetectedWithCodeAndSeverity)
{
    const FaultInjectCase c = GetParam();
    PageBuffer buf{};
    void *page = BuildAndCorrupt(buf, c);
    ASSERT_NE(page, nullptr) << c.label;

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, c.triggerLevel, &report), DSTORE_FAIL) << c.label;

    /* Primary code MUST fire.  altCode only used for cases with legitimate
     * disjunction (currently none in kRepresentativeCases; kept for future
     * additions).  Reviewer 次要 #6: do not allow silent fallback — if an
     * altCode is set, a dedicated test covers the fallback path. */
    EXPECT_TRUE(HasVerifyCode(report, c.expectedCode))
        << c.label
        << " primary=0x" << std::hex << static_cast<uint32>(c.expectedCode);

    /* Severity pinning: the matching result must fire at the expected
     * severity, not downgraded to INFO. */
    for (const auto &r : report.GetResults()) {
        if (r.code == c.expectedCode) {
            EXPECT_EQ(r.severity, c.expectedSeverity) << c.label;
        }
    }
    EXPECT_GE(report.GetErrorCount() + report.GetFatalCount(), 1u) << c.label;
}

static std::string FaultCaseLabel(const ::testing::TestParamInfo<FaultInjectCase> &info)
{
    return info.param.label;
}

INSTANTIATE_TEST_SUITE_P(
    AllKinds,
    UTFaultInjectVerifyP,
    ::testing::ValuesIn(kRepresentativeCases),
    FaultCaseLabel);

/* ========================================================================
 * (B) Orthogonal behavioural tests — TEST_F on a shared fixture so GUC
 * RAII and Register* apply once per case (reviewer 阻塞 #2, 次要 #11).
 * ======================================================================== */

class UTFaultInjectVerify : public ::testing::Test {
protected:
    void SetUp() override
    {
        RegisterHeapPageVerifier();
        RegisterIndexPageVerifier();
        EnableAllModules();
    }

    ScopedVerifyConfig m_guard{};
};

TEST_F(UTFaultInjectVerify, ReadAndFullScenarios_SameCorruption)
{
    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {112, 1});
    InjectHeapBoundary(reinterpret_cast<char *>(buf.data()), buf.size());

    /* OnWrite triggers PANIC on failure — not exercised here (注释对齐
     * 次要 #10; name no longer promises three scenarios). */

    /* Read path → FAIL + boundary code at ERROR severity. */
    VerifyReport rRead;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &rRead), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(rRead, VerifyCode::PAGE_BOUNDARY_INVALID));
    EXPECT_EQ(CountSeverity(rRead, VerifySeverity::SEVERITY_ERROR), 1u);

    /* Full-scan path → SUCC (collect-only), report still carries the error. */
    VerifyReport rFull;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &rFull), DSTORE_SUCC);
    EXPECT_TRUE(HasVerifyCode(rFull, VerifyCode::PAGE_BOUNDARY_INVALID));
    EXPECT_EQ(CountSeverity(rFull, VerifySeverity::SEVERITY_ERROR), 1u);
}

TEST_F(UTFaultInjectVerify, MultipleFaults_FirstCaught_EarlyReturnPinned)
{
    /* Reviewer 阻塞 #4: early-return contract requires EXACTLY one error
     * (TD count), not "at least one".  The FSM fault MUST NOT leak into
     * the report or the test silently tolerates regression. */
    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {113, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);

    page->dataHeader.tdCount = 200;                     /* fault 1 */
    page->SetFsmIndex({INVALID_PAGE_ID, FSM_MAX_HWM});  /* fault 2 */
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TD_COUNT_OVERFLOW));
    EXPECT_FALSE(HasVerifyCode(report, VerifyCode::HEAP_FSM_SLOT_INVALID));
    EXPECT_EQ(report.GetErrorCount(), 1u);
}

TEST_F(UTFaultInjectVerify, MultipleFaults_HeavyLevel_CollectsExactlyThree)
{
    /* Reviewer 阻塞 #5: collect-all must pin EXPECT_EQ(errorCount, 3U);
     * a >= check lets the verifier silently double-report or over-report. */
    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {200, 300});

    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER,     32, 0);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER + 1, 32, 0);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER + 2, 32, 0);

    page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER)
        ->SetTdStatus(static_cast<TupleTdStatus>(0xFF));            /* fault 1 */
    page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER + 1)->SetNumColumn(0); /* fault 2 */
    page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER + 2)
        ->SetLockerTdId(static_cast<uint8>(page->GetTdCount() + 10)); /* fault 3 */
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetErrorCount(), 3u);
    /* Every error must fire at ERROR severity (no INFO-demotion). */
    EXPECT_EQ(CountSeverity(report, VerifySeverity::SEVERITY_ERROR), 3u);
}

TEST_F(UTFaultInjectVerify, NoneLevel_BypassesAllFaults)
{
    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {115, 1});
    InjectHeapCrc(reinterpret_cast<char *>(buf.data()), buf.size());
    /* Compound corruption: CRC flipped + TD count overflow. */
    page->dataHeader.tdCount = 200;

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::NONE, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
    EXPECT_EQ(report.GetTotalChecks(), 0u);
}

TEST_F(UTFaultInjectVerify, IndexSpecialOffset_PrimaryCodeMustHit)
{
    /* Reviewer 次要 #6: the primary BTR_SPECIAL_OFFSET_INVALID must fire.
     * Silent fallback to PAGE_BOUNDARY_INVALID is not acceptable because
     * it erases the verifier's diagnostic specificity. */
    PageBuffer buf{};
    BtrPage *page = MakeValidIndexPage(buf, {108, 1});
    /* 8-byte misalignment — on the well-aligned special region this
     * triggers BTR_SPECIAL_OFFSET_INVALID, not the generic boundary. */
    page->SetSpecialOffset(static_cast<uint16>(page->GetSpecialOffset() - 8));
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::BTR_SPECIAL_OFFSET_INVALID))
        << "primary code missing; fallback to PAGE_BOUNDARY_INVALID is not "
           "acceptable for the well-aligned 8-byte corruption case";
}

TEST_F(UTFaultInjectVerify, IndexMetaPageTypeMismatch)
{
    /* Reviewer 次要 #7: label renamed to match injector behaviour —
     * the mutation is the page type, not TD count. */
    PageBuffer buf{};
    BtrPage *page = MakeValidIndexPage(buf, {111, 1});
    page->GetLinkAndStatus()->SetType(BtrPageType::META_PAGE);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_GE(report.GetErrorCount(), 1u);
    /* TODO(DFX-followup): meta-page/type-mismatch currently lacks a
     * dedicated VerifyCode.  Once added, tighten this to
     * EXPECT_TRUE(HasVerifyCode(report, VerifyCode::BTR_META_PAGE_ID_INVALID))
     * — tracked in ut_dfx/ut_dfx_test_utils.h FaultInjectCase.altCode doc. */
}

TEST_F(UTFaultInjectVerify, ModuleFilter_WhenHeapBitCleared_SkipsHeapVerify)
{
    /* HEAP bit (0x01) off → Heap checks must be fully skipped. */
    SetDfxVerifyModules(DEFAULT_VERIFY_MODULES & ~0x01ULL);

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {116, 1});
    InjectHeapCrc(reinterpret_cast<char *>(buf.data()), buf.size());

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
    EXPECT_EQ(report.GetErrorCount(), 0u);
}

/* ========================================================================
 * (C) Concurrency tests — race detectors, two-valued stable-state checks.
 * Each launches ≥4 workers with a ready-count barrier so the race window
 * is entered together.  Assertions are clock/schedule-independent:
 * - no sleeps
 * - no ordering expectations
 * - outcomes partitioned into "allowed stable states" and "forbidden
 *   third state" — only the third-state count is hard-zeroed.
 * ======================================================================== */

TEST(UTFaultInjectVerifyConcurrency, Concurrent_SameCorruption_ReportsStableCode)
{
    RegisterHeapPageVerifier();
    ScopedVerifyConfig guard;
    SetDfxVerifyModules(DEFAULT_VERIFY_MODULES);

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {300, 1});
    InjectHeapCrc(reinterpret_cast<char *>(buf.data()), buf.size());

    std::atomic<int> ready{0};
    std::atomic<int> stableHits{0};
    std::atomic<int> otherCode{0};

    std::vector<std::thread> ts;
    for (int i = 0; i < CONCURRENT_WORKERS; ++i) {
        ts.emplace_back([&] {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (ready.load(std::memory_order_acquire) < CONCURRENT_WORKERS) {
                std::this_thread::yield();
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int k = 0; k < CONCURRENT_LOOPS; ++k) {
                VerifyReport r;
                RetStatus ret = VerifyPage(page, VerifyLevel::LIGHT, &r);
                if (ret == DSTORE_FAIL &&
                    HasVerifyCode(r, VerifyCode::PAGE_CRC_MISMATCH)) {
                    stableHits.fetch_add(1, std::memory_order_relaxed);
                } else {
                    otherCode.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto &t : ts) t.join();

    /* No "third state": every iteration must report the same code. */
    EXPECT_EQ(otherCode.load(), 0);
    EXPECT_EQ(stableHits.load(), CONCURRENT_WORKERS * CONCURRENT_LOOPS);
}

TEST(UTFaultInjectVerifyConcurrency, Concurrent_GucLevelToggle_ReportsTwoValued)
{
    RegisterHeapPageVerifier();
    ScopedVerifyConfig guard;
    SetDfxVerifyModules(DEFAULT_VERIFY_MODULES);

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {301, 1});
    InjectHeapCrc(reinterpret_cast<char *>(buf.data()), buf.size());

    std::atomic<bool> stop{false};
    std::atomic<int> passCount{0};
    std::atomic<int> failCount{0};
    std::atomic<int> inconsistent{0};

    std::thread toggler([&] {
        const VerifyLevel seq[] = {VerifyLevel::NONE, VerifyLevel::LIGHT, VerifyLevel::HEAVY};
        int i = 0;
        while (!stop.load(std::memory_order_acquire)) {
            SetDfxVerifyLevel(seq[i++ % 3]);
            std::this_thread::yield();
        }
    });

    std::vector<std::thread> ws;
    for (int i = 0; i < CONCURRENT_WORKERS; ++i) {
        ws.emplace_back([&] {
            for (int k = 0; k < CONCURRENT_LOOPS; ++k) {
                VerifyLevel lv = GetDfxVerifyLevel();
                VerifyReport r;
                RetStatus ret = VerifyPage(page, lv, &r);
                /* Contract: NONE ⇒ SUCC∧!HasError; else FAIL∧HasError. */
                if (lv == VerifyLevel::NONE) {
                    if (ret == DSTORE_SUCC && !r.HasError()) {
                        passCount.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        inconsistent.fetch_add(1, std::memory_order_relaxed);
                    }
                } else {
                    if (ret == DSTORE_FAIL && r.HasError()) {
                        failCount.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        inconsistent.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });
    }
    for (auto &t : ws) t.join();
    stop.store(true, std::memory_order_release);
    toggler.join();

    EXPECT_EQ(inconsistent.load(), 0);
    /* Both sides observed — otherwise test degenerates. */
    EXPECT_GT(passCount.load(), 0);
    EXPECT_GT(failCount.load(), 0);
}

TEST(UTFaultInjectVerifyConcurrency, Concurrent_MixedPages_ReportsAreIsolated)
{
    RegisterHeapPageVerifier();
    ScopedVerifyConfig guard;
    SetDfxVerifyModules(DEFAULT_VERIFY_MODULES);

    PageBuffer cleanBuf{};
    PageBuffer badBuf{};
    HeapPage *clean = MakeValidHeapPage(cleanBuf, {302, 1});
    HeapPage *bad   = MakeValidHeapPage(badBuf,   {302, 2});
    InjectHeapCrc(reinterpret_cast<char *>(badBuf.data()), badBuf.size());

    std::atomic<int> cleanFalsePositive{0};
    std::atomic<int> badFalseNegative{0};

    std::vector<std::thread> ts;
    for (int i = 0; i < CONCURRENT_WORKERS; ++i) {
        ts.emplace_back([&] {
            for (int k = 0; k < CONCURRENT_LOOPS; ++k) {
                VerifyReport rC;
                VerifyReport rB;
                RetStatus retC = VerifyPage(clean, VerifyLevel::LIGHT, &rC);
                RetStatus retB = VerifyPage(bad,   VerifyLevel::LIGHT, &rB);
                if (retC != DSTORE_SUCC || rC.HasError()) {
                    cleanFalsePositive.fetch_add(1, std::memory_order_relaxed);
                }
                if (retB != DSTORE_FAIL ||
                    !HasVerifyCode(rB, VerifyCode::PAGE_CRC_MISMATCH)) {
                    badFalseNegative.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto &t : ts) t.join();

    EXPECT_EQ(cleanFalsePositive.load(), 0);
    EXPECT_EQ(badFalseNegative.load(), 0);
}

TEST(UTFaultInjectVerifyConcurrency, Concurrent_DynamicCorruption_PairedFlipStaysTwoValued)
{
    /* Reviewer 阻塞 #1 #2: VerifyReentrancy — Verify walks the page
     * while a mutator flips a body byte; results must collapse to the
     * two allowed stable states (clean or CRC-mismatch).  The verifier
     * must NOT crash or produce a third code. */
    RegisterHeapPageVerifier();
    ScopedVerifyConfig guard;
    SetDfxVerifyModules(DEFAULT_VERIFY_MODULES);
    SetDfxVerifyLevel(VerifyLevel::LIGHT);

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {303, 1});
    volatile char *body = reinterpret_cast<volatile char *>(buf.data()) + BLCKSZ / 2;

    std::atomic<bool> stop{false};
    std::atomic<int> cleanObserved{0};
    std::atomic<int> dirtyObserved{0};
    std::atomic<int> other{0};

    std::thread mutator([&] {
        while (!stop.load(std::memory_order_acquire)) {
            /* Paired XOR: page converges back to valid between flips. */
            *body ^= 0x5A;
            std::this_thread::yield();
            *body ^= 0x5A;
            std::this_thread::yield();
        }
    });

    std::vector<std::thread> ws;
    for (int i = 0; i < CONCURRENT_WORKERS; ++i) {
        ws.emplace_back([&] {
            for (int k = 0; k < CONCURRENT_LOOPS; ++k) {
                VerifyReport r;
                RetStatus ret = VerifyPage(page, VerifyLevel::LIGHT, &r);
                if (ret == DSTORE_SUCC && !r.HasError()) {
                    cleanObserved.fetch_add(1, std::memory_order_relaxed);
                } else if (ret == DSTORE_FAIL &&
                           HasVerifyCode(r, VerifyCode::PAGE_CRC_MISMATCH)) {
                    dirtyObserved.fetch_add(1, std::memory_order_relaxed);
                } else {
                    other.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto &t : ws) t.join();
    stop.store(true, std::memory_order_release);
    mutator.join();

    EXPECT_EQ(other.load(), 0);
    EXPECT_GT(cleanObserved.load(), 0);
    EXPECT_GT(dirtyObserved.load(), 0);
}

/* Relocated here per team-lead B 裁决 (2026-04-15): fault_inject is the
 * natural home for level-mixing races since this file already toggles
 * level & module GUCs; ut_heap_page_verify keeps its 3 other concurrency
 * tests (ItemId race / GUC module / multi-tuple HEAVY). */
TEST(UTFaultInjectVerifyConcurrency, Concurrent_LightAndHeavy_SamePageConsistentCode)
{
    RegisterHeapPageVerifier();
    ScopedVerifyConfig guard;
    SetDfxVerifyModules(DEFAULT_VERIFY_MODULES);

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {304, 1});
    InjectHeapCrc(reinterpret_cast<char *>(buf.data()), buf.size());

    std::atomic<int> lightHits{0};
    std::atomic<int> heavyHits{0};
    std::atomic<int> anyMismatch{0};

    std::vector<std::thread> ts;
    for (int i = 0; i < CONCURRENT_WORKERS; ++i) {
        VerifyLevel lv = (i % 2 == 0) ? VerifyLevel::LIGHT : VerifyLevel::HEAVY;
        ts.emplace_back([&, lv] {
            for (int k = 0; k < CONCURRENT_LOOPS; ++k) {
                VerifyReport r;
                RetStatus ret = VerifyPage(page, lv, &r);
                if (ret != DSTORE_FAIL ||
                    !HasVerifyCode(r, VerifyCode::PAGE_CRC_MISMATCH)) {
                    anyMismatch.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                if (lv == VerifyLevel::LIGHT) {
                    lightHits.fetch_add(1, std::memory_order_relaxed);
                } else {
                    heavyHits.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto &t : ts) t.join();

    EXPECT_EQ(anyMismatch.load(), 0);
    EXPECT_GT(lightHits.load(), 0);
    EXPECT_GT(heavyHits.load(), 0);
}

}  /* anonymous namespace */

/*
 * Unit tests for Undo page verification (UndoRecordPage + TransactionSlotPage).
 *
 * v2 delta vs v1:
 *   - Param-merged 6 TEST_P families: ValidUndoRecordPageAllLevels(2),
 *     ValidTxnSlotPageAllLevels(2), UndoRecordSizeCorruption(3),
 *     TxnSlotFinalStatusWithInvalidCsn(3), UndoRecordPageLinkSelfReference(2),
 *     TxnSlotPageHeaderBoundaryCorruption(2)
 *   - Added 2 concurrency TEST (Undo-specific: TxnSlot status race,
 *     UndoRecordPage read-only control)
 *   - All tests use ScopedVerifyConfig RAII and EnableAllModules
 *   - Pure deletions: 0 — all redundant cases absorbed into TEST_P families
 *
 * Arithmetic breakdown (four-way independent, self-consistent):
 *   original TEST_F  = 22
 *   merge absorbed   = -14 (= 2+2+3+3+2+2)
 *   new TEST_P       = +6
 *   pure deletions   = -0
 *   new concurrency  = +2
 *   final TEST total = 16 (= 8 TEST_F + 6 TEST_P + 2 concurrency)
 *
 * === Absorbed cases (all via TEST_P, no pure deletions) ===
 *   ValidUndoRecordPagePassesLight + ValidUndoRecordWithRecordsPassesHeavy
 *     → ValidUndoRecordPageAllLevels (2 rows: empty-light / withrecords-heavy)
 *   ValidTxnSlotPagePassesLight + ValidTxnSlotPagePassesHeavy
 *     → ValidTxnSlotPageAllLevels (2 rows: light / heavy)
 *   UndoRecordSizeZeroFails + UndoRecordSizeTooSmallFails +
 *     UndoRecordSizeCrossesLimitFails
 *     → UndoRecordSizeCorruption (3 rows: size0 / size1 / claim200-actual50)
 *   TxnSlotCommittedWithInvalidCsnFails + TxnSlotAbortedWithInvalidCsnFails +
 *     TxnSlotPreparedWithInvalidCsnFails
 *     → TxnSlotFinalStatusWithInvalidCsn (3 rows: committed / aborted / prepared)
 *   UndoRecordPageSelfReferenceFails + UndoRecordPageNextSelfReferenceFails
 *     → UndoRecordPageLinkSelfReference (2 rows: prev / next)
 *   TxnSlotPageLowerMismatchFails + TxnSlotPageUpperMismatchFails
 *     → TxnSlotPageHeaderBoundaryCorruption (2 rows: lower / upper)
 */
#include <atomic>
#include <cstring>
#include <thread>
#include <vector>
#include <gtest/gtest.h>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_undo_page.h"
#include "undo/dstore_undo_record.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;
using DSTORE::ut_dfx::HasVerifyCode;
using DSTORE::ut_dfx::ScopedVerifyConfig;

namespace {

constexpr int CONCURRENT_WORKERS = 8;
constexpr int CONCURRENT_LOOPS = 2000;

/* Initialize a valid UndoRecordPage (kept local — undo-specific, not
 * shared with heap/index callers so no reason to hoist into utils.h). */
UndoRecordPage *InitUndoRecordPage(PageBuffer &buffer, PageId pageId)
{
    UndoRecordPage *page = reinterpret_cast<UndoRecordPage *>(buffer.data());
    page->m_undoRecPageHeader = {0, pageId, INVALID_PAGE_ID, INVALID_PAGE_ID};
    page->InitUndoRecPage(pageId);
    page->SetLsn(1, 1, 1, false);
    /* Set lower to data area start; records are written from this offset onwards. */
    page->m_header.m_lower = UNDO_RECORD_PAGE_HEADER_SIZE;
    page->m_header.m_upper = BLCKSZ;
    page->SetChecksum();
    return page;
}

/* Write a fake undo record into an UndoRecordPage.
 * Layout: [serializeSize:1B][undoType:1B][padding...] */
void AddFakeUndoRecord(UndoRecordPage *page, uint8 serializeSize, UndoType undoType)
{
    char *cursor = reinterpret_cast<char *>(page) + page->GetLower();
    *reinterpret_cast<uint8 *>(cursor) = serializeSize;
    memcpy(cursor + sizeof(uint8), &undoType, sizeof(UndoType));
    page->m_header.m_lower = static_cast<uint16>(page->GetLower() + serializeSize);
}

/* Initialize a valid TransactionSlotPage. */
TransactionSlotPage *InitTransactionSlotPage(PageBuffer &buffer, PageId pageId)
{
    TransactionSlotPage *page = reinterpret_cast<TransactionSlotPage *>(buffer.data());
    page->InitTxnSlotPage(pageId);
    page->SetLsn(1, 1, 1, false);
    page->m_header.m_lower = TRX_PAGE_HEADER_SIZE;
    page->m_header.m_upper = BLCKSZ;
    for (int i = 0; i < TRX_PAGE_SLOTS_NUM; ++i) {
        page->m_slots[i].SetTrxSlotStatus(TXN_STATUS_FROZEN);
    }
    page->SetChecksum();
    return page;
}

}  /* anonymous namespace */

class UTUndoPageVerify : public ::testing::Test {
protected:
    void SetUp() override
    {
        RegisterUndoPageVerifiers();
    }
    PageBuffer pageBuffer{};
    VerifyReport report;
};

/* ========== TEST_P: Valid UndoRecordPage across levels ========== */

struct ValidUndoRecordCase {
    VerifyLevel level;
    bool withRecords;  /* if true, add 2 UNDO_HEAP_INSERT records */
    const char *label;
};

class UTUndoRecordPageValid : public ::testing::TestWithParam<ValidUndoRecordCase> {
protected:
    void SetUp() override { RegisterUndoPageVerifiers(); }
    PageBuffer pageBuffer{};
    VerifyReport report;
};

TEST_P(UTUndoRecordPageValid, ValidUndoRecordPageAllLevels)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    const auto &p = GetParam();
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 100});
    if (p.withRecords) {
        AddFakeUndoRecord(page, 16, UNDO_HEAP_INSERT);
        AddFakeUndoRecord(page, 16, UNDO_HEAP_INSERT);
    }
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, p.level, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

INSTANTIATE_TEST_SUITE_P(Levels, UTUndoRecordPageValid,
    ::testing::Values(
        ValidUndoRecordCase{VerifyLevel::LIGHT, false, "light_empty"},
        ValidUndoRecordCase{VerifyLevel::HEAVY, true, "heavy_with_records"}),
    [](const ::testing::TestParamInfo<ValidUndoRecordCase> &info) {
        return info.param.label;
    });

/* ========== TEST_P: Valid TxnSlotPage across levels ========== */

struct ValidTxnSlotCase {
    VerifyLevel level;
    const char *label;
};

class UTTxnSlotPageValid : public ::testing::TestWithParam<ValidTxnSlotCase> {
protected:
    void SetUp() override { RegisterUndoPageVerifiers(); }
    PageBuffer pageBuffer{};
    VerifyReport report;
};

TEST_P(UTTxnSlotPageValid, ValidTxnSlotPageAllLevels)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    const auto &p = GetParam();
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 300});

    EXPECT_EQ(VerifyPage(page, p.level, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

INSTANTIATE_TEST_SUITE_P(Levels, UTTxnSlotPageValid,
    ::testing::Values(
        ValidTxnSlotCase{VerifyLevel::LIGHT, "light"},
        ValidTxnSlotCase{VerifyLevel::HEAVY, "heavy"}),
    [](const ::testing::TestParamInfo<ValidTxnSlotCase> &info) {
        return info.param.label;
    });

/* ========== TEST_P: UndoRecord size-field corruption (all → PAGE_BOUNDARY_INVALID) ========== */

struct UndoRecordSizeCase {
    uint8 claimedSize;   /* serializeSize byte written to the record */
    uint16 advanceBy;    /* how many bytes to advance `lower` (simulating actual data) */
    const char *label;
};

class UTUndoRecordSize : public ::testing::TestWithParam<UndoRecordSizeCase> {
protected:
    void SetUp() override { RegisterUndoPageVerifiers(); }
    PageBuffer pageBuffer{};
    VerifyReport report;
};

TEST_P(UTUndoRecordSize, UndoRecordSizeCorruption)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    const auto &p = GetParam();
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 201});

    char *cursor = reinterpret_cast<char *>(page) + page->GetLower();
    *reinterpret_cast<uint8 *>(cursor) = p.claimedSize;
    page->m_header.m_lower = static_cast<uint16>(page->GetLower() + p.advanceBy);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

INSTANTIATE_TEST_SUITE_P(Corruptions, UTUndoRecordSize,
    ::testing::Values(
        UndoRecordSizeCase{0, 1, "size_zero"},
        UndoRecordSizeCase{1, 1, "size_too_small"},
        UndoRecordSizeCase{200, 50, "size_claim_exceeds_actual"}),
    [](const ::testing::TestParamInfo<UndoRecordSizeCase> &info) {
        return info.param.label;
    });

/* ========== TEST_P: TxnSlot final-status + INVALID_CSN (all → UNDO_SLOT_XID_INVALID) ========== */

struct TxnSlotFinalStatusCase {
    TrxSlotStatus status;
    const char *label;
};

class UTTxnSlotFinalStatusInvalidCsn : public ::testing::TestWithParam<TxnSlotFinalStatusCase> {
protected:
    void SetUp() override { RegisterUndoPageVerifiers(); }
    PageBuffer pageBuffer{};
    VerifyReport report;
};

TEST_P(UTTxnSlotFinalStatusInvalidCsn, TxnSlotFinalStatusWithInvalidCsn)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    const auto &p = GetParam();
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 402});
    page->m_slots[0].SetTrxSlotStatus(p.status);
    page->m_slots[0].SetCsn(INVALID_CSN);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_SLOT_XID_INVALID));
}

INSTANTIATE_TEST_SUITE_P(Statuses, UTTxnSlotFinalStatusInvalidCsn,
    ::testing::Values(
        TxnSlotFinalStatusCase{TXN_STATUS_COMMITTED, "committed"},
        TxnSlotFinalStatusCase{TXN_STATUS_ABORTED, "aborted"},
        TxnSlotFinalStatusCase{TXN_STATUS_PREPARED, "prepared"}),
    [](const ::testing::TestParamInfo<TxnSlotFinalStatusCase> &info) {
        return info.param.label;
    });

/* ========== TEST_P: UndoRecordPage link self-reference (prev / next → PAGE_ID_INVALID) ========== */

enum class UndoLinkField { PREV, NEXT };

struct UndoRecordSelfRefCase {
    UndoLinkField field;
    const char *label;
};

class UTUndoRecordSelfRef : public ::testing::TestWithParam<UndoRecordSelfRefCase> {
protected:
    void SetUp() override { RegisterUndoPageVerifiers(); }
    PageBuffer pageBuffer{};
    VerifyReport report;
};

TEST_P(UTUndoRecordSelfRef, UndoRecordPageLinkSelfReference)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    const auto &p = GetParam();
    PageId selfId = {10, 206};
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, selfId);
    if (p.field == UndoLinkField::PREV) {
        page->m_undoRecPageHeader.prev = selfId;
    } else {
        page->m_undoRecPageHeader.next = selfId;
    }
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_ID_INVALID));
}

INSTANTIATE_TEST_SUITE_P(Links, UTUndoRecordSelfRef,
    ::testing::Values(
        UndoRecordSelfRefCase{UndoLinkField::PREV, "prev_self_ref"},
        UndoRecordSelfRefCase{UndoLinkField::NEXT, "next_self_ref"}),
    [](const ::testing::TestParamInfo<UndoRecordSelfRefCase> &info) {
        return info.param.label;
    });

/* ========== TEST_P: TxnSlotPage header lower/upper corruption ========== */

enum class TxnSlotBoundaryField { LOWER, UPPER };

struct TxnSlotBoundaryCase {
    TxnSlotBoundaryField field;
    const char *label;
};

class UTTxnSlotBoundary : public ::testing::TestWithParam<TxnSlotBoundaryCase> {
protected:
    void SetUp() override { RegisterUndoPageVerifiers(); }
    PageBuffer pageBuffer{};
    VerifyReport report;
};

TEST_P(UTTxnSlotBoundary, TxnSlotPageHeaderBoundaryCorruption)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    const auto &p = GetParam();
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 301});
    if (p.field == TxnSlotBoundaryField::LOWER) {
        page->m_header.m_lower = TRX_PAGE_HEADER_SIZE + 1;
    } else {
        page->m_header.m_upper = BLCKSZ - 1;
    }
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

INSTANTIATE_TEST_SUITE_P(Boundaries, UTTxnSlotBoundary,
    ::testing::Values(
        TxnSlotBoundaryCase{TxnSlotBoundaryField::LOWER, "lower_mismatch"},
        TxnSlotBoundaryCase{TxnSlotBoundaryField::UPPER, "upper_mismatch"}),
    [](const ::testing::TestParamInfo<TxnSlotBoundaryCase> &info) {
        return info.param.label;
    });

/* ========== Remaining single-shot TEST_F (each exercises a distinct field) ========== */

TEST_F(UTUndoPageVerify, UndoRecordPageLowerBelowHeaderFails)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 101});
    page->m_header.m_lower = static_cast<uint16>(sizeof(Page)) - 1;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

TEST_F(UTUndoPageVerify, UndoRecordPageLowerAboveUpperFails)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 102});
    page->m_header.m_lower = BLCKSZ;
    page->m_header.m_upper = UNDO_RECORD_PAGE_HEADER_SIZE;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTUndoPageVerify, UndoRecordPageCurMismatchFails)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 103});
    page->m_undoRecPageHeader.cur = {10, 999};
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_ID_MISMATCH));
}

TEST_F(UTUndoPageVerify, UndoRecordPageCurInvalidSkipped)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 104});
    page->m_undoRecPageHeader.cur = INVALID_PAGE_ID;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTUndoPageVerify, UndoRecordTypeInvalidFails)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 204});
    AddFakeUndoRecord(page, 16, UNDO_UNKNOWN);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_REC_TYPE_INVALID));
}

TEST_F(UTUndoPageVerify, TxnSlotStatusInvalidFails)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 401});
    page->m_slots[0].SetTrxSlotStatus(static_cast<TrxSlotStatus>(0xFF));
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_SLOT_STATE_INVALID));
}

TEST_F(UTUndoPageVerify, TxnSlotNextLogicSlotExceedsRangeFails)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    PageId selfId = {10, 403};
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, selfId);
    uint64 overLimit = static_cast<uint64>(TRX_PAGE_SLOTS_NUM) +
        static_cast<uint64>(page->GetBlockNum()) * static_cast<uint64>(TRX_PAGE_SLOTS_NUM) + 1;
    page->SetNextFreeLogicSlotId(overLimit);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_SLOT_STATE_INVALID));
}

TEST_F(UTUndoPageVerify, MultipleSlotCorruptionReportsFirst)
{
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();

    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 406});
    page->m_slots[0].SetTrxSlotStatus(static_cast<TrxSlotStatus>(0xFE));
    page->m_slots[1].SetTrxSlotStatus(static_cast<TrxSlotStatus>(0xFF));
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_SLOT_STATE_INVALID));
}

/* ========== Concurrency tests (Undo-specific) ==========
 *
 * Same Y scheme as heap_page / index_page: 1 writer flips a field, 7
 * readers run verifier in parallel, bucketed counters with torn-read
 * guardrail (forbiddenCount must stay 0).
 *
 * No independent mutator/stopper threads — writer runs fixed
 * CONCURRENT_LOOPS then joins naturally, so C2 ordering rule does
 * not apply.
 */

TEST(UTUndoPageVerifyConcurrency, Concurrent_TxnSlotStatusFlip_ReportsBoundedCodes)
{
    /* Writer flips slot[0].status between TXN_STATUS_FROZEN (valid) and
     * 0xFF (invalid → UNDO_SLOT_STATE_INVALID).  7 readers run HEAVY.
     *
     * CRC note: TransactionSlotPage has no special-region carve-out —
     * m_slots[] lies in the CRC-covered body (unlike BtrPage's
     * LinkAndStatus).  Writer MUST re-SetChecksum after each flip or
     * every reader would trip PAGE_CRC_MISMATCH before reaching the
     * slot-state check.  SetChecksum itself is non-atomic against
     * CheckPageCrcMatch, so torn PAGE_CRC_MISMATCH reads are admitted
     * into the bounded set.
     *
     * Bounded set: { OK, UNDO_SLOT_STATE_INVALID, PAGE_CRC_MISMATCH }.
     * Any other code is forbidden.  Strong-signal assertion requires
     * the slot-state path (OK or UNDO_SLOT_STATE_INVALID) to fire at
     * least once, so the test cannot degenerate into a pure CRC race. */
    RegisterUndoPageVerifiers();
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();
    SetDfxVerifyLevel(VerifyLevel::HEAVY);

    PageBuffer buf{};
    TransactionSlotPage *page = InitTransactionSlotPage(buf, {10, 500});

    std::atomic<int> ready{0};
    std::atomic<uint64> okCount{0};
    std::atomic<uint64> slotStateFailCount{0};
    std::atomic<uint64> crcTornCount{0};
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
                    page->m_slots[0].SetTrxSlotStatus(
                        (i & 1) ? static_cast<TrxSlotStatus>(0xFF) : TXN_STATUS_FROZEN);
                    page->SetChecksum();
                }
            } else {
                for (int i = 0; i < CONCURRENT_LOOPS; ++i) {
                    VerifyReport r;
                    (void)VerifyPage(page, VerifyLevel::HEAVY, &r);
                    if (!r.HasError()) {
                        okCount.fetch_add(1, std::memory_order_relaxed);
                    } else if (HasVerifyCode(r, VerifyCode::UNDO_SLOT_STATE_INVALID)) {
                        slotStateFailCount.fetch_add(1, std::memory_order_relaxed);
                    } else if (HasVerifyCode(r, VerifyCode::PAGE_CRC_MISMATCH)) {
                        crcTornCount.fetch_add(1, std::memory_order_relaxed);
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
        << "verifier reported unrelated codes during TxnSlot status race";
    EXPECT_EQ(fatalCount.load(), 0u)
        << "verifier escalated to FATAL during TxnSlot status race";
    EXPECT_GT(okCount.load() + slotStateFailCount.load(), 0u)
        << "slot-state path never observed — test degenerated into CRC-only race";
}

TEST(UTUndoPageVerifyConcurrency, Concurrent_UndoRecordPageReadOnly_NoCrossTalk)
{
    /* Control test: 8 readers run HEAVY verify against a fully-valid
     * UndoRecordPage with 2 records; no writer.  Verifies that
     * concurrent read-only verify produces zero errors (no false
     * positives from shared verifier scratch / registry state). */
    RegisterUndoPageVerifiers();
    ScopedVerifyConfig guard;
    DSTORE::ut_dfx::EnableAllModules();
    SetDfxVerifyLevel(VerifyLevel::HEAVY);

    PageBuffer buf{};
    UndoRecordPage *page = InitUndoRecordPage(buf, {10, 501});
    AddFakeUndoRecord(page, 16, UNDO_HEAP_INSERT);
    AddFakeUndoRecord(page, 16, UNDO_HEAP_INSERT);
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
        << "concurrent read-only HEAVY verify on valid UndoRecordPage should produce zero errors";
}

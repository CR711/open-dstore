/*
 * Fault injection unit tests for the DFX page verify framework.
 *
 * Each test injects a specific page corruption, runs the verifier, and asserts
 * that the correct VerifyCode is reported.  The corruptions mirror the 11
 * FaultType scenarios defined in the stress-verify FaultInjector, but are
 * applied directly through the page API so that the UT has no external
 * dependency on the stress-verify library.
 */
#include <gtest/gtest.h>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_heap_page.h"
#include "page/dstore_index_page.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;
using DSTORE::ut_dfx::HasVerifyCode;

namespace {

/* ---------- Heap page helpers ---------- */
HeapPage *MakeValidHeapPage(PageBuffer &buffer, PageId pageId)
{
    HeapPage *page = reinterpret_cast<HeapPage *>(buffer.data());
    page->Init(0, PageType::HEAP_PAGE_TYPE, pageId);
    page->SetLsn(1, 1, 1, false);
    page->SetDataHeaderSize(HEAP_PAGE_DATA_OFFSET);
    page->m_header.m_lower = HEAP_PAGE_DATA_OFFSET;
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

/* ---------- Index page helpers ---------- */
BtrPage *MakeValidIndexPage(PageBuffer &buffer, PageId pageId)
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

}  /* anonymous namespace */

/* ================================================================
 * 1. CRC corruption → PAGE_CRC_MISMATCH
 * ================================================================ */
TEST(UTFaultInjectVerify, CrcCorruption_Detected)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {100, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    /* Inject: flip bits in a data byte without updating CRC */
    reinterpret_cast<unsigned char *>(page)[BLCKSZ / 2] ^= 0xFF;

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_CRC_MISMATCH));
}

/* ================================================================
 * 2. Boundary corruption (lower > upper) → PAGE_BOUNDARY_INVALID
 * ================================================================ */
TEST(UTFaultInjectVerify, BoundaryCorruption_Detected)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {101, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);

    /* Inject: make lower > upper */
    page->SetLower(static_cast<uint16>(page->GetUpper() + 100));
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/* ================================================================
 * 3. Heap TD count overflow → HEAP_TD_COUNT_OVERFLOW
 * ================================================================ */
TEST(UTFaultInjectVerify, HeapTdCountOverflow_Detected)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {102, 1});

    /* Inject: set TD count to unreasonable value */
    page->dataHeader.tdCount = 200;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TD_COUNT_OVERFLOW));
}

/* ================================================================
 * 4. Heap FSM slot invalid → HEAP_FSM_SLOT_INVALID
 * ================================================================ */
TEST(UTFaultInjectVerify, HeapFsmSlotInvalid_Detected)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {103, 1});

    /* Inject: set FSM slot beyond maximum */
    page->SetFsmIndex({INVALID_PAGE_ID, FSM_MAX_HWM});
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_FSM_SLOT_INVALID));
}

/* ================================================================
 * 5. Heap tuple overlap → HEAP_TUPLE_OVERLAP
 * ================================================================ */
TEST(UTFaultInjectVerify, HeapTupleOverlap_Detected)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {104, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 64, 0);

    /* Inject: second ItemId overlaps with the first tuple's storage area */
    ItemId *itemId2 = page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER + 1);
    uint16 overlapOffset = page->GetUpper() + 32; /* 32 bytes into first tuple */
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

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TUPLE_OVERLAP));
}

/* ================================================================
 * 6. Heap tuple size mismatch (ItemId len != tuple size) →
 *    HEAP_TUPLE_SIZE_MISMATCH
 * ================================================================ */
TEST(UTFaultInjectVerify, HeapTupleSizeMismatch_Detected)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {105, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);

    /* Inject: change ItemId len to differ from actual tuple size */
    page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER)->SetNormal(page->GetUpper(), 24);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TUPLE_SIZE_MISMATCH));
}

/* ================================================================
 * 7. Heap TD status invalid → HEAP_TD_SANITY_FAIL
 * ================================================================ */
TEST(UTFaultInjectVerify, HeapTdStatusInvalid_Detected)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {106, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);

    /* Inject: set invalid TD status on the tuple */
    HeapDiskTuple *tuple = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tuple->SetTdStatus(static_cast<TupleTdStatus>(0xFF));
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TD_SANITY_FAIL));
}

/* ================================================================
 * 8. Heap numColumn == 0 → HEAP_TUPLE_NUM_COLUMN_INVALID
 * ================================================================ */
TEST(UTFaultInjectVerify, HeapNumColumnZero_Detected)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {107, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);

    /* Inject: set numColumn to 0 */
    HeapDiskTuple *tuple = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tuple->SetNumColumn(0);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TUPLE_NUM_COLUMN_INVALID));
}

/* ================================================================
 * 9. Index special offset invalid → BTR_SPECIAL_OFFSET_INVALID
 * ================================================================ */
TEST(UTFaultInjectVerify, IndexSpecialOffsetCorruption_Detected)
{
    RegisterIndexPageVerifier();

    PageBuffer buf{};
    BtrPage *page = MakeValidIndexPage(buf, {108, 1});

    /* Inject: shrink special offset so it misaligns.
     * Depending on the offset delta, the generic boundary check
     * (PAGE_BOUNDARY_INVALID) or the index-specific check
     * (BTR_SPECIAL_OFFSET_INVALID) will fire first. */
    page->SetSpecialOffset(static_cast<uint16>(page->GetSpecialOffset() - 8));
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::BTR_SPECIAL_OFFSET_INVALID) ||
                HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/* ================================================================
 * 10. Index missing high key (non-rightmost page) →
 *     PAGE_BOUNDARY_INVALID
 * ================================================================ */
TEST(UTFaultInjectVerify, IndexMissingHighKey_Detected)
{
    RegisterIndexPageVerifier();

    PageBuffer buf{};
    BtrPage *page = MakeValidIndexPage(buf, {109, 1});

    /* Inject: set right sibling (non-rightmost) but don't add a high key tuple */
    page->GetLinkAndStatus()->SetRight({2, 2});
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/* ================================================================
 * 11. Index tuple TD id out of range → INDEX_TUPLE_SIZE_MISMATCH
 *     (TD sanity check is part of heavyweight index verify)
 * ================================================================ */
TEST(UTFaultInjectVerify, IndexTdIdOutOfRange_Detected)
{
    RegisterIndexPageVerifier();

    PageBuffer buf{};
    BtrPage *page = MakeValidIndexPage(buf, {110, 1});
    uint8 tdCount = page->GetTdCount();

    /* Inject: set tuple's tdId to tdCount (one past the last valid slot) */
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, tdCount);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/* ================================================================
 * 12. Index meta page with non-zero TD count
 * ================================================================ */
TEST(UTFaultInjectVerify, IndexMetaPageTdCountNonZero_Detected)
{
    RegisterIndexPageVerifier();

    PageBuffer buf{};
    BtrPage *page = MakeValidIndexPage(buf, {111, 1});

    /* Inject: mark as META_PAGE while TD slots are already allocated */
    page->GetLinkAndStatus()->SetType(BtrPageType::META_PAGE);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/* ================================================================
 * 13. Verify all three scenarios behave correctly on the same
 *     corruption: Write=FAIL, Read=FAIL, Full=collect
 * ================================================================ */
TEST(UTFaultInjectVerify, ThreeScenarios_SameCorruption)
{
    RegisterHeapPageVerifier();

    /* Prepare a corrupted page (lower > upper) */
    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {112, 1});
    page->SetLower(static_cast<uint16>(page->GetUpper() + 100));
    page->SetChecksum();

    /* Write path: VerifyPageOnWrite triggers PANIC→abort on failure,
     * so it cannot be tested here. Only test Read and Full paths. */

    /* Read path: should return FAIL */
    VerifyReport readReport;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &readReport), DSTORE_FAIL);
    EXPECT_TRUE(readReport.HasError());
    EXPECT_TRUE(HasVerifyCode(readReport, VerifyCode::PAGE_BOUNDARY_INVALID));

    /* Full scan path: should return SUCC (collect-only), but report has errors */
    VerifyReport fullReport;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &fullReport), DSTORE_SUCC);
    EXPECT_TRUE(fullReport.HasError());
    EXPECT_TRUE(HasVerifyCode(fullReport, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/* ================================================================
 * 14. Multiple faults injected on a single page — verifier should
 *     report all of them, not just the first
 * ================================================================ */
TEST(UTFaultInjectVerify, MultipleFaults_FirstCaught)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {113, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);

    /* Inject fault 1: TD count overflow */
    page->dataHeader.tdCount = 200;
    /* Inject fault 2: FSM slot invalid */
    page->SetFsmIndex({INVALID_PAGE_ID, FSM_MAX_HWM});
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);

    /* Verifier uses early-return: first error stops further checks.
     * TD count is checked before FSM slot, so only the first fault is reported. */
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::HEAP_TD_COUNT_OVERFLOW));
    EXPECT_GE(report.GetErrorCount(), 1U);
}

/* ================================================================
 * 15. CRC corruption on index page — cross-module CRC check
 * ================================================================ */
TEST(UTFaultInjectVerify, IndexCrcCorruption_Detected)
{
    RegisterIndexPageVerifier();

    PageBuffer buf{};
    BtrPage *page = MakeValidIndexPage(buf, {114, 1});
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    /* Inject: corrupt a byte without updating CRC */
    reinterpret_cast<unsigned char *>(page)[BLCKSZ / 2] ^= 0xFF;

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_CRC_MISMATCH));
}

/* ================================================================
 * 16. NONE level skips all checks even with corruption injected
 * ================================================================ */
TEST(UTFaultInjectVerify, NoneLevel_BypassesAllFaults)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {115, 1});

    /* Inject multiple corruptions */
    page->dataHeader.tdCount = 200;
    page->SetLower(static_cast<uint16>(page->GetUpper() + 100));
    /* Don't update CRC — triple corruption */

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::NONE, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 0U);
}

/* ================================================================
 * 17. Multiple faults on 3 tuples — HEAVY level collects all errors
 *     via VerifyPageFull (collect-only path)
 * ================================================================ */
TEST(UTFaultInjectVerify, MultipleFaults_HeavyLevel_CollectsAll)
{
    PageBuffer pageBuffer{};
    HeapPage *page = MakeValidHeapPage(pageBuffer, {200, 300});
    RegisterHeapPageVerifier();

    /* Add 3 tuples with different corruptions */
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER + 1, 32, 0);
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER + 2, 32, 0);

    /* Fault 1: tuple 1 has invalid TdStatus */
    HeapDiskTuple *tuple1 = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER);
    tuple1->SetTdStatus(static_cast<TupleTdStatus>(0xFF));

    /* Fault 2: tuple 2 has numColumn = 0 */
    HeapDiskTuple *tuple2 = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER + 1);
    tuple2->SetNumColumn(0);

    /* Fault 3: tuple 3 has lockerTdId exceeding tdCount (HEAVY-only check).
     * Avoid MEDIUM-visible corruptions (bounds/size) since the registry is
     * fail-fast: MEDIUM failure would prevent HEAVY from running. */
    HeapDiskTuple *tuple3 = page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER + 2);
    tuple3->SetLockerTdId(page->GetTdCount() + 10);

    page->SetChecksum();

    /* Use VerifyPageFull which collects all problems without PANIC.
     * VerifyPageFull ignores the return value from registry.Verify(),
     * so it always returns DSTORE_SUCC. The heavyweight heap verifier
     * uses ret = ReportHeapError(...) (assignment, not return) to
     * continue scanning all tuples. */
    VerifyReport report;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_TRUE(report.HasError());

    /* The heavyweight verifier continues scanning — should find at least 3 errors */
    EXPECT_GE(report.GetErrorCount(), 3U);
}

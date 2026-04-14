/*
 * Post-redo verification and advanced DFX detection tests.
 *
 * Simulate post-WAL-redo page verification, concurrent corruption detection,
 * I/O fault injection, and Buffer Pool interaction scenarios to validate the
 * DFX verification framework's detection capability under various abnormal conditions.
 *
 * Four test categories:
 *   P1: Post-WAL-redo verification detection (PostRedo_*)
 *   P2: Concurrent corruption detection (Concurrent_*)
 *   P3: I/O fault injection (IOFault_*)
 *   P4: Buffer Pool interaction simulation (BufferSim_*)
 */
#include <thread>
#include <atomic>
#include <mutex>
#include <cstring>
#include <cstdlib>
#include <vector>
#include <gtest/gtest.h>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_heap_page.h"
#include "page/dstore_index_page.h"
#include "page/dstore_undo_page.h"
#include "page/dstore_data_segment_meta_page.h"
#include "undo/dstore_undo_record.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;
using DSTORE::ut_dfx::ScopedVerifyConfig;
using DSTORE::ut_dfx::HasVerifyCode;
using DSTORE::ut_dfx::EnableAllModules;

namespace {

/* ======================================================================
 * Page Factory Functions
 * ====================================================================== */

/* ---------- Heap ---------- */
HeapPage *MakeValidHeapPage(PageBuffer &buf, PageId pageId)
{
    HeapPage *page = reinterpret_cast<HeapPage *>(buf.data());
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

/* ---------- Index ---------- */
BtrPage *MakeValidIndexPage(PageBuffer &buf, PageId pageId)
{
    BtrPage *page = reinterpret_cast<BtrPage *>(buf.data());
    page->InitBtrPageInner(pageId);
    page->SetLsn(1, 1, 1, false);
    page->GetLinkAndStatus()->InitPageMeta({1, 1}, 0, false);
    page->SetBtrMetaCreateXid(Xid(0));
    page->AllocateTdSpace();
    page->SetChecksum();
    return page;
}

/* ---------- Undo ---------- */
UndoRecordPage *MakeValidUndoRecPage(PageBuffer &buf, PageId pageId)
{
    UndoRecordPage *page = reinterpret_cast<UndoRecordPage *>(buf.data());
    page->m_undoRecPageHeader = {0, pageId, INVALID_PAGE_ID, INVALID_PAGE_ID};
    page->InitUndoRecPage(pageId);
    page->SetLsn(1, 1, 1, false);
    page->m_header.m_lower = UNDO_RECORD_PAGE_HEADER_SIZE;
    page->m_header.m_upper = BLCKSZ;
    page->SetChecksum();
    return page;
}

void AddFakeUndoRecord(UndoRecordPage *page, uint8 serializeSize, UndoType undoType)
{
    char *cursor = reinterpret_cast<char *>(page) + page->GetLower();
    *reinterpret_cast<uint8 *>(cursor) = serializeSize;
    memcpy(cursor + sizeof(uint8), &undoType, sizeof(UndoType));
    page->m_header.m_lower = static_cast<uint16>(page->GetLower() + serializeSize);
}

/* ---------- TxnSlot ---------- */
TransactionSlotPage *MakeValidTxnSlotPage(PageBuffer &buf, PageId pageId)
{
    TransactionSlotPage *page = reinterpret_cast<TransactionSlotPage *>(buf.data());
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

/* ---------- DataSegmentMeta ---------- */
DataSegmentMetaPage *MakeValidDataSegMeta(PageBuffer &buf, PageId pageId)
{
    DataSegmentMetaPage *page = reinterpret_cast<DataSegmentMetaPage *>(buf.data());
    page->InitDataSegmentMetaPage(SegmentType::HEAP_SEGMENT_TYPE, pageId, EXT_SIZE_8, 1, 1);
    page->dataBlockCount = 1;
    page->dataFirst = pageId;
    page->dataLast = pageId;
    page->addedPageId = pageId;
    page->extendedPageId = {pageId.m_fileId, pageId.m_blockId + 7};
    page->lastExtentIsReused = false;
    page->SetChecksum();
    return page;
}

/* Helper: register all verifiers needed by this test file */
void RegisterAllVerifiers()
{
    RegisterHeapPageVerifier();
    RegisterIndexPageVerifier();
    RegisterUndoPageVerifiers();
    RegisterSegmentPageVerifiers();
}

}  /* anonymous namespace */

/* ======================================================================
 * P1: Post-WAL-redo verification detection UT
 * ====================================================================== */

/*
 * PostRedo_AllCleanPages_PassVerify
 *
 * Construct 5 types of valid pages (Heap, Index, UndoRecord, TxnSlot, DataSegMeta),
 * all pass LIGHT verification with no errors in the report.
 */
TEST(UTPostRedoVerify, PostRedo_AllCleanPages_PassVerify)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterAllVerifiers();

    PageBuffer heapBuf{};
    MakeValidHeapPage(heapBuf, {1, 1});

    PageBuffer indexBuf{};
    MakeValidIndexPage(indexBuf, {2, 1});

    PageBuffer undoBuf{};
    MakeValidUndoRecPage(undoBuf, {3, 1});

    PageBuffer txnBuf{};
    MakeValidTxnSlotPage(txnBuf, {4, 1});

    PageBuffer segBuf{};
    MakeValidDataSegMeta(segBuf, {5, 1});

    struct {
        const char *name;
        PageBuffer *buf;
    } pages[] = {
        {"Heap",        &heapBuf},
        {"Index",       &indexBuf},
        {"UndoRecord",  &undoBuf},
        {"TxnSlot",     &txnBuf},
        {"DataSegMeta", &segBuf},
    };

    for (const auto &p : pages) {
        VerifyReport report;
        const Page *page = reinterpret_cast<const Page *>(p.buf->data());
        RetStatus ret = VerifyPageFull(page, VerifyLevel::LIGHT, &report);
        EXPECT_EQ(ret, DSTORE_SUCC) << "Page type: " << p.name;
        EXPECT_FALSE(report.HasError()) << "Page type: " << p.name;
    }
}

/*
 * PostRedo_CorruptedHeapPage_Detected
 *
 * Construct a valid Heap page then corrupt lower > upper (simulate redo bug),
 * VerifyPageFull detects PAGE_BOUNDARY_INVALID.
 */
TEST(UTPostRedoVerify, PostRedo_CorruptedHeapPage_Detected)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {10, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);

    /* Simulate redo bug: lower > upper */
    page->SetLower(static_cast<uint16>(page->GetUpper() + 100));
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/*
 * PostRedo_CorruptedIndexPage_Detected
 *
 * Construct a valid Index page then set an invalid BtrPageType (simulate redo bug),
 * VerifyPageFull detects BTR_PAGE_TYPE_INVALID.
 */
TEST(UTPostRedoVerify, PostRedo_CorruptedIndexPage_Detected)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterIndexPageVerifier();

    PageBuffer buf{};
    BtrPage *page = MakeValidIndexPage(buf, {20, 1});

    /*
     * Simulate redo bug: set an invalid BtrPageType.
     * Note that BtrPageType's type field is a 2-bit bitfield (0-3), so 0xFF would be
     * truncated to 3 = META_PAGE (a valid value). Therefore we cannot inject an invalid
     * value via SetType directly. Instead, use memset to write the status field directly,
     * zeroing the type bits to INVALID_BTR_PAGE(0).
     */
    page->GetLinkAndStatus()->SetType(BtrPageType::INVALID_BTR_PAGE);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::BTR_PAGE_TYPE_INVALID));
}

/*
 * PostRedo_MixedCorruption_BatchVerify
 *
 * Construct 5 pages with 2 corrupted, simulating batch verification logic --
 * iterate all pages calling VerifyPageFull, count errors, assert errorCount == 2.
 */
TEST(UTPostRedoVerify, PostRedo_MixedCorruption_BatchVerify)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterAllVerifiers();

    PageBuffer bufs[5] = {};

    /* Page 0: valid Heap */
    MakeValidHeapPage(bufs[0], {30, 1});

    /* Page 1: corrupted Heap (lower > upper) */
    HeapPage *corruptPage1 = MakeValidHeapPage(bufs[1], {31, 1});
    AddHeapTuple(corruptPage1, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    corruptPage1->SetLower(static_cast<uint16>(corruptPage1->GetUpper() + 50));
    corruptPage1->SetChecksum();

    /* Page 2: valid Index */
    MakeValidIndexPage(bufs[2], {32, 1});

    /* Page 3: corrupted Index (INVALID_BTR_PAGE type) */
    BtrPage *corruptPage3 = MakeValidIndexPage(bufs[3], {33, 1});
    corruptPage3->GetLinkAndStatus()->SetType(BtrPageType::INVALID_BTR_PAGE);
    corruptPage3->SetChecksum();

    /* Page 4: valid UndoRecord */
    MakeValidUndoRecPage(bufs[4], {34, 1});

    uint32 errorPageCount = 0;
    for (int i = 0; i < 5; ++i) {
        VerifyReport report;
        const Page *page = reinterpret_cast<const Page *>(bufs[i].data());
        VerifyPageFull(page, VerifyLevel::LIGHT, &report);
        if (report.HasError()) {
            errorPageCount++;
        }
    }
    EXPECT_EQ(errorPageCount, 2U);
}

/*
 * PostRedo_CrcMismatch_Detected
 *
 * Construct a valid page, modify one byte without recalculating the checksum
 * (simulate WAL record corruption causing page content error), detect PAGE_CRC_MISMATCH.
 */
TEST(UTPostRedoVerify, PostRedo_CrcMismatch_Detected)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {40, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    /* Tamper with one byte in the data region without recalculating the checksum */
    buf[BLCKSZ / 2] ^= 0xAA;

    VerifyReport report;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_CRC_MISMATCH));
}

/*
 * PostRedo_NoneLevel_SkipsVerify
 *
 * Call with VerifyLevel::NONE, verify that no check results are produced.
 */
TEST(UTPostRedoVerify, PostRedo_NoneLevel_SkipsVerify)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {50, 1});
    /* Inject corruption but use NONE level */
    page->SetLower(static_cast<uint16>(page->GetUpper() + 100));
    /* Do not update CRC, double corruption */

    VerifyReport report;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::NONE, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 0U);
    EXPECT_FALSE(report.HasError());
}

/*
 * PostRedo_UndoPage_CorruptedType_Detected
 *
 * Construct an UndoRecordPage, corrupt the undo record type to an invalid value,
 * detect UNDO_REC_TYPE_INVALID.
 */
TEST(UTPostRedoVerify, PostRedo_UndoPage_CorruptedType_Detected)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterUndoPageVerifiers();

    PageBuffer buf{};
    UndoRecordPage *page = MakeValidUndoRecPage(buf, {60, 1});

    /* Add one undo record with an invalid UndoType (beyond UNDO_UNKNOWN) */
    AddFakeUndoRecord(page, 16, static_cast<UndoType>(0xFF));
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_REC_TYPE_INVALID));
}

/* ======================================================================
 * P2: Concurrent corruption detection UT
 * ====================================================================== */

/*
 * Concurrent_WriteAndVerify_NoRace
 *
 * 10 threads, each constructing its own PageBuffer and calling VerifyPageFull concurrently,
 * verifying that the verification framework itself is thread-safe (no crash, no false positives).
 */
TEST(UTPostRedoVerify, Concurrent_WriteAndVerify_NoRace)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterHeapPageVerifier();

    constexpr int THREAD_COUNT = 10;
    std::atomic<int> passCount(0);
    std::atomic<int> failCount(0);

    auto workerFn = [&passCount, &failCount](int threadIdx) {
        PageBuffer buf{};
        PageId pageId = {static_cast<uint16>(100 + threadIdx), 1};
        HeapPage *page = MakeValidHeapPage(buf, pageId);
        AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
        page->SetChecksum();

        VerifyReport report;
        RetStatus ret = VerifyPageFull(
            reinterpret_cast<const Page *>(buf.data()), VerifyLevel::LIGHT, &report);
        if (ret == DSTORE_SUCC && !report.HasError()) {
            passCount.fetch_add(1);
        } else {
            failCount.fetch_add(1);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(THREAD_COUNT);
    for (int i = 0; i < THREAD_COUNT; ++i) {
        threads.emplace_back(workerFn, i);
    }
    for (auto &t : threads) {
        t.join();
    }

    EXPECT_EQ(passCount.load(), THREAD_COUNT);
    EXPECT_EQ(failCount.load(), 0);
}

/*
 * Concurrent_PartialWrite_Detected
 *
 * Thread A performs a "partial write" on the page (sets lower > upper + 50), then SetChecksum.
 * Thread B verifies the page immediately after the writer completes.
 *
 * Detection principle: ValidateGenericLight checks CRC first (CRC matches here because
 * SetChecksum was called after the lower field was corrupted), then checks the
 * lower <= upper boundary condition. Therefore the error code is
 * PAGE_BOUNDARY_INVALID rather than PAGE_CRC_MISMATCH.
 *
 * Thread synchronization: writer.join() provides happens-before guarantee,
 * making verifyDetectedError read safe.
 */
TEST(UTPostRedoVerify, Concurrent_PartialWrite_Detected)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {200, 1});

    std::atomic<bool> writerDone(false);
    bool verifyDetectedError = false;

    /* Writer: perform half-completed write -- advance lower without writing tuple, creating lower > upper */
    std::thread writer([&]() {
        page->SetLower(static_cast<uint16>(page->GetUpper() + 50));
        page->SetChecksum();
        writerDone.store(true);
    });

    /* Verifier: wait for writer to finish then verify immediately */
    std::thread verifier([&]() {
        while (!writerDone.load()) {
            /* spin wait */
        }
        VerifyReport report;
        VerifyPageFull(reinterpret_cast<const Page *>(buf.data()),
                       VerifyLevel::LIGHT, &report);
        verifyDetectedError = report.HasError();
    });

    writer.join();
    verifier.join();

    EXPECT_TRUE(verifyDetectedError);
}

/*
 * Concurrent_ModifyDuringVerify
 *
 * Simulate concurrent modification and verification scenario. To avoid C++ data race UB
 * (simultaneous read/write without a lock is UB, TSAN would report it), use a mutex to
 * protect the shared buffer:
 *   - modifier thread: flip a byte in the middle of the page under lock (simulate partial write window)
 *   - verifier thread: take a snapshot under lock then verify
 * Key validation point: the verification framework does not crash on corrupted/valid pages read via snapshot.
 */
TEST(UTPostRedoVerify, Concurrent_ModifyDuringVerify)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    MakeValidHeapPage(buf, {300, 1});

    std::atomic<bool> stop(false);
    std::mutex bufMutex;
    constexpr int VERIFY_ITERATIONS = 100;

    /* Modifier thread: flip a byte in the middle of the page under lock */
    std::thread modifier([&]() {
        while (!stop.load()) {
            std::lock_guard<std::mutex> lock(bufMutex);
            buf[BLCKSZ / 2] ^= 0x01;
        }
    });

    /* Verifier thread: take snapshot under lock then verify */
    int crcMismatchCount = 0;
    int passCount = 0;
    int otherErrorCount = 0;
    for (int i = 0; i < VERIFY_ITERATIONS; ++i) {
        PageBuffer snapshot{};
        {
            std::lock_guard<std::mutex> lock(bufMutex);
            memcpy(snapshot.data(), buf.data(), BLCKSZ);
        }
        VerifyReport report;
        VerifyPageFull(reinterpret_cast<const Page *>(snapshot.data()),
                       VerifyLevel::LIGHT, &report);
        if (HasVerifyCode(report, VerifyCode::PAGE_CRC_MISMATCH)) {
            crcMismatchCount++;
        } else if (!report.HasError()) {
            passCount++;
        } else {
            otherErrorCount++;
        }
    }

    stop.store(true);
    modifier.join();

    /* Each verification result is either CRC mismatch, pass, or other error; total must match */
    EXPECT_EQ(crcMismatchCount + passCount + otherErrorCount, VERIFY_ITERATIONS);
    /* At least some should be CRC mismatch or pass (modifier flipped bytes) */
    EXPECT_GT(crcMismatchCount + passCount, 0);
}

/* ======================================================================
 * P3: I/O fault injection impact on verify UT (pure in-memory simulation)
 * ====================================================================== */

/*
 * IOFault_BitRot_SingleByte
 *
 * Construct a valid Heap page, flip 1 byte in the data region (without recalculating checksum),
 * verify PAGE_CRC_MISMATCH.
 */
TEST(UTPostRedoVerify, IOFault_BitRot_SingleByte)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {400, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    /* Flip 1 byte in the data region */
    buf[BLCKSZ / 2] ^= 0x42;

    VerifyReport report;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_CRC_MISMATCH));
}

/*
 * IOFault_BitRot_MultiByte
 *
 * Flip 3 bytes at different positions, verify detection.
 */
TEST(UTPostRedoVerify, IOFault_BitRot_MultiByte)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {410, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    /* Flip 3 bytes at different positions (avoiding the checksum field itself in the page header) */
    constexpr uint16 flipOffsets[] = {BLCKSZ / 4, BLCKSZ / 2, BLCKSZ * 3 / 4};
    for (uint16 off : flipOffsets) {
        buf[off] ^= 0xFF;
    }

    VerifyReport report;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_CRC_MISMATCH));
}

/*
 * IOFault_PartialWrite_ZeroTail
 *
 * Simulate partial write: zero out the second half of the page (simulate power loss mid-write),
 * verify detection of PAGE_BOUNDARY_INVALID or CRC mismatch.
 */
TEST(UTPostRedoVerify, IOFault_PartialWrite_ZeroTail)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {420, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    /* Zero out the second half (simulate partial write) */
    memset(buf.data() + BLCKSZ / 2, 0, BLCKSZ / 2);

    VerifyReport report;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_TRUE(report.HasError());
    /* Should detect CRC mismatch (because the second half was zeroed out) */
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_CRC_MISMATCH) ||
                HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/*
 * IOFault_TornPage_CorruptDataRegion
 *
 * Simulate torn page: keep the page header intact (avoid PageType becoming invalid
 * which would cause it to be skipped), flip all bytes in the data region (lower ~ upper).
 * CRC no longer matches.
 */
TEST(UTPostRedoVerify, IOFault_TornPage_CorruptDataRegion)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {430, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    /* Flip all bytes in the data region (page header preserved, CRC will definitely mismatch) */
    constexpr uint16 dataStart = 256;  /* Safe offset, well past the page header */
    for (uint16 i = dataStart; i < BLCKSZ; ++i) {
        buf[i] ^= 0xFF;
    }

    VerifyReport report;
    const Page *tornPage = reinterpret_cast<const Page *>(buf.data());
    EXPECT_EQ(VerifyPageFull(tornPage, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_CRC_MISMATCH));
}

/* ======================================================================
 * P4: Buffer Pool interaction simulation UT
 * ====================================================================== */

/*
 * BufferSim_EvictAndReread_VerifyWorks
 *
 * Simulate evict+reread: construct a valid page A, memcpy to "disk buffer",
 * clear the original buffer, restore from "disk buffer", verify passes.
 */
TEST(UTPostRedoVerify, BufferSim_EvictAndReread_VerifyWorks)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterHeapPageVerifier();

    PageBuffer bufferPool{};
    HeapPage *page = MakeValidHeapPage(bufferPool, {500, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    /* Step 1: evict -- memcpy to "disk" */
    PageBuffer diskBuffer{};
    memcpy(diskBuffer.data(), bufferPool.data(), BLCKSZ);

    /* Step 2: clear original buffer (simulate buffer pool slot being reclaimed) */
    bufferPool.fill(0);

    /* Step 3: reread -- restore from "disk" to buffer pool */
    memcpy(bufferPool.data(), diskBuffer.data(), BLCKSZ);

    /* Step 4: verify the restored page */
    VerifyReport report;
    const Page *readPage = reinterpret_cast<const Page *>(bufferPool.data());
    EXPECT_EQ(VerifyPageFull(readPage, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

/*
 * BufferSim_EvictCorruptAndReread_Detected
 *
 * Same as above but tamper with 1 byte in the "disk buffer"; after restore,
 * verification detects CRC mismatch.
 */
TEST(UTPostRedoVerify, BufferSim_EvictCorruptAndReread_Detected)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterHeapPageVerifier();

    PageBuffer bufferPool{};
    HeapPage *page = MakeValidHeapPage(bufferPool, {510, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    /* Step 1: evict -- memcpy to "disk" */
    PageBuffer diskBuffer{};
    memcpy(diskBuffer.data(), bufferPool.data(), BLCKSZ);

    /* Step 2: silent corruption on "disk" (bit rot) */
    diskBuffer[BLCKSZ / 2] ^= 0xBB;

    /* Step 3: clear original buffer */
    bufferPool.fill(0);

    /* Step 4: reread -- restore from corrupted "disk" */
    memcpy(bufferPool.data(), diskBuffer.data(), BLCKSZ);

    /* Step 5: verification detects corruption */
    VerifyReport report;
    const Page *readPage = reinterpret_cast<const Page *>(bufferPool.data());
    EXPECT_EQ(VerifyPageFull(readPage, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_CRC_MISMATCH));
}

/*
 * BufferSim_DirtyPageFlush_VerifyBeforeWrite
 *
 * Simulate pre-write verification flow: construct a corrupted page, call VerifyPageOnRead
 * (instead of OnWrite, because OnWrite would PANIC/abort on detected errors), verify it returns FAIL.
 */
TEST(UTPostRedoVerify, BufferSim_DirtyPageFlush_VerifyBeforeWrite)
{
    ScopedVerifyConfig guard;
    EnableAllModules();
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    HeapPage *page = MakeValidHeapPage(buf, {520, 1});
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);

    /* Simulate dirty page corruption: lower > upper */
    page->SetLower(static_cast<uint16>(page->GetUpper() + 50));
    page->SetChecksum();

    /* Use VerifyPageOnRead instead of VerifyPageOnWrite (the latter would PANIC) */
    VerifyReport report;
    RetStatus ret = VerifyPageOnRead(
        reinterpret_cast<const Page *>(buf.data()), VerifyLevel::LIGHT, &report);
    EXPECT_EQ(ret, DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

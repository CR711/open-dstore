/*
 * Unit tests for Undo page verification (UndoRecordPage + TransactionSlotPage).
 *
 * Covers both lightweight and heavyweight verifiers registered by
 * RegisterUndoPageVerifiers().
 */
#include <cstring>
#include <gtest/gtest.h>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_undo_page.h"
#include "undo/dstore_undo_record.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;
using DSTORE::ut_dfx::HasVerifyCode;

namespace {

/* Initialize a valid UndoRecordPage */
UndoRecordPage *InitUndoRecordPage(PageBuffer &buffer, PageId pageId)
{
    UndoRecordPage *page = reinterpret_cast<UndoRecordPage *>(buffer.data());
    page->m_undoRecPageHeader = {0, pageId, INVALID_PAGE_ID, INVALID_PAGE_ID};
    page->InitUndoRecPage(pageId);
    page->SetLsn(1, 1, 1, false);
    /* Set lower to data area start; records are written from this offset onwards.
     * Note: Page::Init sets lower=sizeof(Page), but for test convenience we use
     * UNDO_RECORD_PAGE_HEADER_SIZE so AddFakeUndoRecord won't overwrite the header. */
    page->m_header.m_lower = UNDO_RECORD_PAGE_HEADER_SIZE;
    page->m_header.m_upper = BLCKSZ;
    page->SetChecksum();
    return page;
}

/*
 * Write a fake undo record into an UndoRecordPage.
 * Layout: [serializeSize:1B][undoType:1B][padding...]
 * Total size = serializeSize, minimum 2 (1 + 1).
 * Note: remaining fields are varint-compressed and not validated by the
 * heavyweight verifier, so we only need serializeSize and undoType.
 */
void AddFakeUndoRecord(UndoRecordPage *page, uint8 serializeSize, UndoType undoType)
{
    char *cursor = reinterpret_cast<char *>(page) + page->GetLower();
    /* serializeSize */
    *reinterpret_cast<uint8 *>(cursor) = serializeSize;
    /* undoType */
    memcpy(cursor + sizeof(uint8), &undoType, sizeof(UndoType));
    /* advance lower */
    page->m_header.m_lower = static_cast<uint16>(page->GetLower() + serializeSize);
}

/* Initialize a valid TransactionSlotPage */
TransactionSlotPage *InitTransactionSlotPage(PageBuffer &buffer, PageId pageId)
{
    TransactionSlotPage *page = reinterpret_cast<TransactionSlotPage *>(buffer.data());
    page->InitTxnSlotPage(pageId);
    page->SetLsn(1, 1, 1, false);
    page->m_header.m_lower = TRX_PAGE_HEADER_SIZE;
    page->m_header.m_upper = BLCKSZ;
    /* Initialize all slots to FROZEN status */
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

/* ========== UndoRecordPage Lightweight Tests ========== */

TEST_F(UTUndoPageVerify, ValidUndoRecordPagePassesLight)
{
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 100});

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTUndoPageVerify, UndoRecordPageLowerBelowHeaderFails)
{
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 101});

    page->m_header.m_lower = static_cast<uint16>(sizeof(Page)) - 1;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

TEST_F(UTUndoPageVerify, UndoRecordPageLowerAboveUpperFails)
{
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 102});

    page->m_header.m_lower = BLCKSZ;
    page->m_header.m_upper = UNDO_RECORD_PAGE_HEADER_SIZE;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST_F(UTUndoPageVerify, UndoRecordPageCurMismatchFails)
{
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 103});

    /* Set cur to a different page id than self */
    page->m_undoRecPageHeader.cur = {10, 999};
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_ID_MISMATCH));
}

TEST_F(UTUndoPageVerify, UndoRecordPageCurInvalidSkipped)
{
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 104});

    /* cur = INVALID_PAGE_ID should be tolerated */
    page->m_undoRecPageHeader.cur = INVALID_PAGE_ID;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

/* ========== UndoRecordPage Heavyweight Tests ========== */

TEST_F(UTUndoPageVerify, ValidUndoRecordWithRecordsPassesHeavy)
{
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 200});

    /* Add 2 valid records: serializeSize=16, undoType=UNDO_HEAP_INSERT */
    AddFakeUndoRecord(page, 16, UNDO_HEAP_INSERT);
    AddFakeUndoRecord(page, 16, UNDO_HEAP_INSERT);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTUndoPageVerify, UndoRecordSizeZeroFails)
{
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 201});

    /* Manually write a record with serializeSize = 0 at lower position */
    char *cursor = reinterpret_cast<char *>(page) + page->GetLower();
    *reinterpret_cast<uint8 *>(cursor) = 0;
    /* Advance lower by 1 byte so the heavyweight loop enters */
    page->m_header.m_lower = static_cast<uint16>(page->GetLower() + 1);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

TEST_F(UTUndoPageVerify, UndoRecordSizeTooSmallFails)
{
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 202});

    /* serializeSize = 1 (< minimum 2 = serializeSize + undoType) */
    char *cursor = reinterpret_cast<char *>(page) + page->GetLower();
    *reinterpret_cast<uint8 *>(cursor) = 1;
    page->m_header.m_lower = static_cast<uint16>(page->GetLower() + 1);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

TEST_F(UTUndoPageVerify, UndoRecordSizeCrossesLimitFails)
{
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 203});

    /* Write a record claiming serializeSize=200, but only advance lower by 50 */
    char *cursor = reinterpret_cast<char *>(page) + page->GetLower();
    *reinterpret_cast<uint8 *>(cursor) = 200;
    page->m_header.m_lower = static_cast<uint16>(page->GetLower() + 50);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

TEST_F(UTUndoPageVerify, UndoRecordTypeInvalidFails)
{
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, {10, 204});

    AddFakeUndoRecord(page, 16, UNDO_UNKNOWN);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_REC_TYPE_INVALID));
}

TEST_F(UTUndoPageVerify, UndoRecordPageSelfReferenceFails)
{
    PageId selfId = {10, 206};
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, selfId);

    /* prev or next points to self */
    page->m_undoRecPageHeader.prev = selfId;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_ID_INVALID));
}

/* ========== TransactionSlotPage Lightweight Tests ========== */

TEST_F(UTUndoPageVerify, ValidTxnSlotPagePassesLight)
{
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 300});

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTUndoPageVerify, TxnSlotPageLowerMismatchFails)
{
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 301});

    page->m_header.m_lower = TRX_PAGE_HEADER_SIZE + 1;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

TEST_F(UTUndoPageVerify, TxnSlotPageUpperMismatchFails)
{
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 302});

    page->m_header.m_upper = BLCKSZ - 1;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_BOUNDARY_INVALID));
}

/* ========== TransactionSlotPage Heavyweight Tests ========== */

TEST_F(UTUndoPageVerify, ValidTxnSlotPagePassesHeavy)
{
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 400});

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST_F(UTUndoPageVerify, TxnSlotStatusInvalidFails)
{
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 401});

    page->m_slots[0].SetTrxSlotStatus(static_cast<TrxSlotStatus>(0xFF));
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_SLOT_STATE_INVALID));
}

TEST_F(UTUndoPageVerify, TxnSlotCommittedWithInvalidCsnFails)
{
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 402});

    page->m_slots[0].SetTrxSlotStatus(TXN_STATUS_COMMITTED);
    page->m_slots[0].SetCsn(INVALID_CSN);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_SLOT_XID_INVALID));
}

TEST_F(UTUndoPageVerify, TxnSlotNextLogicSlotExceedsRangeFails)
{
    PageId selfId = {10, 403};
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, selfId);

    /* Set nextFreeLogicSlotId beyond the valid range for this page:
     * valid max = TRX_PAGE_SLOTS_NUM + blockNum * TRX_PAGE_SLOTS_NUM */
    uint64 overLimit = static_cast<uint64>(TRX_PAGE_SLOTS_NUM) +
        static_cast<uint64>(page->GetBlockNum()) * static_cast<uint64>(TRX_PAGE_SLOTS_NUM) + 1;
    page->SetNextFreeLogicSlotId(overLimit);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_SLOT_STATE_INVALID));
}

TEST_F(UTUndoPageVerify, UndoRecordPageNextSelfReferenceFails)
{
    PageId selfId = {10, 207};
    UndoRecordPage *page = InitUndoRecordPage(pageBuffer, selfId);

    /* next points to self */
    page->m_undoRecPageHeader.next = selfId;
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::PAGE_ID_INVALID));
}

TEST_F(UTUndoPageVerify, TxnSlotAbortedWithInvalidCsnFails)
{
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 404});

    page->m_slots[0].SetTrxSlotStatus(TXN_STATUS_ABORTED);
    page->m_slots[0].SetCsn(INVALID_CSN);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_SLOT_XID_INVALID));
}

TEST_F(UTUndoPageVerify, TxnSlotPreparedWithInvalidCsnFails)
{
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 405});

    page->m_slots[0].SetTrxSlotStatus(TXN_STATUS_PREPARED);
    page->m_slots[0].SetCsn(INVALID_CSN);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_SLOT_XID_INVALID));
}

TEST_F(UTUndoPageVerify, MultipleSlotCorruptionReportsFirst)
{
    TransactionSlotPage *page = InitTransactionSlotPage(pageBuffer, {10, 406});

    /* Corrupt slot 0 and slot 1 */
    page->m_slots[0].SetTrxSlotStatus(static_cast<TrxSlotStatus>(0xFE));
    page->m_slots[1].SetTrxSlotStatus(static_cast<TrxSlotStatus>(0xFF));
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVY, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    /* Should have exactly 1 error (breaks after first) */
    EXPECT_TRUE(HasVerifyCode(report, VerifyCode::UNDO_SLOT_STATE_INVALID));
}

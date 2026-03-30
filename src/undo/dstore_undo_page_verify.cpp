#include "dfx/dstore_page_verify.h"

#include "page/dstore_undo_page.h"
#include "undo/dstore_undo_record.h"

namespace DSTORE {

namespace {

RetStatus ReportUndoError(VerifyReport *report, const Page *page, const char *checkName, uint64 expected, uint64 actual,
    const char *message)
{
    if (report != nullptr) {
        report->AddResult(VerifySeverity::ERROR_LEVEL, "page", page->GetSelfPageId(), checkName, expected, actual, "%s",
            message);
    }
    return DSTORE_FAIL;
}

bool IsValidTxnSlotStatus(TrxSlotStatus status)
{
    switch (status) {
        case TXN_STATUS_FROZEN:
        case TXN_STATUS_IN_PROGRESS:
        case TXN_STATUS_PENDING_COMMIT:
        case TXN_STATUS_COMMITTED:
        case TXN_STATUS_ABORTED:
        case TXN_STATUS_FAILED:
        case TXN_STATUS_PREPARED:
            return true;
        default:
            return false;
    }
}

RetStatus VerifyUndoRecordPageLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const UndoRecordPage *undoPage = static_cast<const UndoRecordPage *>(page);

    if (undoPage->GetLower() < UNDO_RECORD_PAGE_HEADER_SIZE || undoPage->GetLower() > undoPage->GetUpper()) {
        return ReportUndoError(report, undoPage, "undo_page_lower_invalid", UNDO_RECORD_PAGE_HEADER_SIZE,
            undoPage->GetLower(), "Undo record page lower offset is outside the record area");
    }

    if (!undoPage->m_undoRecPageHeader.cur.IsInvalid() &&
        undoPage->m_undoRecPageHeader.cur != undoPage->GetSelfPageId()) {
        return ReportUndoError(report, undoPage, "undo_page_cur_invalid", undoPage->GetSelfPageId().m_blockId,
            undoPage->m_undoRecPageHeader.cur.m_blockId, "Undo record page header cur page id mismatches self page id");
    }

    return DSTORE_SUCC;
}

RetStatus VerifyUndoRecordPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    RetStatus ret = VerifyUndoRecordPageLightweight(page, level, report);
    const UndoRecordPage *undoPage = static_cast<const UndoRecordPage *>(page);
    const char *cursor = reinterpret_cast<const char *>(undoPage) + UNDO_RECORD_PAGE_HEADER_SIZE;
    const char *limit = reinterpret_cast<const char *>(undoPage) + undoPage->GetLower();

    if (undoPage->m_undoRecPageHeader.prev == undoPage->GetSelfPageId() ||
        undoPage->m_undoRecPageHeader.next == undoPage->GetSelfPageId()) {
        ret = ReportUndoError(report, undoPage, "undo_page_link_self_reference", 0, 1,
            "Undo record page prev/next link must not point to itself");
    }

    while (cursor < limit) {
        const uint8 serializeSize = *reinterpret_cast<const uint8 *>(cursor);
        if (serializeSize == 0 || cursor + serializeSize > limit) {
            ret = ReportUndoError(report, undoPage, "undo_record_size_invalid",
                static_cast<uint64>(limit - cursor), serializeSize,
                "Undo record serialized size is zero or crosses the page lower bound");
            break;
        }

        const UndoType undoType = *reinterpret_cast<const UndoType *>(cursor + sizeof(uint8));
        if (undoType >= UNDO_UNKNOWN) {
            ret = ReportUndoError(report, undoPage, "undo_record_type_invalid", UNDO_UNKNOWN - 1, undoType,
                "Undo record type is invalid");
            break;
        }

        const uint64 fileVersion = *reinterpret_cast<const uint64 *>(cursor + serializeSize - sizeof(uint64));
        if (fileVersion == INVALID_FILE_VERSION) {
            ret = ReportUndoError(report, undoPage, "undo_record_file_version_invalid", INVALID_FILE_VERSION - 1,
                fileVersion, "Undo record file version is invalid");
            break;
        }

        cursor += serializeSize;
    }

    return ret;
}

RetStatus VerifyTransactionSlotPageLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const TransactionSlotPage *slotPage = static_cast<const TransactionSlotPage *>(page);

    if (slotPage->GetLower() != TRX_PAGE_HEADER_SIZE) {
        return ReportUndoError(report, slotPage, "txn_slot_page_lower_invalid", TRX_PAGE_HEADER_SIZE,
            slotPage->GetLower(), "Transaction slot page lower offset must match the fixed header size");
    }

    if (slotPage->GetUpper() != BLCKSZ) {
        return ReportUndoError(report, slotPage, "txn_slot_page_upper_invalid", BLCKSZ, slotPage->GetUpper(),
            "Transaction slot page upper offset must span the full page");
    }

    return DSTORE_SUCC;
}

RetStatus VerifyTransactionSlotPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    RetStatus ret = VerifyTransactionSlotPageLightweight(page, level, report);
    const TransactionSlotPage *slotPage = static_cast<const TransactionSlotPage *>(page);

    if (slotPage->GetNextFreeLogicSlotId() > static_cast<uint64>(TRX_PAGE_SLOTS_NUM) + slotPage->GetBlockNum() *
        static_cast<uint64>(TRX_PAGE_SLOTS_NUM)) {
        ret = ReportUndoError(report, slotPage, "txn_slot_page_next_logic_slot_invalid",
            static_cast<uint64>(TRX_PAGE_SLOTS_NUM) + slotPage->GetBlockNum() * static_cast<uint64>(TRX_PAGE_SLOTS_NUM),
            slotPage->GetNextFreeLogicSlotId(), "Transaction slot page next free logic slot id exceeds local page range");
    }

    for (uint32 slotId = 0; slotId < TRX_PAGE_SLOTS_NUM; ++slotId) {
        const TransactionSlot *slot = &slotPage->m_slots[slotId];
        if (!IsValidTxnSlotStatus(slot->GetTxnSlotStatus())) {
            ret = ReportUndoError(report, slotPage, "txn_slot_status_invalid", TXN_STATUS_PREPARED,
                slot->GetTxnSlotStatus(), "Transaction slot status is invalid");
            break;
        }

        if ((slot->GetTxnSlotStatus() == TXN_STATUS_COMMITTED || slot->GetTxnSlotStatus() == TXN_STATUS_ABORTED ||
             slot->GetTxnSlotStatus() == TXN_STATUS_PREPARED) && slot->GetCsn() == INVALID_CSN) {
            ret = ReportUndoError(report, slotPage, "txn_slot_csn_invalid", 1, 0,
                "Committed/aborted/prepared transaction slot must keep a valid CSN");
            break;
        }
    }

    return ret;
}

}  // namespace

void RegisterUndoPageVerifiers()
{
    (void)RegisterPageVerifier(
        PageType::UNDO_PAGE_TYPE, "UndoRecordPage", VerifyUndoRecordPageLightweight, VerifyUndoRecordPageHeavyweight);
    (void)RegisterPageVerifier(PageType::TRANSACTION_SLOT_PAGE, "TransactionSlotPage",
        VerifyTransactionSlotPageLightweight, VerifyTransactionSlotPageHeavyweight);
}

}  // namespace DSTORE

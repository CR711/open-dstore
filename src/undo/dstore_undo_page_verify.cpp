#include "dfx/dstore_page_verify.h"

#include <cstring>
#include "page/dstore_undo_page.h"
#include "undo/dstore_undo_record.h"

namespace DSTORE {

namespace {

RetStatus ReportUndoError(VerifyReport *report, const Page *page, const char *checkName, uint64 expected, uint64 actual,
    const char *message, VerifyCode code)
{
    if (report != nullptr) {
        const PageId pageId = (page != nullptr) ? page->GetSelfPageId() : INVALID_PAGE_ID;
        report->AddResultWithCode(
            VerifySeverity::SEVERITY_ERROR, code, "page", pageId, checkName, expected, actual, "%s", message);
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

    /*
     * lower 的合法最小值为 sizeof(Page)（即 Page::Init 的初始值），
     * 而非 UNDO_RECORD_PAGE_HEADER_SIZE。InitUndoRecPage 调用 Page::Init 后
     * 不会修正 m_lower，因此空页面的 lower == sizeof(Page) 是正常状态。
     */
    const uint16 minLower = static_cast<uint16>(sizeof(Page));
    if (undoPage->GetLower() < minLower || undoPage->GetLower() > undoPage->GetUpper()) {
        return ReportUndoError(report, undoPage, "undo_page_lower_invalid", minLower,
            undoPage->GetLower(), "Undo record page lower offset is outside the valid range",
            VerifyCode::PAGE_BOUNDARY_INVALID);
    }

    if (!undoPage->m_undoRecPageHeader.cur.IsInvalid() &&
        undoPage->m_undoRecPageHeader.cur != undoPage->GetSelfPageId()) {
        return ReportUndoError(report, undoPage, "undo_page_cur_invalid", undoPage->GetSelfPageId().m_blockId,
            undoPage->m_undoRecPageHeader.cur.m_blockId, "Undo record page header cur page id mismatches self page id",
            VerifyCode::PAGE_ID_MISMATCH);
    }

    return DSTORE_SUCC;
}

RetStatus VerifyUndoRecordPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    RetStatus ret = DSTORE_SUCC;
    const UndoRecordPage *undoPage = static_cast<const UndoRecordPage *>(page);
    const char *cursor = reinterpret_cast<const char *>(undoPage) + UNDO_RECORD_PAGE_HEADER_SIZE;
    const char *limit = reinterpret_cast<const char *>(undoPage) + undoPage->GetLower();

    if (undoPage->m_undoRecPageHeader.prev == undoPage->GetSelfPageId() ||
        undoPage->m_undoRecPageHeader.next == undoPage->GetSelfPageId()) {
        ret = ReportUndoError(report, undoPage, "undo_page_link_self_reference", 0, 1,
            "Undo record page prev/next link must not point to itself", VerifyCode::PAGE_ID_INVALID);
    }

    /*
     * Minimum undo record: serializeSize(1B) + undoType(1B) = 2.
     * Remaining fields (cid, tdPreUndoPtr, xid, csn, tdId, csnStatus, txnPreUndoPtr,
     * ctid, fileVersion) are all varint-compressed and cannot be decoded at fixed offsets,
     * so we only validate serializeSize range and undoType legality here.
     */
    constexpr uint8 UNDO_RECORD_MIN_SERIALIZE_SIZE = sizeof(uint8) + sizeof(UndoType);

    while (cursor < limit) {
        const uint8 serializeSize = *reinterpret_cast<const uint8 *>(cursor);
        if (serializeSize < UNDO_RECORD_MIN_SERIALIZE_SIZE || cursor + serializeSize > limit) {
            ret = ReportUndoError(report, undoPage, "undo_record_size_invalid",
                static_cast<uint64>(limit - cursor), serializeSize,
                "Undo record serialized size is too small or crosses the page lower bound",
                VerifyCode::PAGE_BOUNDARY_INVALID);
            break;
        }

        UndoType undoType;
        memcpy(&undoType, cursor + sizeof(uint8), sizeof(UndoType));
        if (undoType >= UNDO_UNKNOWN) {
            ret = ReportUndoError(report, undoPage, "undo_record_type_invalid", UNDO_UNKNOWN - 1, undoType,
                "Undo record type is invalid", VerifyCode::UNDO_REC_TYPE_INVALID);
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

    /*
     * lower 的合法值：sizeof(Page)（Page::Init 初始值）或 TRX_PAGE_HEADER_SIZE。
     * InitTxnSlotPage 调用 Page::Init 后不修正 m_lower，因此二者均为合法状态。
     */
    if (slotPage->GetLower() != TRX_PAGE_HEADER_SIZE &&
        slotPage->GetLower() != static_cast<uint16>(sizeof(Page))) {
        return ReportUndoError(report, slotPage, "txn_slot_page_lower_invalid", TRX_PAGE_HEADER_SIZE,
            slotPage->GetLower(), "Transaction slot page lower offset does not match expected header size",
            VerifyCode::PAGE_BOUNDARY_INVALID);
    }

    if (slotPage->GetUpper() != BLCKSZ) {
        return ReportUndoError(report, slotPage, "txn_slot_page_upper_invalid", BLCKSZ, slotPage->GetUpper(),
            "Transaction slot page upper offset must span the full page", VerifyCode::PAGE_BOUNDARY_INVALID);
    }

    return DSTORE_SUCC;
}

RetStatus VerifyTransactionSlotPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    RetStatus ret = DSTORE_SUCC;
    const TransactionSlotPage *slotPage = static_cast<const TransactionSlotPage *>(page);

    if (slotPage->GetNextFreeLogicSlotId() > static_cast<uint64>(TRX_PAGE_SLOTS_NUM) + slotPage->GetBlockNum() *
        static_cast<uint64>(TRX_PAGE_SLOTS_NUM)) {
        ret = ReportUndoError(report, slotPage, "txn_slot_page_next_logic_slot_invalid",
            static_cast<uint64>(TRX_PAGE_SLOTS_NUM) + slotPage->GetBlockNum() * static_cast<uint64>(TRX_PAGE_SLOTS_NUM),
            slotPage->GetNextFreeLogicSlotId(),
            "Transaction slot page next free logic slot id exceeds local page range", VerifyCode::UNDO_SLOT_STATE_INVALID);
    }

    for (uint32 slotId = 0; slotId < TRX_PAGE_SLOTS_NUM; ++slotId) {
        const TransactionSlot *slot = &slotPage->m_slots[slotId];
        if (!IsValidTxnSlotStatus(slot->GetTxnSlotStatus())) {
            ret = ReportUndoError(report, slotPage, "txn_slot_status_invalid", TXN_STATUS_PREPARED,
                slot->GetTxnSlotStatus(), "Transaction slot status is invalid", VerifyCode::UNDO_SLOT_STATE_INVALID);
            break;
        }

        if ((slot->GetTxnSlotStatus() == TXN_STATUS_COMMITTED || slot->GetTxnSlotStatus() == TXN_STATUS_ABORTED ||
             slot->GetTxnSlotStatus() == TXN_STATUS_PREPARED) && slot->GetCsn() == INVALID_CSN) {
            ret = ReportUndoError(report, slotPage, "txn_slot_csn_invalid", 1, 0,
                "Committed/aborted/prepared transaction slot must keep a valid CSN", VerifyCode::UNDO_SLOT_XID_INVALID);
            break;
        }
    }

    return ret;
}

}  // namespace

void RegisterUndoPageVerifiers()
{
    (void)RegisterPageVerifier(
        PageType::UNDO_PAGE_TYPE, "UndoRecordPage", VerifyModule::UNDO,
        VerifyUndoRecordPageLightweight, nullptr, VerifyUndoRecordPageHeavyweight);
    (void)RegisterPageVerifier(PageType::TRANSACTION_SLOT_PAGE, "TransactionSlotPage",
        VerifyModule::UNDO, VerifyTransactionSlotPageLightweight, nullptr, VerifyTransactionSlotPageHeavyweight);
}

}  // namespace DSTORE

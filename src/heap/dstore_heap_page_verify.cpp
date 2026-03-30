#include "dfx/dstore_page_verify.h"

#include "page/dstore_heap_page.h"

namespace DSTORE {

namespace {

RetStatus ReportHeapError(VerifyReport *report, const HeapPage *page, const char *checkName, uint64 expected, uint64 actual,
    const char *message)
{
    if (report != nullptr) {
        report->AddResult(VerifySeverity::ERROR_LEVEL, "page", page->GetSelfPageId(), checkName, expected, actual, "%s",
            message);
    }
    return DSTORE_FAIL;
}

bool IsHeapTupleTdStatusValid(TupleTdStatus status)
{
    switch (status) {
        case ATTACH_TD_AS_HISTORY_OWNER:
        case ATTACH_TD_AS_NEW_OWNER:
        case DETACH_TD:
            return true;
        default:
            return false;
    }
}

RetStatus VerifyHeapPageLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const HeapPage *heapPage = static_cast<const HeapPage *>(page);
    const FsmIndex fsmIndex = heapPage->GetFsmIndex();

    if (heapPage->GetPotentialDelSize() > BLCKSZ) {
        return ReportHeapError(report, heapPage, "heap_potential_delete_size", BLCKSZ, heapPage->GetPotentialDelSize(),
            "Heap page potential delete size exceeds page size");
    }

    if (fsmIndex.slot >= FSM_MAX_HWM && fsmIndex.slot != INVALID_FSM_SLOT_NUM) {
        return ReportHeapError(report, heapPage, "heap_fsm_slot_invalid", FSM_MAX_HWM - 1, fsmIndex.slot,
            "Heap page FSM slot is out of range");
    }

    if (fsmIndex.page.IsInvalid() && fsmIndex.slot != 0) {
        return ReportHeapError(report, heapPage, "heap_fsm_index_invalid", 0, fsmIndex.slot,
            "Heap page FSM slot must be zero when FSM page id is invalid");
    }

    return DSTORE_SUCC;
}

RetStatus VerifyHeapPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    RetStatus ret = VerifyHeapPageLightweight(page, level, report);
    const HeapPage *heapPage = static_cast<const HeapPage *>(page);
    uint32 previousTupleBegin = heapPage->GetSpecialOffset();

    if (heapPage->GetTdCount() < MIN_TD_COUNT || heapPage->GetTdCount() > MAX_TD_COUNT) {
        return ReportHeapError(report, heapPage, "heap_td_count_invalid", MIN_TD_COUNT, heapPage->GetTdCount(),
            "Heap page TD count is out of supported range");
    }

    for (OffsetNumber offset = FIRST_ITEM_OFFSET_NUMBER; offset <= heapPage->GetMaxOffset(); ++offset) {
        const ItemId *itemId = const_cast<HeapPage *>(heapPage)->GetItemIdPtr(offset);

        if (itemId->IsUnused()) {
            if (itemId->GetLen() != 0) {
                ret = ReportHeapError(report, heapPage, "heap_unused_item_len", 0, itemId->GetLen(),
                    "Unused ItemId must have zero length");
            }
            continue;
        }

        if (itemId->IsNormal()) {
            const HeapDiskTuple *diskTuple = const_cast<HeapPage *>(heapPage)->GetDiskTuple(offset);
            if (itemId->GetLen() == 0 || itemId->GetLen() < diskTuple->GetTupleSize()) {
                ret = ReportHeapError(report, heapPage, "heap_item_len_mismatch", diskTuple->GetTupleSize(),
                    itemId->GetLen(), "Heap ItemId length is smaller than tuple size");
            }
            if (itemId->GetOffset() < heapPage->GetUpper() ||
                itemId->GetOffset() + itemId->GetLen() > heapPage->GetSpecialOffset()) {
                ret = ReportHeapError(report, heapPage, "heap_item_offset_invalid", heapPage->GetUpper(),
                    itemId->GetOffset(), "Heap ItemId points outside tuple storage area");
            }
            if (itemId->GetOffset() + itemId->GetLen() > previousTupleBegin) {
                ret = ReportHeapError(report, heapPage, "heap_tuple_overlap", previousTupleBegin,
                    itemId->GetOffset() + itemId->GetLen(), "Heap tuple storage overlaps with previous tuple");
            } else {
                previousTupleBegin = itemId->GetOffset();
            }

            const TupleTdStatus tdStatus = diskTuple->GetTdStatus();
            if (!IsHeapTupleTdStatusValid(tdStatus)) {
                ret = ReportHeapError(report, heapPage, "heap_td_status_invalid", DETACH_TD, tdStatus,
                    "Heap tuple TD status is invalid");
            }
            if (!diskTuple->TestTdStatus(DETACH_TD) && diskTuple->GetTdId() >= heapPage->GetTdCount()) {
                ret = ReportHeapError(report, heapPage, "heap_td_id_invalid", heapPage->GetTdCount() - 1,
                    diskTuple->GetTdId(), "Heap tuple TD id exceeds TD array size");
            }
            if (diskTuple->GetLockerTdId() != INVALID_TD_SLOT && diskTuple->GetLockerTdId() >= heapPage->GetTdCount()) {
                ret = ReportHeapError(report, heapPage, "heap_locker_td_invalid", heapPage->GetTdCount() - 1,
                    diskTuple->GetLockerTdId(), "Heap tuple locker TD id exceeds TD array size");
            }
            if (diskTuple->GetNumColumn() == 0) {
                ret = ReportHeapError(report, heapPage, "heap_num_column_invalid", 1, 0,
                    "Heap tuple column count must be greater than zero");
            }
        } else if (itemId->IsNoStorage()) {
            if (itemId->GetTdId() >= heapPage->GetTdCount()) {
                ret = ReportHeapError(report, heapPage, "heap_redirect_td_invalid", heapPage->GetTdCount() - 1,
                    itemId->GetTdId(), "Heap redirect ItemId points to an invalid TD slot");
            }
            if (!IsHeapTupleTdStatusValid(itemId->GetTdStatus())) {
                ret = ReportHeapError(report, heapPage, "heap_redirect_td_status_invalid", DETACH_TD, itemId->GetTdStatus(),
                    "Heap redirect ItemId has an invalid TD status");
            }
        } else if (itemId->IsRangePlaceholder()) {
            if (itemId->GetLen() == 0) {
                ret = ReportHeapError(report, heapPage, "heap_range_placeholder_len", 1, 0,
                    "Heap range placeholder must preserve non-zero tuple length");
            }
        } else {
            ret = ReportHeapError(report, heapPage, "heap_item_state_invalid", ITEM_ID_UNUSED, itemId->GetFlags(),
                "Heap ItemId state is invalid");
        }
    }

    return ret;
}

}  // namespace

void RegisterHeapPageVerifier()
{
    (void)RegisterPageVerifier(
        PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyHeapPageLightweight, VerifyHeapPageHeavyweight);
}

}  // namespace DSTORE

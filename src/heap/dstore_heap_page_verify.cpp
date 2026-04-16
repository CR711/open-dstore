#include "dfx/dstore_page_verify.h"

#include "page/dstore_heap_page.h"

namespace DSTORE {

namespace {

RetStatus ReportHeapError(VerifyReport *report, const HeapPage *page, const char *checkName, uint64 expected, uint64 actual,
    const char *message, VerifyCode code)
{
    if (report != nullptr) {
        const PageId pageId = (page != nullptr) ? page->GetSelfPageId() : INVALID_PAGE_ID;
        report->AddResultWithCode(
            VerifySeverity::SEVERITY_ERROR, code, "page", pageId, checkName, expected, actual, "%s", message);
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

    /* Heap 页面无 special area，specialOffset 必须等于 BLCKSZ */
    if (heapPage->GetSpecialOffset() != BLCKSZ) {
        return ReportHeapError(report, heapPage, "heap_special_offset_mismatch", BLCKSZ, heapPage->GetSpecialOffset(),
            "Heap page special offset must equal page size", VerifyCode::HEAP_SPECIAL_OFFSET_MISMATCH);
    }

    /* TD count 校验 - LIGHT 级别 */
    if (heapPage->GetTdCount() < MIN_TD_COUNT || heapPage->GetTdCount() > MAX_TD_COUNT) {
        return ReportHeapError(report, heapPage, "heap_td_count_invalid", MIN_TD_COUNT, heapPage->GetTdCount(),
            "Heap page TD count is out of supported range", VerifyCode::HEAP_TD_COUNT_OVERFLOW);
    }

    if (heapPage->GetPotentialDelSize() > BLCKSZ) {
        return ReportHeapError(report, heapPage, "heap_potential_delete_size", BLCKSZ, heapPage->GetPotentialDelSize(),
            "Heap page potential delete size exceeds page size", VerifyCode::PAGE_BOUNDARY_INVALID);
    }

    if (fsmIndex.slot >= FSM_MAX_HWM && fsmIndex.slot != INVALID_FSM_SLOT_NUM) {
        return ReportHeapError(report, heapPage, "heap_fsm_slot_invalid", FSM_MAX_HWM - 1, fsmIndex.slot,
            "Heap page FSM slot is out of range", VerifyCode::HEAP_FSM_SLOT_INVALID);
    }

    if (fsmIndex.page.IsInvalid() && fsmIndex.slot != 0) {
        return ReportHeapError(report, heapPage, "heap_fsm_index_invalid", 0, fsmIndex.slot,
            "Heap page FSM slot must be zero when FSM page id is invalid", VerifyCode::HEAP_FSM_SLOT_INVALID);
    }

    /* dataHeaderSize 精确校验 — Heap 页面必须为 HEAP_PAGE_DATA_OFFSET */
    if (heapPage->DataHeaderSize() != HEAP_PAGE_DATA_OFFSET) {
        return ReportHeapError(report, heapPage, "heap_data_header_invalid",
            HEAP_PAGE_DATA_OFFSET, heapPage->DataHeaderSize(),
            "Heap page data header size does not match expected constant",
            VerifyCode::HEAP_HEADER_OFFSET_INVALID);
    }

    return DSTORE_SUCC;
}

RetStatus VerifyHeapPageMediumweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const HeapPage *heapPage = static_cast<const HeapPage *>(page);

    /* dataHeaderSize 合理性校验 */
    if (heapPage->DataHeaderSize() < HEAP_PAGE_DATA_OFFSET ||
        heapPage->DataHeaderSize() > heapPage->GetLower()) {
        return ReportHeapError(report, heapPage, "heap_data_header_size_invalid",
            HEAP_PAGE_DATA_OFFSET, heapPage->DataHeaderSize(),
            "Heap page data header size is out of valid range", VerifyCode::PAGE_BOUNDARY_INVALID);
    }

    /* ItemId 数组对齐校验 */
    if (heapPage->GetLower() < heapPage->DataHeaderSize() + heapPage->TdDataSize()) {
        return ReportHeapError(report, heapPage, "heap_lower_too_small",
            heapPage->DataHeaderSize() + heapPage->TdDataSize(), heapPage->GetLower(),
            "Heap page lower offset is smaller than header plus TD area",
            VerifyCode::PAGE_BOUNDARY_INVALID);
    }
    uint16 itemIdRegion = heapPage->GetLower() - heapPage->DataHeaderSize() - heapPage->TdDataSize();
    if (itemIdRegion % sizeof(ItemId) != 0) {
        return ReportHeapError(report, heapPage, "heap_itemid_alignment_invalid",
            0, itemIdRegion % sizeof(ItemId),
            "Heap page ItemId array is not aligned to ItemId size",
            VerifyCode::HEAP_ITEMID_ALIGNMENT_INVALID);
    }

    /* O(n) ItemId 偏移量边界扫描（不检查 tuple 内容） */
    for (OffsetNumber offset = FIRST_ITEM_OFFSET_NUMBER; offset <= heapPage->GetMaxOffset(); ++offset) {
        const ItemId *itemId = heapPage->GetItemIdPtr(offset);
        if (itemId->IsNormal()) {
            if (itemId->GetOffset() < heapPage->GetUpper() ||
                itemId->GetOffset() + itemId->GetLen() > heapPage->GetSpecialOffset()) {
                return ReportHeapError(report, heapPage, "heap_item_bounds_invalid",
                    heapPage->GetUpper(), itemId->GetOffset(),
                    "Heap ItemId offset is outside tuple storage area", VerifyCode::PAGE_BOUNDARY_INVALID);
            }
            /* tuple 最小尺寸校验 */
            if (itemId->GetLen() < HEAP_DISK_TUP_HEADER_SIZE) {
                return ReportHeapError(report, heapPage, "heap_tuple_too_small",
                    HEAP_DISK_TUP_HEADER_SIZE, itemId->GetLen(),
                    "Heap tuple size is smaller than the minimum header size",
                    VerifyCode::HEAP_TUPLE_HEADER_SIZE_INVALID);
            }
        }
    }

    return DSTORE_SUCC;
}

RetStatus VerifyHeapPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    /*
     * 注意：此函数使用 ret = ReportHeapError(...) 赋值而非 return，
     * 目的是在发现第一个错误后继续扫描后续 tuple，让 report 收集页面内所有错误。
     * 但返回值仅保留最后一次赋值结果。调用方应通过 report->HasError() 判断校验结果，
     * 不应依赖返回值区分不同错误类型。
     */
    (void)level;
    RetStatus ret = DSTORE_SUCC;
    const HeapPage *heapPage = static_cast<const HeapPage *>(page);
    uint32 previousTupleBegin = heapPage->GetSpecialOffset();

    for (OffsetNumber offset = FIRST_ITEM_OFFSET_NUMBER; offset <= heapPage->GetMaxOffset(); ++offset) {
        const ItemId *itemId = heapPage->GetItemIdPtr(offset);

        if (itemId->IsUnused()) {
            if (itemId->GetLen() != 0) {
                ret = ReportHeapError(report, heapPage, "heap_unused_item_len", 0, itemId->GetLen(),
                    "Unused ItemId must have zero length", VerifyCode::PAGE_BOUNDARY_INVALID);
            }
            continue;
        }

        if (itemId->IsNormal()) {
            const HeapDiskTuple *diskTuple = heapPage->GetDiskTuple(offset);
            if (diskTuple == nullptr) {
                ret = ReportHeapError(report, heapPage, "heap_disk_tuple_null", offset, 0,
                    "GetDiskTuple returned nullptr for a normal ItemId",
                    VerifyCode::HEAP_TUPLE_SIZE_MISMATCH);
                continue;
            }
            if (itemId->GetLen() == 0 || itemId->GetLen() < diskTuple->GetTupleSize()) {
                ret = ReportHeapError(report, heapPage, "heap_item_len_mismatch", diskTuple->GetTupleSize(),
                    itemId->GetLen(), "Heap ItemId length is smaller than tuple size", VerifyCode::HEAP_TUPLE_SIZE_MISMATCH);
            }
            if (itemId->GetOffset() < heapPage->GetUpper() ||
                itemId->GetOffset() + itemId->GetLen() > heapPage->GetSpecialOffset()) {
                ret = ReportHeapError(report, heapPage, "heap_item_offset_invalid", heapPage->GetUpper(),
                    itemId->GetOffset(), "Heap ItemId points outside tuple storage area", VerifyCode::PAGE_BOUNDARY_INVALID);
            }
            if (itemId->GetOffset() + itemId->GetLen() > previousTupleBegin) {
                ret = ReportHeapError(report, heapPage, "heap_tuple_overlap", previousTupleBegin,
                    itemId->GetOffset() + itemId->GetLen(), "Heap tuple storage overlaps with previous tuple",
                    VerifyCode::HEAP_TUPLE_OVERLAP);
            } else {
                previousTupleBegin = itemId->GetOffset();
            }

            const TupleTdStatus tdStatus = diskTuple->GetTdStatus();
            if (!IsHeapTupleTdStatusValid(tdStatus)) {
                ret = ReportHeapError(report, heapPage, "heap_td_status_invalid", DETACH_TD, tdStatus,
                    "Heap tuple TD status is invalid", VerifyCode::HEAP_TD_SANITY_FAIL);
            }
            if (!diskTuple->TestTdStatus(DETACH_TD) && diskTuple->GetTdId() >= heapPage->GetTdCount()) {
                ret = ReportHeapError(report, heapPage, "heap_td_id_invalid", heapPage->GetTdCount() - 1,
                    diskTuple->GetTdId(), "Heap tuple TD id exceeds TD array size", VerifyCode::HEAP_TD_SANITY_FAIL);
            }
            if (diskTuple->GetLockerTdId() != INVALID_TD_SLOT && diskTuple->GetLockerTdId() >= heapPage->GetTdCount()) {
                ret = ReportHeapError(report, heapPage, "heap_locker_td_invalid", heapPage->GetTdCount() - 1,
                    diskTuple->GetLockerTdId(), "Heap tuple locker TD id exceeds TD array size",
                    VerifyCode::HEAP_TD_SANITY_FAIL);
            }
            if (diskTuple->GetNumColumn() == 0) {
                ret = ReportHeapError(report, heapPage, "heap_num_column_invalid", 1, 0,
                    "Heap tuple column count must be greater than zero", VerifyCode::HEAP_TUPLE_NUM_COLUMN_INVALID);
            }
            if (diskTuple->HasExternal() && !diskTuple->HasVariable()) {
                ret = ReportHeapError(report, heapPage, "heap_tuple_flag_inconsistent", 1, 0,
                    "Heap tuple with external storage must also have variable-width flag set",
                    VerifyCode::HEAP_TUPLE_FLAG_INCONSISTENT);
            }
        } else if (itemId->IsNoStorage()) {
            if (itemId->GetTdId() >= heapPage->GetTdCount()) {
                ret = ReportHeapError(report, heapPage, "heap_redirect_td_invalid", heapPage->GetTdCount() - 1,
                    itemId->GetTdId(), "Heap redirect ItemId points to an invalid TD slot", VerifyCode::HEAP_TD_SANITY_FAIL);
            }
            if (!IsHeapTupleTdStatusValid(itemId->GetTdStatus())) {
                ret = ReportHeapError(report, heapPage, "heap_redirect_td_status_invalid", DETACH_TD, itemId->GetTdStatus(),
                    "Heap redirect ItemId has an invalid TD status", VerifyCode::HEAP_TD_SANITY_FAIL);
            }
        } else if (itemId->IsRangePlaceholder()) {
            if (itemId->GetLen() == 0) {
                ret = ReportHeapError(report, heapPage, "heap_range_placeholder_len", 1, 0,
                    "Heap range placeholder must preserve non-zero tuple length", VerifyCode::HEAP_TUPLE_SIZE_MISMATCH);
            }
        } else {
            ret = ReportHeapError(report, heapPage, "heap_item_state_invalid", ITEM_ID_UNUSED, itemId->GetFlags(),
                "Heap ItemId state is invalid", VerifyCode::PAGE_BOUNDARY_INVALID);
        }
    }

    return ret;
}

}  // namespace

void RegisterHeapPageVerifier()
{
    (void)RegisterPageVerifier(
        PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapPageLightweight, VerifyHeapPageMediumweight, VerifyHeapPageHeavyweight);
}

}  // namespace DSTORE

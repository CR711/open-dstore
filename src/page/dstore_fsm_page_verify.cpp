#include "dfx/dstore_page_verify.h"

#include "page/dstore_fsm_page.h"

namespace DSTORE {

namespace {

RetStatus ReportFsmError(VerifyReport *report, const Page *page, const char *checkName, uint64 expected, uint64 actual,
    const char *message)
{
    if (report != nullptr) {
        report->AddResult(VerifySeverity::ERROR_LEVEL, "page", page->GetSelfPageId(), checkName, expected, actual, "%s",
            message);
    }
    return DSTORE_FAIL;
}

bool IsValidFsmSlot(uint16 slot)
{
    return slot < FSM_MAX_HWM || slot == INVALID_FSM_SLOT_NUM;
}

RetStatus VerifyFsmMetaPageLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const FreeSpaceMapMetaPage *fsmMetaPage = static_cast<const FreeSpaceMapMetaPage *>(page);

    if (fsmMetaPage->GetExtendCoefficient() < MIN_FSM_EXTEND_COEFFICIENT ||
        fsmMetaPage->GetExtendCoefficient() > MAX_FSM_EXTEND_COEFFICIENT) {
        return ReportFsmError(report, fsmMetaPage, "fsm_meta_extend_coefficient_invalid", MAX_FSM_EXTEND_COEFFICIENT,
            fsmMetaPage->GetExtendCoefficient(), "FSM meta page extend coefficient is out of range");
    }

    if (fsmMetaPage->GetFsmRootLevel() >= HEAP_MAX_MAP_LEVEL) {
        return ReportFsmError(report, fsmMetaPage, "fsm_meta_level_invalid", HEAP_MAX_MAP_LEVEL - 1,
            fsmMetaPage->GetFsmRootLevel(), "FSM meta page root level exceeds heap map levels");
    }

    return DSTORE_SUCC;
}

RetStatus VerifyFsmMetaPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    RetStatus ret = VerifyFsmMetaPageLightweight(page, level, report);
    const FreeSpaceMapMetaPage *fsmMetaPage = static_cast<const FreeSpaceMapMetaPage *>(page);

    if (fsmMetaPage->GetNumTotalPages() < fsmMetaPage->GetNumUsedPages()) {
        ret = ReportFsmError(report, fsmMetaPage, "fsm_meta_page_count_invalid", fsmMetaPage->GetNumUsedPages(),
            fsmMetaPage->GetNumTotalPages(), "FSM meta page used page count exceeds total page count");
    }

    for (uint16 i = 1; i < FSM_FREE_LIST_COUNT; ++i) {
        if (fsmMetaPage->listRange[i - 1] > fsmMetaPage->listRange[i]) {
            ret = ReportFsmError(report, fsmMetaPage, "fsm_meta_list_range_invalid", fsmMetaPage->listRange[i - 1],
                fsmMetaPage->listRange[i], "FSM meta page listRange array must be non-decreasing");
            break;
        }
    }

    for (uint16 i = 0; i < HEAP_MAX_MAP_LEVEL; ++i) {
        if (fsmMetaPage->mapCount[i] == 0 && fsmMetaPage->currMap[i].IsValid()) {
            ret = ReportFsmError(report, fsmMetaPage, "fsm_meta_curr_map_invalid", 0, fsmMetaPage->mapCount[i],
                "FSM meta page has a current map page without map count");
            break;
        }
    }

    return ret;
}

RetStatus VerifyFsmPageLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const FsmPage *fsmPage = static_cast<const FsmPage *>(page);
    const uint16 expectedSpecialOffset = static_cast<uint16>(BLCKSZ - DstoreRoundUp<Size>(sizeof(FsmSearchSeed),
        sizeof(uint32)));

    if (fsmPage->GetSpecialOffset() != expectedSpecialOffset) {
        return ReportFsmError(report, fsmPage, "fsm_page_special_offset_invalid", expectedSpecialOffset,
            fsmPage->GetSpecialOffset(), "FSM page special offset does not match search-seed area");
    }

    if (fsmPage->fsmPageHeader.hwm > FSM_MAX_HWM) {
        return ReportFsmError(report, fsmPage, "fsm_page_hwm_invalid", FSM_MAX_HWM, fsmPage->fsmPageHeader.hwm,
            "FSM page high water mark exceeds the maximum slot count");
    }

    if (!IsValidFsmSlot(fsmPage->GetUpperSlot())) {
        return ReportFsmError(report, fsmPage, "fsm_page_upper_slot_invalid", FSM_MAX_HWM - 1, fsmPage->GetUpperSlot(),
            "FSM page upper slot is invalid");
    }

    return DSTORE_SUCC;
}

RetStatus VerifyFsmPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    RetStatus ret = VerifyFsmPageLightweight(page, level, report);
    FsmPage *fsmPage = const_cast<FsmPage *>(static_cast<const FsmPage *>(page));
    uint32 totalListCount = 0;

    for (uint16 listId = 0; listId < FSM_FREE_LIST_COUNT; ++listId) {
        const FsmList *fsmList = fsmPage->FsmListPtr(listId);
        uint16 slot = fsmList->first;
        uint16 prev = INVALID_FSM_SLOT_NUM;
        uint16 traversed = 0;

        if (!IsValidFsmSlot(slot)) {
            ret = ReportFsmError(report, fsmPage, "fsm_page_list_head_invalid", FSM_MAX_HWM - 1, slot,
                "FSM page list head points outside the slot array");
            continue;
        }

        while (slot != INVALID_FSM_SLOT_NUM) {
            if (slot >= fsmPage->fsmPageHeader.hwm) {
                ret = ReportFsmError(report, fsmPage, "fsm_page_slot_range_invalid", fsmPage->fsmPageHeader.hwm, slot,
                    "FSM page list references a slot beyond the high water mark");
                break;
            }

            FsmNode *node = fsmPage->FsmNodePtr(slot);
            if (node->listId != listId) {
                ret = ReportFsmError(report, fsmPage, "fsm_page_list_id_mismatch", listId, node->listId,
                    "FSM node list id does not match the owning list");
                break;
            }
            if (node->prev != prev) {
                ret = ReportFsmError(report, fsmPage, "fsm_page_prev_link_invalid", prev, node->prev,
                    "FSM node prev pointer is inconsistent with list traversal");
                break;
            }
            if (!IsValidFsmSlot(node->next)) {
                ret = ReportFsmError(report, fsmPage, "fsm_page_next_link_invalid", FSM_MAX_HWM - 1, node->next,
                    "FSM node next pointer is invalid");
                break;
            }

            prev = slot;
            slot = node->next;
            ++traversed;
            if (traversed > fsmPage->fsmPageHeader.hwm) {
                ret = ReportFsmError(report, fsmPage, "fsm_page_cycle_detected", fsmPage->fsmPageHeader.hwm, traversed,
                    "FSM page list traversal exceeded hwm and likely contains a cycle");
                break;
            }
        }

        if (traversed != fsmList->count) {
            ret = ReportFsmError(report, fsmPage, "fsm_page_list_count_invalid", fsmList->count, traversed,
                "FSM page list count does not match the traversed node count");
        }
        totalListCount += traversed;
    }

    if (totalListCount > fsmPage->fsmPageHeader.hwm) {
        ret = ReportFsmError(report, fsmPage, "fsm_page_total_count_invalid", fsmPage->fsmPageHeader.hwm, totalListCount,
            "FSM page list counts exceed the number of allocated slots");
    }

    return ret;
}

}  // namespace

void RegisterFsmPageVerifiers()
{
    (void)RegisterPageVerifier(
        PageType::FSM_PAGE_TYPE, "FsmPage", VerifyFsmPageLightweight, VerifyFsmPageHeavyweight);
    (void)RegisterPageVerifier(
        PageType::FSM_META_PAGE_TYPE, "FsmMetaPage", VerifyFsmMetaPageLightweight, VerifyFsmMetaPageHeavyweight);
}

}  // namespace DSTORE

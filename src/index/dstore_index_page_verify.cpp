#include "dfx/dstore_page_verify.h"

#include "page/dstore_index_page.h"

namespace DSTORE {

namespace {

RetStatus ReportIndexError(VerifyReport *report, const BtrPage *page, const char *checkName, uint64 expected, uint64 actual,
    const char *message)
{
    if (report != nullptr) {
        report->AddResult(VerifySeverity::ERROR_LEVEL, "page", page->GetSelfPageId(), checkName, expected, actual, "%s",
            message);
    }
    return DSTORE_FAIL;
}

bool IsValidBtrPageType(uint16 type)
{
    return type > static_cast<uint16>(BtrPageType::INVALID_BTR_PAGE) &&
           type <= static_cast<uint16>(BtrPageType::META_PAGE);
}

RetStatus VerifyIndexPageLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const BtrPage *btrPage = static_cast<const BtrPage *>(page);
    const BtrPageLinkAndStatus *link = const_cast<BtrPage *>(btrPage)->GetLinkAndStatus();
    const uint16 expectedSpecialOffset = static_cast<uint16>(BLCKSZ - MAXALIGN(sizeof(BtrPageLinkAndStatus)));

    if (btrPage->GetSpecialOffset() != expectedSpecialOffset) {
        return ReportIndexError(report, btrPage, "index_special_offset_invalid", expectedSpecialOffset,
            btrPage->GetSpecialOffset(), "Index page special offset does not match link-status area size");
    }

    if (!IsValidBtrPageType(link->GetType())) {
        return ReportIndexError(report, btrPage, "index_page_type_invalid", static_cast<uint16>(BtrPageType::META_PAGE),
            link->GetType(), "Index page btree subtype is invalid");
    }

    if (btrPage->IsDamaged()) {
        return ReportIndexError(report, btrPage, "index_page_damaged", 0, 1,
            "Index page layout is marked as damaged");
    }

    return DSTORE_SUCC;
}

RetStatus VerifyIndexPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    RetStatus ret = VerifyIndexPageLightweight(page, level, report);
    BtrPage *btrPage = const_cast<BtrPage *>(static_cast<const BtrPage *>(page));
    const BtrPageLinkAndStatus *link = btrPage->GetLinkAndStatus();
    const OffsetNumber firstDataOffset = link->GetFirstDataOffset();
    uint32 previousTupleBegin = btrPage->GetSpecialOffset();

    if (!btrPage->IsInitialized()) {
        ret = ReportIndexError(report, btrPage, "index_page_uninitialized", 1, 0,
            "Index page metadata is not initialized");
    }

    if (link->TestType(BtrPageType::META_PAGE) && btrPage->GetTdCount() != 0) {
        ret = ReportIndexError(report, btrPage, "index_meta_td_count", 0, btrPage->GetTdCount(),
            "Btree meta page should not allocate TD slots");
    }

    if (!link->TestType(BtrPageType::META_PAGE) && btrPage->GetBtrMetaPageId().IsInvalid()) {
        ret = ReportIndexError(report, btrPage, "index_meta_page_invalid", 1, 0,
            "Index page must reference a valid btree meta page");
    }

    if (btrPage->GetMaxOffset() != 0 && firstDataOffset > btrPage->GetMaxOffset()) {
        ret = ReportIndexError(report, btrPage, "index_first_data_offset_invalid", btrPage->GetMaxOffset(), firstDataOffset,
            "Index page first data offset exceeds max offset");
    }

    if (!link->IsRightmost()) {
        const ItemId *highKeyItem = const_cast<BtrPage *>(btrPage)->GetItemIdPtr(BTREE_PAGE_HIKEY);
        if (!highKeyItem->IsNormal()) {
            ret = ReportIndexError(report, btrPage, "index_high_key_invalid", ITEM_ID_NORMAL, highKeyItem->GetFlags(),
                "Non-rightmost index page must keep a normal high key tuple");
        }
    }

    for (OffsetNumber offset = BTREE_PAGE_HIKEY; offset <= btrPage->GetMaxOffset(); ++offset) {
        const ItemId *itemId = const_cast<BtrPage *>(btrPage)->GetItemIdPtr(offset);

        if (itemId->IsUnused()) {
            if (offset >= firstDataOffset) {
                ret = ReportIndexError(report, btrPage, "index_unused_data_item", ITEM_ID_NORMAL, ITEM_ID_UNUSED,
                    "Index data region must not contain unused ItemIds");
            }
            continue;
        }

        if (!itemId->IsNormal()) {
            ret = ReportIndexError(report, btrPage, "index_item_state_invalid", ITEM_ID_NORMAL, itemId->GetFlags(),
                "Index ItemId must be normal in active tuple area");
            continue;
        }

        const IndexTuple *tuple = const_cast<BtrPage *>(btrPage)->GetIndexTuple(offset);
        if (itemId->GetLen() == 0 || itemId->GetLen() < tuple->GetSize()) {
            ret = ReportIndexError(report, btrPage, "index_item_len_mismatch", tuple->GetSize(), itemId->GetLen(),
                "Index ItemId length is smaller than tuple size");
        }
        if (itemId->GetOffset() < btrPage->GetUpper() ||
            itemId->GetOffset() + itemId->GetLen() > btrPage->GetSpecialOffset()) {
            ret = ReportIndexError(report, btrPage, "index_item_offset_invalid", btrPage->GetUpper(), itemId->GetOffset(),
                "Index ItemId points outside tuple storage area");
        }
        if (itemId->GetOffset() + itemId->GetLen() > previousTupleBegin) {
            ret = ReportIndexError(report, btrPage, "index_tuple_overlap", previousTupleBegin,
                itemId->GetOffset() + itemId->GetLen(), "Index tuple storage overlaps with previous tuple");
        } else {
            previousTupleBegin = itemId->GetOffset();
        }

        if (!tuple->TestTdStatus(DETACH_TD) && tuple->GetTdId() >= btrPage->GetTdCount() &&
            !link->TestType(BtrPageType::META_PAGE)) {
            ret = ReportIndexError(report, btrPage, "index_td_id_invalid", btrPage->GetTdCount() == 0 ? 0 : btrPage->GetTdCount() - 1,
                tuple->GetTdId(), "Index tuple TD id exceeds TD array size");
        }
    }

    return ret;
}

}  // namespace

void RegisterIndexPageVerifier()
{
    (void)RegisterPageVerifier(
        PageType::INDEX_PAGE_TYPE, "IndexPage", VerifyIndexPageLightweight, VerifyIndexPageHeavyweight);
}

}  // namespace DSTORE

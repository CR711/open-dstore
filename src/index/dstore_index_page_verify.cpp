#include "dfx/dstore_page_verify.h"

#include <algorithm>
#include <cstring>

#include "page/dstore_index_page.h"

namespace DSTORE {

namespace {

RetStatus ReportIndexError(VerifyReport *report, const BtrPage *page, const char *checkName, uint64 expected, uint64 actual,
    const char *message, VerifyCode code)
{
    if (report != nullptr) {
        const PageId pageId = (page != nullptr) ? page->GetSelfPageId() : INVALID_PAGE_ID;
        report->AddResultWithCode(
            VerifySeverity::SEVERITY_ERROR, code, "page", pageId, checkName, expected, actual, "%s", message);
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
    const BtrPageLinkAndStatus *link = btrPage->GetLinkAndStatus();
    const uint16 expectedSpecialOffset = static_cast<uint16>(BLCKSZ - MAXALIGN(sizeof(BtrPageLinkAndStatus)));

    if (btrPage->GetSpecialOffset() != expectedSpecialOffset) {
        return ReportIndexError(report, btrPage, "index_special_offset_invalid", expectedSpecialOffset,
            btrPage->GetSpecialOffset(), "Index page special offset does not match link-status area size",
            VerifyCode::BTR_SPECIAL_OFFSET_INVALID);
    }

    if (!IsValidBtrPageType(link->GetType())) {
        return ReportIndexError(report, btrPage, "index_page_type_invalid", static_cast<uint16>(BtrPageType::META_PAGE),
            link->GetType(), "Index page btree subtype is invalid", VerifyCode::BTR_PAGE_TYPE_INVALID);
    }

    if (btrPage->IsDamaged()) {
        return ReportIndexError(report, btrPage, "index_page_damaged", 0, 1,
            "Index page layout is marked as damaged", VerifyCode::PAGE_BOUNDARY_INVALID);
    }

    /* splitStat / liveStat 枚举范围校验 */
    const uint16 liveStat = link->GetLiveStatus();
    const uint16 splitStatMax = static_cast<uint16>(BtrPageSplitStatus::SPLIT_INCOMPLETE);
    const uint16 liveStatMax = static_cast<uint16>(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB);
    if (liveStat > liveStatMax) {
        return ReportIndexError(report, btrPage, "index_live_stat_invalid", liveStatMax, liveStat,
            "Index page live status is out of valid enum range", VerifyCode::BTR_SPLIT_STAT_INVALID);
    }
    if (!link->IsSplitComplete() &&
        link->status.bitVal.splitStat > splitStatMax) {
        return ReportIndexError(report, btrPage, "index_split_stat_invalid", splitStatMax,
            link->status.bitVal.splitStat,
            "Index page split status is out of valid enum range", VerifyCode::BTR_SPLIT_STAT_INVALID);
    }

    return DSTORE_SUCC;
}

RetStatus VerifyIndexPageMediumweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    const BtrPage *btrPage = static_cast<const BtrPage *>(page);
    const BtrPageLinkAndStatus *link = btrPage->GetLinkAndStatus();

    /* level vs type 一致性校验 */
    if (link->TestType(BtrPageType::LEAF_PAGE) && link->GetLevel() != 0) {
        return ReportIndexError(report, btrPage, "index_leaf_level_invalid", 0,
            link->GetLevel(), "Leaf page must have level 0",
            VerifyCode::PAGE_BOUNDARY_INVALID);
    }
    if (link->TestType(BtrPageType::INTERNAL_PAGE) && link->GetLevel() == 0) {
        return ReportIndexError(report, btrPage, "index_internal_level_invalid", 1,
            link->GetLevel(), "Internal page must have level > 0",
            VerifyCode::PAGE_BOUNDARY_INVALID);
    }

    /* sibling 自引用检测 */
    const PageId selfId = btrPage->GetSelfPageId();
    if (link->GetRight() == selfId) {
        return ReportIndexError(report, btrPage, "index_right_self_reference", 0, 1,
            "Index page right sibling must not point to itself",
            VerifyCode::PAGE_ID_INVALID);
    }
    if (link->GetLeft() == selfId) {
        return ReportIndexError(report, btrPage, "index_left_self_reference", 0, 1,
            "Index page left sibling must not point to itself",
            VerifyCode::PAGE_ID_INVALID);
    }

    /* O(n) ItemId 偏移量边界扫描 */
    for (OffsetNumber offset = BTREE_PAGE_HIKEY; offset <= btrPage->GetMaxOffset(); ++offset) {
        const ItemId *itemId = btrPage->GetItemIdPtr(offset);
        if (itemId->IsNormal()) {
            if (itemId->GetOffset() < btrPage->GetUpper() ||
                itemId->GetOffset() + itemId->GetLen() > btrPage->GetSpecialOffset()) {
                return ReportIndexError(report, btrPage, "index_item_bounds_invalid",
                    btrPage->GetUpper(), itemId->GetOffset(),
                    "Index ItemId offset is outside tuple storage area",
                    VerifyCode::PAGE_BOUNDARY_INVALID);
            }
            if (itemId->GetOffset() != MAXALIGN(itemId->GetOffset())) {
                return ReportIndexError(report, btrPage, "index_itemid_offset_misaligned",
                    MAXALIGN(itemId->GetOffset()), itemId->GetOffset(),
                    "Index ItemId offset is not MAXALIGN'd",
                    VerifyCode::INDEX_ITEMID_OFFSET_MISALIGNED);
            }
        }
    }

    return DSTORE_SUCC;
}

RetStatus VerifyIndexPageHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    /*
     * 注意：此函数使用 ret = ReportIndexError(...) 赋值而非 return，
     * 目的是在发现第一个错误后继续扫描后续 tuple，让 report 收集页面内所有错误。
     * 但返回值仅保留最后一次赋值结果。调用方应通过 report->HasError() 判断校验结果，
     * 不应依赖返回值区分不同错误类型。
     */
    (void)level;
    RetStatus ret = DSTORE_SUCC;
    const BtrPage *btrPage = static_cast<const BtrPage *>(page);
    const BtrPageLinkAndStatus *link = btrPage->GetLinkAndStatus();
    const OffsetNumber firstDataOffset = link->GetFirstDataOffset();
    uint32 previousTupleBegin = btrPage->GetSpecialOffset();
    const IndexTuple *prevTuple = nullptr;
    uint16 prevKeyLen = 0;

    /* IsInitialized: specialOffset matches link-status size && btrMetaPageId valid */
    const uint16 expectedSpecial = static_cast<uint16>(BLCKSZ - MAXALIGN(sizeof(BtrPageLinkAndStatus)));
    if (btrPage->GetSpecialOffset() != expectedSpecial || !link->btrMetaPageId.IsValid()) {
        ret = ReportIndexError(report, btrPage, "index_page_uninitialized", 1, 0,
            "Index page metadata is not initialized", VerifyCode::PAGE_BOUNDARY_INVALID);
    }

    if (link->TestType(BtrPageType::META_PAGE) && btrPage->GetTdCount() != 0) {
        ret = ReportIndexError(report, btrPage, "index_meta_td_count", 0, btrPage->GetTdCount(),
            "Btree meta page should not allocate TD slots", VerifyCode::PAGE_BOUNDARY_INVALID);
    }

    if (!link->TestType(BtrPageType::META_PAGE) && !link->btrMetaPageId.IsValid()) {
        ret = ReportIndexError(report, btrPage, "index_meta_page_invalid", 1, 0,
            "Index page must reference a valid btree meta page", VerifyCode::BTR_META_PAGE_ID_INVALID);
    }

    if (btrPage->GetMaxOffset() != 0 && firstDataOffset > btrPage->GetMaxOffset()) {
        ret = ReportIndexError(report, btrPage, "index_first_data_offset_invalid", btrPage->GetMaxOffset(), firstDataOffset,
            "Index page first data offset exceeds max offset", VerifyCode::PAGE_BOUNDARY_INVALID);
    }

    if (!link->IsRightmost()) {
        const ItemId *highKeyItem = btrPage->GetItemIdPtr(BTREE_PAGE_HIKEY);
        if (!highKeyItem->IsNormal()) {
            ret = ReportIndexError(report, btrPage, "index_high_key_invalid", ITEM_ID_NORMAL, highKeyItem->GetFlags(),
                "Non-rightmost index page must keep a normal high key tuple", VerifyCode::PAGE_BOUNDARY_INVALID);
        }
    }

    for (OffsetNumber offset = BTREE_PAGE_HIKEY; offset <= btrPage->GetMaxOffset(); ++offset) {
        const ItemId *itemId = btrPage->GetItemIdPtr(offset);

        if (itemId->IsUnused()) {
            if (offset >= firstDataOffset) {
                ret = ReportIndexError(report, btrPage, "index_unused_data_item", ITEM_ID_NORMAL, ITEM_ID_UNUSED,
                    "Index data region must not contain unused ItemIds", VerifyCode::PAGE_BOUNDARY_INVALID);
            }
            continue;
        }

        if (!itemId->IsNormal()) {
            ret = ReportIndexError(report, btrPage, "index_item_state_invalid", ITEM_ID_NORMAL, itemId->GetFlags(),
                "Index ItemId must be normal in active tuple area", VerifyCode::PAGE_BOUNDARY_INVALID);
            continue;
        }

        const IndexTuple *tuple = btrPage->GetIndexTuple(offset);
        if (itemId->GetLen() == 0 || itemId->GetLen() < tuple->GetSize()) {
            ret = ReportIndexError(report, btrPage, "index_item_len_mismatch", tuple->GetSize(), itemId->GetLen(),
                "Index ItemId length is smaller than tuple size", VerifyCode::INDEX_TUPLE_SIZE_MISMATCH);
        }
        if (itemId->GetOffset() < btrPage->GetUpper() ||
            itemId->GetOffset() + itemId->GetLen() > btrPage->GetSpecialOffset()) {
            ret = ReportIndexError(report, btrPage, "index_item_offset_invalid", btrPage->GetUpper(), itemId->GetOffset(),
                "Index ItemId points outside tuple storage area", VerifyCode::PAGE_BOUNDARY_INVALID);
        }
        if (itemId->GetOffset() + itemId->GetLen() > previousTupleBegin) {
            ret = ReportIndexError(report, btrPage, "index_tuple_overlap", previousTupleBegin,
                itemId->GetOffset() + itemId->GetLen(), "Index tuple storage overlaps with previous tuple",
                VerifyCode::PAGE_BOUNDARY_INVALID);
        } else {
            previousTupleBegin = itemId->GetOffset();
        }

        if (!tuple->TestTdStatus(DETACH_TD) && tuple->GetTdId() >= btrPage->GetTdCount() &&
            !link->TestType(BtrPageType::META_PAGE)) {
            ret = ReportIndexError(report, btrPage, "index_td_id_invalid", btrPage->GetTdCount() == 0 ? 0 : btrPage->GetTdCount() - 1,
                tuple->GetTdId(), "Index tuple TD id exceeds TD array size", VerifyCode::PAGE_BOUNDARY_INVALID);
        }

        /* Intra-page key ordering check (heuristic: raw byte comparison) */
        if (offset >= firstDataOffset && !link->TestType(BtrPageType::META_PAGE)) {
            uint16 curKeyLen = 0;
            const char *curKeyData = nullptr;
            if (tuple->GetSize() > INDEX_TUPLE_SIZE) {
                curKeyLen = tuple->GetSize() - INDEX_TUPLE_SIZE;
                curKeyData = reinterpret_cast<const char *>(tuple) + INDEX_TUPLE_SIZE;
            }
            if (prevTuple != nullptr && prevKeyLen > 0 && curKeyLen > 0) {
                uint16 cmpLen = std::min(prevKeyLen, curKeyLen);
                const char *prevKeyData = reinterpret_cast<const char *>(prevTuple) + INDEX_TUPLE_SIZE;
                int cmp = memcmp(prevKeyData, curKeyData, cmpLen);
                if (cmp > 0 || (cmp == 0 && prevKeyLen > curKeyLen)) {
                    ret = ReportIndexError(report, btrPage, "index_key_order_invalid",
                        offset - 1, offset, "Index tuples are not in sorted key order within page",
                        VerifyCode::PAGE_BOUNDARY_INVALID);
                }
            }
            prevTuple = tuple;
            prevKeyLen = curKeyLen;
        }
    }

    return ret;
}

}  // namespace

void RegisterIndexPageVerifier()
{
    (void)RegisterPageVerifier(
        PageType::INDEX_PAGE_TYPE, "IndexPage", VerifyModule::INDEX,
        VerifyIndexPageLightweight, VerifyIndexPageMediumweight, VerifyIndexPageHeavyweight);
}

}  // namespace DSTORE

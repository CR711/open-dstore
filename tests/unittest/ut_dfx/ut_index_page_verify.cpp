#include <array>
#include <gtest/gtest.h>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_index_page.h"

using namespace DSTORE;

namespace {

using PageBuffer = std::array<unsigned char, BLCKSZ>;

BtrPage *InitIndexPage(PageBuffer &buffer, PageId pageId)
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

}  // namespace

TEST(UTIndexPageVerify, ValidIndexPagePasses)
{
    PageBuffer pageBuffer{};
    BtrPage *page = InitIndexPage(pageBuffer, {30, 40});
    VerifyReport report;

    RegisterIndexPageVerifier();
    AddIndexTuple(page, BTREE_PAGE_HIKEY, 24, 0);
    page->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVYWEIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST(UTIndexPageVerify, InvalidSpecialOffsetFails)
{
    PageBuffer pageBuffer{};
    BtrPage *page = InitIndexPage(pageBuffer, {31, 41});
    VerifyReport report;

    RegisterIndexPageVerifier();
    page->SetSpecialOffset(static_cast<uint16>(page->GetSpecialOffset() - 8));
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHTWEIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTIndexPageVerify, MissingHighKeyFails)
{
    PageBuffer pageBuffer{};
    BtrPage *page = InitIndexPage(pageBuffer, {32, 42});
    VerifyReport report;

    RegisterIndexPageVerifier();
    page->GetLinkAndStatus()->SetRight({2, 2});
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVYWEIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

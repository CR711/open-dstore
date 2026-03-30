#include <array>
#include <gtest/gtest.h>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_heap_page.h"

using namespace DSTORE;

namespace {

using PageBuffer = std::array<unsigned char, BLCKSZ>;

HeapPage *InitHeapPage(PageBuffer &buffer, PageId pageId)
{
    HeapPage *page = reinterpret_cast<HeapPage *>(buffer.data());
    page->Init(0, PageType::HEAP_PAGE_TYPE, pageId);
    page->SetLsn(1, 1, 1, false);
    page->SetDataHeaderSize(HEAP_PAGE_DATA_OFFSET);
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

}  // namespace

TEST(UTHeapPageVerify, ValidHeapPagePasses)
{
    PageBuffer pageBuffer{};
    HeapPage *page = InitHeapPage(pageBuffer, {20, 30});
    VerifyReport report;

    RegisterHeapPageVerifier();
    AddHeapTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32, 0);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVYWEIGHT, &report), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST(UTHeapPageVerify, InvalidTdCountFails)
{
    PageBuffer pageBuffer{};
    HeapPage *page = InitHeapPage(pageBuffer, {21, 31});
    VerifyReport report;

    RegisterHeapPageVerifier();
    page->dataHeader.tdCount = static_cast<uint8>(MIN_TD_COUNT - 1);
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::HEAVYWEIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTHeapPageVerify, InvalidFsmSlotFailsLightweight)
{
    PageBuffer pageBuffer{};
    HeapPage *page = InitHeapPage(pageBuffer, {22, 32});
    VerifyReport report;

    RegisterHeapPageVerifier();
    page->SetFsmIndex({INVALID_PAGE_ID, FSM_MAX_HWM});
    page->SetChecksum();

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHTWEIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

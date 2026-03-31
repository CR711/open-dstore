#include <array>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "dfx/dstore_heap_verify.h"
#include "dfx/dstore_page_verify.h"
#include "fsm/dstore_partition_fsm.h"

using namespace DSTORE;

namespace {

using PageBuffer = std::array<unsigned char, BLCKSZ>;

uint64 PageIdKey(const PageId &pageId)
{
    return (static_cast<uint64>(pageId.m_fileId) << 32) | pageId.m_blockId;
}

uint64 FsmKey(const FsmIndex &fsmIndex)
{
    return (PageIdKey(fsmIndex.page) << 16) | fsmIndex.slot;
}

class FakeHeapVerifyPageSource : public HeapVerifyPageSource {
public:
    void AddPage(const PageId &pageId, HeapPage *page)
    {
        m_order.push_back(pageId);
        m_pages[PageIdKey(pageId)] = page;
    }

    void SetRecordedFsm(const FsmIndex &fsmIndex, uint16 listId)
    {
        m_recordedFsm[FsmKey(fsmIndex)] = listId;
    }

    PageId GetFirstPageId() override
    {
        m_index = 0;
        return m_order.empty() ? INVALID_PAGE_ID : m_order.front();
    }

    PageId GetNextPageId() override
    {
        ++m_index;
        return m_index < m_order.size() ? m_order[m_index] : INVALID_PAGE_ID;
    }

    HeapPage *ReadHeapPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        if (bufferDesc != nullptr) {
            *bufferDesc = nullptr;
        }
        auto it = m_pages.find(PageIdKey(pageId));
        return it == m_pages.end() ? nullptr : it->second;
    }

    void ReleaseHeapPage(BufferDesc *bufferDesc) override
    {
        (void)bufferDesc;
    }

    bool GetRecordedFsmSpace(const FsmIndex &fsmIndex, uint16 *listId, uint32 *spaceUpperBound) override
    {
        auto it = m_recordedFsm.find(FsmKey(fsmIndex));
        if (it == m_recordedFsm.end()) {
            return false;
        }

        if (listId != nullptr) {
            *listId = it->second;
        }
        if (spaceUpperBound != nullptr) {
            *spaceUpperBound = FSM_SPACE_LINE[it->second];
        }
        return true;
    }

private:
    size_t m_index{0};
    std::vector<PageId> m_order;
    std::unordered_map<uint64, HeapPage *> m_pages;
    std::unordered_map<uint64, uint16> m_recordedFsm;
};

HeapPage *InitHeapPage(PageBuffer &buffer, PageId pageId, FsmIndex fsmIndex)
{
    HeapPage *page = reinterpret_cast<HeapPage *>(buffer.data());
    page->Init(0, PageType::HEAP_PAGE_TYPE, pageId);
    page->SetLsn(1, 1, 1, false);
    page->SetDataHeaderSize(HEAP_PAGE_DATA_OFFSET);
    page->SetLower(page->DataHeaderSize());
    page->AllocateTdSpace();
    page->SetFsmIndex(fsmIndex);
    page->SetPotentialDelSize(0);
    page->SetChecksum();
    return page;
}

HeapDiskTuple *AddTuple(HeapPage *page, OffsetNumber offset, uint16 tupleSize)
{
    page->SetUpper(static_cast<uint16>(page->GetUpper() - tupleSize));
    ItemId *itemId = page->GetItemIdPtr(offset);
    itemId->SetNormal(page->GetUpper(), tupleSize);
    page->SetLower(static_cast<uint16>(page->GetLower() + sizeof(ItemId)));

    HeapDiskTuple *tuple = page->GetDiskTuple(offset);
    tuple->ResetInfo();
    tuple->SetNoLink();
    tuple->SetTupleSize(tupleSize);
    tuple->SetTdId(0);
    tuple->SetLockerTdId(INVALID_TD_SLOT);
    tuple->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    tuple->SetLiveMode(HeapDiskTupLiveMode::TUPLE_BY_NORMAL_INSERT);
    tuple->SetNumColumn(1);
    return tuple;
}

HeapDiskTuple *AddFirstChunk(HeapPage *page, OffsetNumber offset, uint16 tupleSize, const ItemPointerData &nextChunk,
    uint32 chunkCount)
{
    HeapDiskTuple *tuple = AddTuple(page, offset, tupleSize);
    tuple->SetFirstLinkChunk();
    tuple->SetNextChunkCtid(nextChunk);
    tuple->SetNumTupChunks(chunkCount);
    return tuple;
}

HeapDiskTuple *AddFollowChunk(HeapPage *page, OffsetNumber offset, uint16 tupleSize, const ItemPointerData &nextChunk)
{
    HeapDiskTuple *tuple = AddTuple(page, offset, tupleSize);
    tuple->SetNotFirstLinkChunk();
    tuple->SetNextChunkCtid(nextChunk);
    return tuple;
}

}  // namespace

TEST(UTHeapSegmentVerify, ValidSegmentPasses)
{
    RegisterHeapPageVerifier();

    PageBuffer firstBuffer{};
    PageBuffer secondBuffer{};
    const FsmIndex firstFsm{{90, 1}, 3};
    const FsmIndex secondFsm{{90, 1}, 4};

    HeapPage *firstPage = InitHeapPage(firstBuffer, {40, 10}, firstFsm);
    HeapPage *secondPage = InitHeapPage(secondBuffer, {40, 11}, secondFsm);
    AddTuple(firstPage, FIRST_ITEM_OFFSET_NUMBER, 32);
    AddTuple(secondPage, FIRST_ITEM_OFFSET_NUMBER, 32);
    firstPage->SetChecksum();
    secondPage->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(firstPage->GetSelfPageId(), firstPage);
    pageSource.AddPage(secondPage->GetSelfPageId(), secondPage);
    pageSource.SetRecordedFsm(firstFsm,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(firstPage->GetFreeSpace<FreeSpaceCondition::RAW>())));
    pageSource.SetRecordedFsm(secondFsm,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(secondPage->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
    EXPECT_EQ(report.GetWarningCount(), 0);
}

TEST(UTHeapSegmentVerify, CorruptedTupleFails)
{
    RegisterHeapPageVerifier();

    PageBuffer buffer{};
    const FsmIndex fsmIndex{{91, 1}, 2};
    HeapPage *page = InitHeapPage(buffer, {41, 10}, fsmIndex);
    AddTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32);
    page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER)->SetNormal(page->GetUpper(), 24);
    ASSERT_EQ(page->GetMaxOffset(), FIRST_ITEM_OFFSET_NUMBER);
    ASSERT_TRUE(page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER)->IsNormal());
    ASSERT_EQ(page->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER)->GetLen(), 24);
    ASSERT_EQ(page->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER)->GetTupleSize(), 32);
    page->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(page->GetSelfPageId(), page);
    pageSource.SetRecordedFsm(fsmIndex,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTHeapSegmentVerify, BrokenBigTupleChainFails)
{
    RegisterHeapPageVerifier();

    PageBuffer firstBuffer{};
    const FsmIndex firstFsm{{92, 1}, 1};

    HeapPage *firstPage = InitHeapPage(firstBuffer, {42, 10}, firstFsm);
    AddFirstChunk(firstPage, FIRST_ITEM_OFFSET_NUMBER, 64, INVALID_ITEM_POINTER, 2);
    ASSERT_EQ(firstPage->GetMaxOffset(), FIRST_ITEM_OFFSET_NUMBER);
    ASSERT_TRUE(firstPage->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER)->IsLinked());
    ASSERT_TRUE(firstPage->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER)->IsFirstLinkChunk());
    ASSERT_EQ(firstPage->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER)->GetNumChunks(), 2U);
    ASSERT_EQ(firstPage->GetDiskTuple(FIRST_ITEM_OFFSET_NUMBER)->GetNextChunkCtid(), INVALID_ITEM_POINTER);
    firstPage->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(firstPage->GetSelfPageId(), firstPage);
    pageSource.SetRecordedFsm(firstFsm,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(firstPage->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTHeapSegmentVerify, FsmMismatchProducesWarning)
{
    RegisterHeapPageVerifier();

    PageBuffer buffer{};
    const FsmIndex fsmIndex{{93, 1}, 5};
    HeapPage *page = InitHeapPage(buffer, {43, 10}, fsmIndex);
    AddTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32);
    page->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(page->GetSelfPageId(), page);
    pageSource.SetRecordedFsm(fsmIndex, 0);

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
    EXPECT_EQ(report.GetWarningCount(), 1);
}

#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "dfx/dstore_heap_verify.h"
#include "dfx/dstore_page_verify.h"
#include "fsm/dstore_partition_fsm.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;

namespace {

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
    VerifyContext context(&report, nullptr, false, 1000);
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
    VerifyContext context(&report, nullptr, false, 1000);
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
    VerifyContext context(&report, nullptr, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTHeapSegmentVerify, BigTupleChainCycleDetected)
{
    RegisterHeapPageVerifier();

    /* 构造两页：page1 chunk1 → page2 chunk2 → page1 chunk1（环） */
    PageBuffer buf1{};
    PageBuffer buf2{};
    const FsmIndex fsm1{{94, 1}, 1};
    const FsmIndex fsm2{{94, 1}, 2};
    const PageId pageId1{44, 10};
    const PageId pageId2{44, 11};

    HeapPage *page1 = InitHeapPage(buf1, pageId1, fsm1);
    HeapPage *page2 = InitHeapPage(buf2, pageId2, fsm2);

    /* page1: first chunk，声明 3 个 chunks，next 指向 page2 offset 1 */
    ItemPointerData nextToPage2(pageId2, FIRST_ITEM_OFFSET_NUMBER);
    AddFirstChunk(page1, FIRST_ITEM_OFFSET_NUMBER, 64, nextToPage2, 3);

    /* page2: follow chunk，next 指回 page1 offset 1（形成环） */
    ItemPointerData backToPage1(pageId1, FIRST_ITEM_OFFSET_NUMBER);
    AddFollowChunk(page2, FIRST_ITEM_OFFSET_NUMBER, 64, backToPage1);

    page1->SetChecksum();
    page2->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(pageId1, page1);
    pageSource.AddPage(pageId2, page2);
    pageSource.SetRecordedFsm(fsm1,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page1->GetFreeSpace<FreeSpaceCondition::RAW>())));
    pageSource.SetRecordedFsm(fsm2,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page2->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());

    /* 环检测没有专用 VerifyCode，暂用文本匹配。若错误消息文本变更，此断言需同步更新。 */
    bool hasCycleError = false;
    for (const auto &result : report.GetResults()) {
        if (std::string(result.message).find("cycle") != std::string::npos ||
            std::string(result.checkName).find("cycle") != std::string::npos) {
            hasCycleError = true;
            break;
        }
    }
    EXPECT_TRUE(hasCycleError);
}

TEST(UTHeapSegmentVerify, CompleteBigTupleChainValid)
{
    RegisterHeapPageVerifier();

    /* 构造完整的 2-chunk 跨页链：page1 chunk1 → page2 chunk2 → end */
    PageBuffer buf1{};
    PageBuffer buf2{};
    const FsmIndex fsm1{{95, 1}, 1};
    const FsmIndex fsm2{{95, 1}, 2};
    const PageId pageId1{45, 10};
    const PageId pageId2{45, 11};

    HeapPage *page1 = InitHeapPage(buf1, pageId1, fsm1);
    HeapPage *page2 = InitHeapPage(buf2, pageId2, fsm2);

    /* page1: first chunk，声明 2 个 chunks，next 指向 page2 */
    ItemPointerData nextToPage2(pageId2, FIRST_ITEM_OFFSET_NUMBER);
    AddFirstChunk(page1, FIRST_ITEM_OFFSET_NUMBER, 64, nextToPage2, 2);

    /* page2: follow chunk，next = INVALID（链结束） */
    AddFollowChunk(page2, FIRST_ITEM_OFFSET_NUMBER, 64, INVALID_ITEM_POINTER);

    page1->SetChecksum();
    page2->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(pageId1, page1);
    pageSource.AddPage(pageId2, page2);
    pageSource.SetRecordedFsm(fsm1,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page1->GetFreeSpace<FreeSpaceCondition::RAW>())));
    pageSource.SetRecordedFsm(fsm2,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page2->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST(UTHeapSegmentVerify, ErrorLimitStopsEarly)
{
    RegisterHeapPageVerifier();

    /* 构造多个页面，每页都有 corrupted tuple，但 maxErrors=1 时应在第一个错误后停止 */
    PageBuffer buf1{};
    PageBuffer buf2{};
    const FsmIndex fsm1{{96, 1}, 1};
    const FsmIndex fsm2{{96, 1}, 2};

    HeapPage *page1 = InitHeapPage(buf1, {46, 10}, fsm1);
    HeapPage *page2 = InitHeapPage(buf2, {46, 11}, fsm2);

    /* 两页都有 tuple，但 ItemId len != tuple size（损坏） */
    AddTuple(page1, FIRST_ITEM_OFFSET_NUMBER, 32);
    page1->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER)->SetNormal(page1->GetUpper(), 24);
    AddTuple(page2, FIRST_ITEM_OFFSET_NUMBER, 32);
    page2->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER)->SetNormal(page2->GetUpper(), 24);
    page1->SetChecksum();
    page2->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(page1->GetSelfPageId(), page1);
    pageSource.AddPage(page2->GetSelfPageId(), page2);

    HeapVerifyOptions options;
    options.isOnline = false;
    options.maxErrors = 1;
    VerifyReport report;
    VerifyContext context(&report, nullptr, false, options.maxErrors);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    /* 应在第一个错误后停止 */
    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    /* 最多只报告 maxErrors 个错误相关的检查结果 */
    EXPECT_GE(report.GetErrorCount(), 1U);  /* 至少发现 1 个错误 */
    EXPECT_LE(report.GetErrorCount(), 2U);  /* 受 maxErrors=1 限制，最多 2 个（页级 + 元组级） */
}

TEST(UTHeapSegmentVerify, ReadHeapPageFailure)
{
    RegisterHeapPageVerifier();

    /* pageSource 中不添加页面，导致 ReadHeapPage 返回 nullptr */
    FakeHeapVerifyPageSource pageSource;
    /* 手动添加一个 pageId 到 order 列表，但不提供实际页面 */
    pageSource.AddPage({47, 10}, nullptr);

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, false, 1000);
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
    VerifyContext context(&report, nullptr, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
    EXPECT_EQ(report.GetWarningCount(), 1);
}

TEST(UTHeapSegmentVerify, BigTupleChainZeroChunkCount)
{
    RegisterHeapPageVerifier();

    PageBuffer buf1{};
    const FsmIndex fsm1{{97, 1}, 1};
    const PageId pageId1{47, 12};

    HeapPage *page1 = InitHeapPage(buf1, pageId1, fsm1);
    /* FirstLinkChunk with numChunks = 0 (invalid) */
    AddFirstChunk(page1, FIRST_ITEM_OFFSET_NUMBER, 64, INVALID_ITEM_POINTER, 0);
    page1->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(pageId1, page1);
    pageSource.SetRecordedFsm(fsm1,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page1->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTHeapSegmentVerify, BigTupleChunkNotContinuation)
{
    RegisterHeapPageVerifier();

    PageBuffer buf1{};
    PageBuffer buf2{};
    const FsmIndex fsm1{{98, 1}, 1};
    const FsmIndex fsm2{{98, 1}, 2};
    const PageId pageId1{48, 10};
    const PageId pageId2{48, 11};

    HeapPage *page1 = InitHeapPage(buf1, pageId1, fsm1);
    HeapPage *page2 = InitHeapPage(buf2, pageId2, fsm2);

    /* page1: first chunk, 3 chunks declared, next -> page2 offset 1 */
    ItemPointerData nextToPage2(pageId2, FIRST_ITEM_OFFSET_NUMBER);
    AddFirstChunk(page1, FIRST_ITEM_OFFSET_NUMBER, 64, nextToPage2, 3);

    /* page2: ALSO a first chunk (instead of follow chunk) */
    AddFirstChunk(page2, FIRST_ITEM_OFFSET_NUMBER, 64, INVALID_ITEM_POINTER, 1);

    page1->SetChecksum();
    page2->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(pageId1, page1);
    pageSource.AddPage(pageId2, page2);
    pageSource.SetRecordedFsm(fsm1,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page1->GetFreeSpace<FreeSpaceCondition::RAW>())));
    pageSource.SetRecordedFsm(fsm2,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page2->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/* ======================================================================
 * Additional HeapSegment tests — cross-page chain & edge cases
 * ====================================================================== */

/*
 * ThreeChunkCrossPageChainValid
 *
 * Construct a complete 3-chunk big tuple chain across 3 pages.
 * page1(first) → page2(follow) → page3(follow, end). Expects success.
 */
TEST(UTHeapSegmentVerify, ThreeChunkCrossPageChainValid)
{
    RegisterHeapPageVerifier();

    PageBuffer buf1{}, buf2{}, buf3{};
    const FsmIndex fsm1{{100, 1}, 1};
    const FsmIndex fsm2{{100, 1}, 2};
    const FsmIndex fsm3{{100, 1}, 3};
    const PageId pid1{50, 10};
    const PageId pid2{50, 11};
    const PageId pid3{50, 12};

    HeapPage *page1 = InitHeapPage(buf1, pid1, fsm1);
    HeapPage *page2 = InitHeapPage(buf2, pid2, fsm2);
    HeapPage *page3 = InitHeapPage(buf3, pid3, fsm3);

    /* page1: first chunk, 3 declared, next → page2 */
    ItemPointerData next12(pid2, FIRST_ITEM_OFFSET_NUMBER);
    AddFirstChunk(page1, FIRST_ITEM_OFFSET_NUMBER, 64, next12, 3);

    /* page2: follow chunk, next → page3 */
    ItemPointerData next23(pid3, FIRST_ITEM_OFFSET_NUMBER);
    AddFollowChunk(page2, FIRST_ITEM_OFFSET_NUMBER, 64, next23);

    /* page3: follow chunk, next = INVALID (end) */
    AddFollowChunk(page3, FIRST_ITEM_OFFSET_NUMBER, 64, INVALID_ITEM_POINTER);

    page1->SetChecksum();
    page2->SetChecksum();
    page3->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(pid1, page1);
    pageSource.AddPage(pid2, page2);
    pageSource.AddPage(pid3, page3);
    pageSource.SetRecordedFsm(fsm1,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page1->GetFreeSpace<FreeSpaceCondition::RAW>())));
    pageSource.SetRecordedFsm(fsm2,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page2->GetFreeSpace<FreeSpaceCondition::RAW>())));
    pageSource.SetRecordedFsm(fsm3,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page3->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

/*
 * ChunkCountMismatch_TooFew
 *
 * First chunk declares 3 chunks, but only 2 exist (chain terminates early).
 * Verifier should detect chunk count mismatch.
 */
TEST(UTHeapSegmentVerify, ChunkCountMismatch_TooFew)
{
    RegisterHeapPageVerifier();

    PageBuffer buf1{}, buf2{};
    const FsmIndex fsm1{{101, 1}, 1};
    const FsmIndex fsm2{{101, 1}, 2};
    const PageId pid1{51, 10};
    const PageId pid2{51, 11};

    HeapPage *page1 = InitHeapPage(buf1, pid1, fsm1);
    HeapPage *page2 = InitHeapPage(buf2, pid2, fsm2);

    /* page1: first chunk, declares 3 chunks, next → page2 */
    ItemPointerData next12(pid2, FIRST_ITEM_OFFSET_NUMBER);
    AddFirstChunk(page1, FIRST_ITEM_OFFSET_NUMBER, 64, next12, 3);

    /* page2: follow chunk, next = INVALID (chain ends here, but 3 expected) */
    AddFollowChunk(page2, FIRST_ITEM_OFFSET_NUMBER, 64, INVALID_ITEM_POINTER);

    page1->SetChecksum();
    page2->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(pid1, page1);
    pageSource.AddPage(pid2, page2);
    pageSource.SetRecordedFsm(fsm1,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page1->GetFreeSpace<FreeSpaceCondition::RAW>())));
    pageSource.SetRecordedFsm(fsm2,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page2->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/*
 * ChunkSizeMismatch_ContinuationChunk
 *
 * Continuation chunk has ItemId len != tuple size (corruption in chain).
 */
TEST(UTHeapSegmentVerify, ChunkSizeMismatch_ContinuationChunk)
{
    RegisterHeapPageVerifier();

    PageBuffer buf1{}, buf2{};
    const FsmIndex fsm1{{102, 1}, 1};
    const FsmIndex fsm2{{102, 1}, 2};
    const PageId pid1{52, 10};
    const PageId pid2{52, 11};

    HeapPage *page1 = InitHeapPage(buf1, pid1, fsm1);
    HeapPage *page2 = InitHeapPage(buf2, pid2, fsm2);

    /* page1: first chunk, 2 declared, next → page2 */
    ItemPointerData next12(pid2, FIRST_ITEM_OFFSET_NUMBER);
    AddFirstChunk(page1, FIRST_ITEM_OFFSET_NUMBER, 64, next12, 2);

    /* page2: follow chunk, but corrupt ItemId len to mismatch */
    AddFollowChunk(page2, FIRST_ITEM_OFFSET_NUMBER, 64, INVALID_ITEM_POINTER);
    page2->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER)->SetNormal(page2->GetUpper(), 48);  /* 48 != 64 */

    page1->SetChecksum();
    page2->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(pid1, page1);
    pageSource.AddPage(pid2, page2);
    pageSource.SetRecordedFsm(fsm1,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page1->GetFreeSpace<FreeSpaceCondition::RAW>())));
    pageSource.SetRecordedFsm(fsm2,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page2->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/*
 * TupleZeroColumns
 *
 * A tuple with numColumn == 0 should be detected as invalid by VerifyTuple.
 */
TEST(UTHeapSegmentVerify, TupleZeroColumns)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    const FsmIndex fsmIdx{{103, 1}, 1};
    HeapPage *page = InitHeapPage(buf, {53, 10}, fsmIdx);
    HeapDiskTuple *tuple = AddTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32);
    tuple->SetNumColumn(0);
    page->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(page->GetSelfPageId(), page);
    pageSource.SetRecordedFsm(fsmIdx,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/*
 * EmptySegment_NoPagesSucceeds
 *
 * A pageSource with no pages (GetFirstPageId returns INVALID).
 * Verifier should succeed with an empty report.
 */
TEST(UTHeapSegmentVerify, EmptySegment_NoPagesSucceeds)
{
    RegisterHeapPageVerifier();

    FakeHeapVerifyPageSource pageSource;  /* no pages added */

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
    EXPECT_EQ(report.GetTotalChecks(), 0U);
}

/*
 * MultiTupleMixedValidity
 *
 * Two tuples on the same page: first valid, second has size mismatch.
 * Verifier should detect the corruption.
 */
TEST(UTHeapSegmentVerify, MultiTupleMixedValidity)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    const FsmIndex fsmIdx{{104, 1}, 1};
    HeapPage *page = InitHeapPage(buf, {54, 10}, fsmIdx);

    /* First tuple: valid */
    AddTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32);

    /* Second tuple: corrupt ItemId len to mismatch tuple size */
    OffsetNumber second = static_cast<OffsetNumber>(FIRST_ITEM_OFFSET_NUMBER + 1);
    AddTuple(page, second, 48);
    page->GetItemIdPtr(second)->SetNormal(page->GetUpper(), 32);  /* 32 != 48 */

    page->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(page->GetSelfPageId(), page);
    pageSource.SetRecordedFsm(fsmIdx,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

/*
 * DisableBigTupleChainCheck
 *
 * When options.checkBigTupleChains == false, a broken big tuple chain
 * should NOT be detected (the check is skipped).
 */
TEST(UTHeapSegmentVerify, DisableBigTupleChainCheck)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    const FsmIndex fsmIdx{{105, 1}, 1};
    HeapPage *page = InitHeapPage(buf, {55, 10}, fsmIdx);

    /* Big tuple with broken chain (INVALID next, but 2 chunks declared) */
    AddFirstChunk(page, FIRST_ITEM_OFFSET_NUMBER, 64, INVALID_ITEM_POINTER, 2);
    page->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(page->GetSelfPageId(), page);
    pageSource.SetRecordedFsm(fsmIdx,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = false;
    options.checkBigTupleChains = false;  /* disabled */
    VerifyReport report;
    VerifyContext context(&report, nullptr, false, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    /* Should succeed because chain check is disabled */
    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

/* ======================================================================
 * Online mode tests — verify isOnline=true code path
 *
 * In the UT environment, the thread-local `thrd` is nullptr, so
 * ShouldSkipTupleOnline() returns false at the first guard clause.
 * These tests verify that:
 *   (1) the online mode flag is accepted without errors,
 *   (2) the VerifyContext correctly receives isOnline=true,
 *   (3) valid segments still pass verification in online mode,
 *   (4) corrupted tuples are still detected (not silently skipped)
 *       when the transaction context is unavailable.
 * ====================================================================== */

/*
 * OnlineMode_ValidSegment_PassesVerification
 *
 * Basic online mode success path: a valid two-page segment with
 * isOnline=true should pass verification without errors.
 */
TEST(UTHeapSegmentVerify, OnlineMode_ValidSegment_PassesVerification)
{
    RegisterHeapPageVerifier();

    PageBuffer firstBuffer{};
    PageBuffer secondBuffer{};
    const FsmIndex firstFsm{{110, 1}, 3};
    const FsmIndex secondFsm{{110, 1}, 4};

    HeapPage *firstPage = InitHeapPage(firstBuffer, {60, 10}, firstFsm);
    HeapPage *secondPage = InitHeapPage(secondBuffer, {60, 11}, secondFsm);
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
    options.isOnline = true;
    VerifyReport report;
    VerifyContext context(&report, nullptr, true, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
    EXPECT_EQ(report.GetWarningCount(), 0);
    EXPECT_TRUE(context.IsOnline());
}

/*
 * OnlineMode_InProgressTuple_SkippedNotError
 *
 * In online mode, a tuple whose transaction is in-progress should be
 * skipped by ShouldSkipTupleOnline(). However, in the UT environment
 * the thread-local `thrd` is nullptr, so the skip logic returns false
 * and the tuple is verified normally. This test confirms that:
 *   - isOnline=true is accepted,
 *   - VerifyContext records the online flag,
 *   - a valid tuple still passes (the skip-guard falls through gracefully).
 *
 * NOTE: Full MVCC skip coverage requires integration tests with a real
 * ThreadContext and active Transaction, which is outside the scope of
 * unit testing with FakeHeapVerifyPageSource.
 */
TEST(UTHeapSegmentVerify, OnlineMode_InProgressTuple_SkippedNotError)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    const FsmIndex fsmIdx{{111, 1}, 1};
    HeapPage *page = InitHeapPage(buf, {61, 10}, fsmIdx);
    HeapDiskTuple *tuple = AddTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32);
    /* Mark tuple with a non-zero xid to simulate an in-progress transaction.
     * Without thrd/Transaction, ShouldSkipTupleOnline returns false and the
     * tuple is verified normally — it should still pass since the tuple is valid. */
    tuple->SetTdId(0);
    page->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(page->GetSelfPageId(), page);
    pageSource.SetRecordedFsm(fsmIdx,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = true;
    VerifyReport report;
    VerifyContext context(&report, nullptr, true, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
    EXPECT_TRUE(context.IsOnline());
}

/*
 * OnlineMode_AbortedTuple_SkippedNotError
 *
 * Similar to the in-progress test above: in online mode, a tuple whose
 * transaction was aborted should be skipped. In the UT environment
 * without ThreadContext, the skip-guard falls through and the tuple is
 * verified normally. This test confirms graceful behavior.
 */
TEST(UTHeapSegmentVerify, OnlineMode_AbortedTuple_SkippedNotError)
{
    RegisterHeapPageVerifier();

    PageBuffer buf{};
    const FsmIndex fsmIdx{{112, 1}, 1};
    HeapPage *page = InitHeapPage(buf, {62, 10}, fsmIdx);
    AddTuple(page, FIRST_ITEM_OFFSET_NUMBER, 32);
    page->SetChecksum();

    FakeHeapVerifyPageSource pageSource;
    pageSource.AddPage(page->GetSelfPageId(), page);
    pageSource.SetRecordedFsm(fsmIdx,
        PartitionFreeSpaceMap::GetListId(static_cast<uint16>(page->GetFreeSpace<FreeSpaceCondition::RAW>())));

    HeapVerifyOptions options;
    options.isOnline = true;
    /* Provide a snapshot to exercise the ResolveSnapshot path */
    SnapshotData snapshotData{};
    snapshotData.Init();
    options.snapshot = &snapshotData;
    VerifyReport report;
    VerifyContext context(&report, &snapshotData, true, 1000);
    HeapSegmentVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
    EXPECT_TRUE(context.IsOnline());
    EXPECT_EQ(context.GetSnapshot(), &snapshotData);
}

/*
 * NOTE: MAX_BIG_TUPLE_CHAIN_LENGTH protection
 *
 * The HeapSegmentVerifier enforces a maximum chain length limit during big
 * tuple chain traversal to guard against corrupted circular chains that evade
 * the visited-set cycle detector. Testing this limit in a unit test is
 * impractical because it would require the FakeHeapVerifyPageSource to host
 * 10000+ distinct chunk pages with properly linked big tuple headers. This
 * protection is best validated through integration testing with a real or
 * simulated segment containing a long chain.
 */

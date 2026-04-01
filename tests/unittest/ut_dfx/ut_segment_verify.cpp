#include <array>
#include <iostream>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "dfx/dstore_segment_verify.h"
#include "dfx/dstore_page_verify.h"
#include "common/memory/dstore_mctx.h"
#include "page/dstore_btr_recycle_root_meta_page.h"

using namespace DSTORE;

namespace {

using PageBuffer = std::array<unsigned char, BLCKSZ>;

uint64 PageIdKey(const PageId &pageId)
{
    return (static_cast<uint64>(pageId.m_fileId) << 32) | pageId.m_blockId;
}

class FakeSegmentVerifyPageSource : public SegmentVerifyPageSource {
public:
    void AddPage(const PageId &pageId, Page *page)
    {
        m_pages[PageIdKey(pageId)] = page;
    }

    void SetRoot(const PageId &rootPageId, uint32 rootLevel)
    {
        m_rootPageId = rootPageId;
        m_rootLevel = rootLevel;
    }

    SegmentMetaPage *ReadSegmentMetaPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return static_cast<SegmentMetaPage *>(ReadPage(pageId, bufferDesc));
    }

    SegExtentMetaPage *ReadExtentMetaPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return static_cast<SegExtentMetaPage *>(ReadPage(pageId, bufferDesc));
    }

    Page *ReadPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        if (bufferDesc != nullptr) {
            *bufferDesc = nullptr;
        }
        auto it = m_pages.find(PageIdKey(pageId));
        return it == m_pages.end() ? nullptr : it->second;
    }

    TbsBitmapMetaPage *ReadBitmapMetaPage(FileId fileId, BufferDesc **bufferDesc) override
    {
        return static_cast<TbsBitmapMetaPage *>(ReadPage({fileId, TBS_BITMAP_META_PAGE}, bufferDesc));
    }

    TbsBitmapPage *ReadBitmapPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return static_cast<TbsBitmapPage *>(ReadPage(pageId, bufferDesc));
    }

    bool GetIndexRootInfo(const PageId &segmentMetaPageId, PageId *rootPageId, uint32 *rootLevel) override
    {
        (void)segmentMetaPageId;
        if (rootPageId == nullptr || rootLevel == nullptr) {
            return false;
        }
        *rootPageId = m_rootPageId;
        *rootLevel = m_rootLevel;
        return m_rootPageId.IsValid();
    }

    void ReleasePage(BufferDesc *bufferDesc) override
    {
        (void)bufferDesc;
    }

private:
    std::unordered_map<uint64, Page *> m_pages;
    PageId m_rootPageId{INVALID_PAGE_ID};
    uint32 m_rootLevel{0};
};

class ScopedMemoryContext {
public:
    ScopedMemoryContext()
    {
        m_context = DstoreAllocSetContextCreate(nullptr, "UtDfxSegmentVerify", ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_INITSIZE, MemoryContextType::SHARED_CONTEXT);
        m_oldContext = DstoreMemoryContextSwitchTo(m_context);
    }

    ~ScopedMemoryContext()
    {
        if (m_context != nullptr) {
            (void)DstoreMemoryContextSwitchTo(m_oldContext);
            DstoreMemoryContextDestroyTop(m_context);
        }
    }

private:
    DstoreMemoryContext m_context{nullptr};
    DstoreMemoryContext m_oldContext{nullptr};
};

SegmentMetaPage *InitIndexSegmentMeta(PageBuffer &buffer, PageId pageId, PageId nextExtent = INVALID_PAGE_ID)
{
    auto *page = reinterpret_cast<DataSegmentMetaPage *>(buffer.data());
    if (page->InitDataSegmentMetaPage(SegmentType::INDEX_SEGMENT_TYPE, pageId, EXT_SIZE_8, 0, 0) != DSTORE_SUCC) {
        return nullptr;
    }
    page->InitSegmentInfo({pageId.m_fileId, static_cast<BlockNumber>(pageId.m_blockId + 2)}, false);
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();
    page->extentMeta.nextExtMetaPageId = nextExtent;
    return static_cast<SegmentMetaPage *>(static_cast<void *>(page));
}

SegExtentMetaPage *InitExtent(PageBuffer &buffer, PageId pageId, ExtentSize extentSize, PageId nextExtent)
{
    auto *page = reinterpret_cast<SegExtentMetaPage *>(buffer.data());
    page->InitSegExtentMetaPage(pageId, extentSize, PageType::TBS_EXTENT_META_PAGE_TYPE);
    page->LinkNextExtent(nextExtent);
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();
    return page;
}

TbsBitmapMetaPage *InitBitmapMeta(PageBuffer &buffer, FileId fileId, ExtentSize extentSize, PageId firstBitmapPageId)
{
    auto *page = reinterpret_cast<TbsBitmapMetaPage *>(buffer.data());
    page->InitBitmapMetaPage({fileId, TBS_BITMAP_META_PAGE}, 1024, extentSize);
    page->groupCount = 1;
    page->bitmapGroups[0].firstBitmapPageId = firstBitmapPageId;
    page->validOffset = static_cast<uint16>(OFFSETOF(TbsBitmapMetaPage, bitmapGroups) + sizeof(TbsBitMapGroup));
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();
    return page;
}

TbsBitmapPage *InitBitmapPage(PageBuffer &buffer, PageId pageId, PageId firstDataPageId)
{
    auto *page = reinterpret_cast<TbsBitmapPage *>(buffer.data());
    page->InitBitmapPage(pageId, firstDataPageId);
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();
    return page;
}

BtrPage *InitBtreePage(PageBuffer &buffer, PageId pageId, const PageId &btrMetaPageId, uint32 level, BtrPageType type,
    bool isRoot)
{
    auto *page = reinterpret_cast<BtrPage *>(buffer.data());
    page->InitBtrPageInner(pageId);
    page->SetLsn(1, 1, 1, false);
    page->GetLinkAndStatus()->InitPageMeta(btrMetaPageId, level, isRoot);
    page->GetLinkAndStatus()->SetType(type);
    page->SetBtrMetaCreateXid(Xid(0));
    page->AllocateTdSpace();
    page->SetChecksum();
    return page;
}

BtrPage *InitBtreeMetaPage(PageBuffer &buffer, PageId pageId, PageId rootPageId, uint32 rootLevel)
{
    BtrPage *page = InitBtreePage(buffer, pageId, pageId, 0, BtrPageType::META_PAGE, false);
    auto *meta = static_cast<BtrMeta *>(static_cast<void *>(page->GetData()));
    memset_s(meta, sizeof(BtrMeta), 0, sizeof(BtrMeta));
    meta->SetBtreeMetaInfo(rootPageId, rootPageId, rootLevel, rootLevel);
    return page;
}

BtrRecycleRootMetaPage *InitRecycleRoot(PageBuffer &buffer, PageId pageId)
{
    auto *page = reinterpret_cast<BtrRecycleRootMetaPage *>(buffer.data());
    page->InitRecycleRootMetaPage(pageId, Xid(1));
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();
    return page;
}

void AddSinglePivot(BtrPage *page, const PageId &childPageId)
{
    Datum datum = Int32GetDatum(1);
    bool isNull = false;
    TupleDesc tupleDesc = nullptr;
    Size tupleDescSize = MAXALIGN(sizeof(TupleDescData));
    Size attrsPointerSize = MAXALIGN(sizeof(Form_pg_attribute));
    Size attrsDataSize = MAXALIGN(sizeof(FormData_pg_attribute));
    char *storage = static_cast<char *>(DstorePalloc0(tupleDescSize + attrsPointerSize + attrsDataSize));
    tupleDesc = reinterpret_cast<TupleDesc>(storage);
    tupleDesc->natts = 1;
    tupleDesc->attrs = reinterpret_cast<Form_pg_attribute *>(storage + tupleDescSize);
    auto *attrData = reinterpret_cast<FormData_pg_attribute *>(storage + tupleDescSize + attrsPointerSize);
    tupleDesc->attrs[0] = attrData;
    attrData->atttypid = INT4OID;
    attrData->attlen = sizeof(int32);
    attrData->attnum = 1;
    attrData->attbyval = true;
    attrData->attalign = 'i';

    IndexTuple *tuple = IndexTuple::FormTuple(tupleDesc, &datum, &isNull);
    tuple->SetKeyNum(1, false);
    tuple->SetLowlevelIndexpageLink(childPageId);

    page->SetUpper(static_cast<uint16>(page->GetUpper() - tuple->GetSize()));
    page->SetLower(static_cast<uint16>(page->GetLower() + sizeof(ItemId)));
    page->GetItemIdPtr(BTREE_PAGE_FIRSTKEY)->SetNormal(page->GetUpper(), tuple->GetSize());
    error_t rc = memcpy_s(page->GetData() + page->GetUpper(), tuple->GetSize(), tuple, tuple->GetSize());
    storage_securec_check(rc, "\0", "\0");
    page->SetChecksum();
}

void SetBitmapBit(TbsBitmapPage *bitmapPage, uint16 bitNo)
{
    ASSERT_LT(bitNo, DF_BITMAP_BIT_CNT);
    if (bitmapPage->TestBitZero(bitNo)) {
        bitmapPage->SetByBit(bitNo);
    }
    bitmapPage->SetChecksum();
}

}  // namespace

TEST(UTSegmentVerify, ValidSegmentPasses)
{
    ScopedMemoryContext scopedMemoryContext;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer segmentMetaBuffer{};
    PageBuffer bitmapMetaBuffer{};
    PageBuffer bitmapBuffer{};
    PageBuffer btrMetaBuffer{};
    PageBuffer recycleBuffer{};
    PageBuffer rootBuffer{};
    PageBuffer siblingBuffer{};

    const PageId segmentMetaPageId{70, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    segmentMeta->dataBlockCount = 1;
    segmentMeta->dataFirst = {70, 15};
    segmentMeta->dataLast = {70, 15};
    segmentMeta->SetChecksum();

    auto *bitmapMeta = InitBitmapMeta(bitmapMetaBuffer, 70, EXT_SIZE_8, {70, 3});
    auto *bitmapPage = InitBitmapPage(bitmapBuffer, {70, 3}, {70, 4});
    SetBitmapBit(bitmapPage, 1);
    bitmapMeta->SetChecksum();

    InitBtreeMetaPage(btrMetaBuffer, {70, 13}, {70, 15}, 0);
    InitRecycleRoot(recycleBuffer, {70, 14});
    BtrPage *root = InitBtreePage(rootBuffer, {70, 15}, {70, 13}, 0, BtrPageType::LEAF_PAGE, true);
    root->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    root->GetLinkAndStatus()->SetLeft(INVALID_PAGE_ID);
    root->SetChecksum();

    FakeSegmentVerifyPageSource pageSource;
    pageSource.SetRoot({70, 15}, 0);
    pageSource.AddPage(segmentMetaPageId, static_cast<Page *>(static_cast<void *>(segmentMeta)));
    pageSource.AddPage({70, TBS_BITMAP_META_PAGE}, static_cast<Page *>(static_cast<void *>(bitmapMeta)));
    pageSource.AddPage({70, 3}, static_cast<Page *>(static_cast<void *>(bitmapPage)));
    pageSource.AddPage({70, 13}, static_cast<Page *>(static_cast<void *>(reinterpret_cast<BtrPage *>(btrMetaBuffer.data()))));
    pageSource.AddPage({70, 14}, static_cast<Page *>(static_cast<void *>(reinterpret_cast<BtrRecycleRootMetaPage *>(recycleBuffer.data()))));
    pageSource.AddPage({70, 15}, static_cast<Page *>(static_cast<void *>(root)));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    RetStatus ret = verifier.Verify();
    if (ret != DSTORE_SUCC) {
        std::cerr << report.FormatText() << std::endl;
    }
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST(UTSegmentVerify, BrokenExtentChainFails)
{
    ScopedMemoryContext scopedMemoryContext;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    const PageId segmentMetaPageId{71, 12};
    InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId, {71, 20});

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTSegmentVerify, CircularExtentChainFails)
{
    ScopedMemoryContext scopedMemoryContext;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    PageBuffer extentBuffer{};
    const PageId segmentMetaPageId{72, 12};
    InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId, {72, 20});
    InitExtent(extentBuffer, {72, 20}, EXT_SIZE_8, segmentMetaPageId);

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage({72, 20}, reinterpret_cast<Page *>(extentBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTSegmentVerify, BitmapMismatchFails)
{
    ScopedMemoryContext scopedMemoryContext;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    PageBuffer bitmapMetaBuffer{};
    PageBuffer bitmapBuffer{};

    const PageId segmentMetaPageId{73, 12};
    InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId);
    InitBitmapMeta(bitmapMetaBuffer, 73, EXT_SIZE_8, {73, 3});
    InitBitmapPage(bitmapBuffer, {73, 3}, {73, 4});

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage({73, TBS_BITMAP_META_PAGE}, reinterpret_cast<Page *>(bitmapMetaBuffer.data()));
    pageSource.AddPage({73, 3}, reinterpret_cast<Page *>(bitmapBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTSegmentVerify, LeafPageCountMismatchWarns)
{
    ScopedMemoryContext scopedMemoryContext;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer segmentMetaBuffer{};
    PageBuffer bitmapMetaBuffer{};
    PageBuffer bitmapBuffer{};
    PageBuffer btrMetaBuffer{};
    PageBuffer recycleBuffer{};
    PageBuffer rootBuffer{};
    PageBuffer leafBuffer{};
    PageBuffer orphanLeafBuffer{};

    const PageId segmentMetaPageId{74, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    segmentMeta->dataBlockCount = 2;
    segmentMeta->dataFirst = {74, 15};
    segmentMeta->dataLast = {74, 16};
    segmentMeta->SetChecksum();

    InitBitmapMeta(bitmapMetaBuffer, 74, EXT_SIZE_8, {74, 3});
    auto *bitmapPage = InitBitmapPage(bitmapBuffer, {74, 3}, {74, 4});
    SetBitmapBit(bitmapPage, 1);

    InitBtreeMetaPage(btrMetaBuffer, {74, 13}, {74, 15}, 0);
    InitRecycleRoot(recycleBuffer, {74, 14});
    BtrPage *root = InitBtreePage(rootBuffer, {74, 15}, {74, 13}, 0, BtrPageType::LEAF_PAGE, true);
    BtrPage *orphanLeaf = InitBtreePage(orphanLeafBuffer, {74, 16}, {74, 13}, 0, BtrPageType::LEAF_PAGE, false);
    root->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    orphanLeaf->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);

    FakeSegmentVerifyPageSource pageSource;
    pageSource.SetRoot({74, 15}, 0);
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage({74, TBS_BITMAP_META_PAGE}, reinterpret_cast<Page *>(bitmapMetaBuffer.data()));
    pageSource.AddPage({74, 3}, reinterpret_cast<Page *>(bitmapBuffer.data()));
    pageSource.AddPage({74, 13}, reinterpret_cast<Page *>(btrMetaBuffer.data()));
    pageSource.AddPage({74, 14}, reinterpret_cast<Page *>(recycleBuffer.data()));
    pageSource.AddPage({74, 15}, reinterpret_cast<Page *>(rootBuffer.data()));
    pageSource.AddPage({74, 16}, reinterpret_cast<Page *>(orphanLeafBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    RetStatus ret = verifier.Verify();
    if (ret != DSTORE_SUCC) {
        std::cerr << report.FormatText() << std::endl;
    }
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
    EXPECT_EQ(report.GetWarningCount(), 1);
}

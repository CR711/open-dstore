#include <array>
#include <atomic>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "dfx/dstore_segment_verify.h"
#include "dfx/dstore_page_verify.h"
#include "common/memory/dstore_mctx.h"
#include "page/dstore_btr_recycle_root_meta_page.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;

namespace {

using DSTORE::ut_dfx::PageBuffer;

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
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
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
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
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
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
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
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
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

using DSTORE::ut_dfx::HasCheckName;
using DSTORE::ut_dfx::HasSeverity;

TEST(UTSegmentVerify, InvalidSegmentMetaPageIdFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    FakeSegmentVerifyPageSource pageSource;

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, INVALID_PAGE_ID, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    /* Invalid page id causes FakeSegmentVerifyPageSource::ReadPage to return nullptr,
     * which surfaces through SegmentVerifier::Verify as segment_meta_read_failed. */
    EXPECT_TRUE(HasCheckName(report, "segment_meta_read_failed"));
}

TEST(UTSegmentVerify, SegmentMetaReadFailedFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    const PageId segmentMetaPageId{40, 12};
    FakeSegmentVerifyPageSource pageSource;

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "segment_meta_read_failed"));
}

TEST(UTSegmentVerify, SegmentMagicInvalidFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    const PageId segmentMetaPageId{41, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    ASSERT_NE(segmentMeta, nullptr);
    segmentMeta->extentMeta.magic = 0xDEADBEEF;
    segmentMeta->SetChecksum();

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "segment_magic_invalid"));
}

TEST(UTSegmentVerify, InvalidSegmentTypeFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    const PageId segmentMetaPageId{42, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    ASSERT_NE(segmentMeta, nullptr);
    /* Force an unsupported type (outside of enum range). */
    segmentMeta->segmentHeader.segmentType = static_cast<SegmentType>(99);
    segmentMeta->SetChecksum();

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "segment_type_invalid"));
}

TEST(UTSegmentVerify, ExtentChainBrokenMidwayFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    const PageId segmentMetaPageId{43, 12};
    /* Link segmentMeta to a next extent that is never added to the page source. */
    InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId, {43, 200});

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "extent_chain_broken"));
}

TEST(UTSegmentVerify, ExtentMagicInvalidFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    PageBuffer extentBuffer{};
    const PageId segmentMetaPageId{44, 12};
    /* Extent index 1 — ResolveExpectedExtentSize returns EXT_SIZE_8 for indices 0..15. */
    const PageId extentPageId{44, 20};
    InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId, extentPageId);
    auto *extent = InitExtent(extentBuffer, extentPageId, EXT_SIZE_8, INVALID_PAGE_ID);
    ASSERT_NE(extent, nullptr);
    extent->extentMeta.magic = 0xCAFEBABE;
    extent->SetChecksum();

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage(extentPageId, reinterpret_cast<Page *>(extentBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "extent_magic_invalid"));
}

TEST(UTSegmentVerify, ExtentSizeInvalidFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    PageBuffer extentBuffer{};
    const PageId segmentMetaPageId{45, 12};
    const PageId extentPageId{45, 20};
    InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId, extentPageId);
    /* Second extent at index=1 expects EXT_SIZE_8 but we install an EXT_SIZE_128. */
    InitExtent(extentBuffer, extentPageId, EXT_SIZE_128, INVALID_PAGE_ID);

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage(extentPageId, reinterpret_cast<Page *>(extentBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "extent_size_invalid"));
}

TEST(UTSegmentVerify, BlockCountMismatchFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    const PageId segmentMetaPageId{46, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    ASSERT_NE(segmentMeta, nullptr);
    segmentMeta->segmentHeader.totalBlockCount = 777;  /* should be 8 after init */
    segmentMeta->SetChecksum();

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "block_count_mismatch"));
}

TEST(UTSegmentVerify, ExtentCountMismatchFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    const PageId segmentMetaPageId{47, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    ASSERT_NE(segmentMeta, nullptr);
    /* Pretend we have three extents but no chain — walked count will be 1. */
    segmentMeta->segmentHeader.extents.count = 3;
    segmentMeta->SetChecksum();

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "extent_count_mismatch"));
}

TEST(UTSegmentVerify, DataRangeEmptyButCountNonZeroFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    const PageId segmentMetaPageId{48, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    ASSERT_NE(segmentMeta, nullptr);
    segmentMeta->dataBlockCount = 5;
    segmentMeta->dataFirst = INVALID_PAGE_ID;
    segmentMeta->dataLast = INVALID_PAGE_ID;
    segmentMeta->SetChecksum();

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "data_range_mismatch"));
}

TEST(UTSegmentVerify, DataRangeOutsideExtentsFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    const PageId segmentMetaPageId{49, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    ASSERT_NE(segmentMeta, nullptr);
    segmentMeta->dataBlockCount = 1;
    /* Block 9999 is well outside the [12, 19] extent range. */
    segmentMeta->dataFirst = {49, 9999};
    segmentMeta->dataLast = {49, 9999};
    segmentMeta->SetChecksum();

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "data_range_mismatch"));
}

TEST(UTSegmentVerify, DataRangeReversedFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    const PageId segmentMetaPageId{50, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    ASSERT_NE(segmentMeta, nullptr);
    segmentMeta->dataBlockCount = 2;
    /* Both inside extent [12,19] but dataFirst > dataLast. */
    segmentMeta->dataFirst = {50, 18};
    segmentMeta->dataLast = {50, 15};
    segmentMeta->SetChecksum();

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "data_range_mismatch"));
}

TEST(UTSegmentVerify, BitmapMetaMissingFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    const PageId segmentMetaPageId{51, 12};
    InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId);

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    /* Intentionally skip adding the bitmap meta page. */

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "bitmap_meta_read_failed"));
}

TEST(UTSegmentVerify, BitmapPopcountMismatchFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    PageBuffer bitmapMetaBuffer{};
    PageBuffer bitmapBuffer{};
    const PageId segmentMetaPageId{52, 12};
    InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId);
    InitBitmapMeta(bitmapMetaBuffer, 52, EXT_SIZE_8, {52, 3});
    auto *bitmapPage = InitBitmapPage(bitmapBuffer, {52, 3}, {52, 4});
    /* Mark segment bit allocated so we get past the allocation check. */
    SetBitmapBit(bitmapPage, 1);
    /* Additionally flip another bit but do not increment allocatedExtentCount. */
    bitmapPage->SetByBit(2);
    bitmapPage->SetChecksum();

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage({52, TBS_BITMAP_META_PAGE}, reinterpret_cast<Page *>(bitmapMetaBuffer.data()));
    pageSource.AddPage({52, 3}, reinterpret_cast<Page *>(bitmapBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "bitmap_count_mismatch"));
}

TEST(UTSegmentVerify, IndexRootMissingFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer segmentMetaBuffer{};
    PageBuffer bitmapMetaBuffer{};
    PageBuffer bitmapBuffer{};
    const PageId segmentMetaPageId{53, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    ASSERT_NE(segmentMeta, nullptr);
    InitBitmapMeta(bitmapMetaBuffer, 53, EXT_SIZE_8, {53, 3});
    auto *bitmapPage = InitBitmapPage(bitmapBuffer, {53, 3}, {53, 4});
    SetBitmapBit(bitmapPage, 1);

    FakeSegmentVerifyPageSource pageSource;
    /* Do not call SetRoot — GetIndexRootInfo will return false. */
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage({53, TBS_BITMAP_META_PAGE}, reinterpret_cast<Page *>(bitmapMetaBuffer.data()));
    pageSource.AddPage({53, 3}, reinterpret_cast<Page *>(bitmapBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "btree_root_missing"));
}

TEST(UTSegmentVerify, BtreeLeafLevelInvalidFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer segmentMetaBuffer{};
    PageBuffer bitmapMetaBuffer{};
    PageBuffer bitmapBuffer{};
    PageBuffer btrMetaBuffer{};
    PageBuffer recycleBuffer{};
    PageBuffer rootBuffer{};

    const PageId segmentMetaPageId{54, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    ASSERT_NE(segmentMeta, nullptr);
    InitBitmapMeta(bitmapMetaBuffer, 54, EXT_SIZE_8, {54, 3});
    auto *bitmapPage = InitBitmapPage(bitmapBuffer, {54, 3}, {54, 4});
    SetBitmapBit(bitmapPage, 1);

    InitBtreeMetaPage(btrMetaBuffer, {54, 13}, {54, 15}, 0);
    InitRecycleRoot(recycleBuffer, {54, 14});
    /*
     * Root is declared as level-0 by SetRoot but the actual page is an INTERNAL_PAGE,
     * which means CountLeafPagesBySiblingTraversal treats it as a leaf-level page and
     * rejects it because the type isn't LEAF_PAGE.
     */
    BtrPage *root = InitBtreePage(rootBuffer, {54, 15}, {54, 13}, 0, BtrPageType::INTERNAL_PAGE, true);
    root->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    root->SetChecksum();

    FakeSegmentVerifyPageSource pageSource;
    pageSource.SetRoot({54, 15}, 0);
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage({54, TBS_BITMAP_META_PAGE}, reinterpret_cast<Page *>(bitmapMetaBuffer.data()));
    pageSource.AddPage({54, 3}, reinterpret_cast<Page *>(bitmapBuffer.data()));
    pageSource.AddPage({54, 13}, reinterpret_cast<Page *>(btrMetaBuffer.data()));
    pageSource.AddPage({54, 14}, reinterpret_cast<Page *>(recycleBuffer.data()));
    pageSource.AddPage({54, 15}, reinterpret_cast<Page *>(rootBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "leaf_page_level_invalid"));
}

TEST(UTSegmentVerify, BtreeSiblingCycleFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
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

    const PageId segmentMetaPageId{55, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    ASSERT_NE(segmentMeta, nullptr);
    segmentMeta->dataBlockCount = 2;
    segmentMeta->dataFirst = {55, 15};
    segmentMeta->dataLast = {55, 16};
    segmentMeta->SetChecksum();

    InitBitmapMeta(bitmapMetaBuffer, 55, EXT_SIZE_8, {55, 3});
    auto *bitmapPage = InitBitmapPage(bitmapBuffer, {55, 3}, {55, 4});
    SetBitmapBit(bitmapPage, 1);

    InitBtreeMetaPage(btrMetaBuffer, {55, 13}, {55, 15}, 0);
    InitRecycleRoot(recycleBuffer, {55, 14});
    BtrPage *root = InitBtreePage(rootBuffer, {55, 15}, {55, 13}, 0, BtrPageType::LEAF_PAGE, true);
    BtrPage *sibling = InitBtreePage(siblingBuffer, {55, 16}, {55, 13}, 0, BtrPageType::LEAF_PAGE, false);
    /* Form a 2-cycle: root -> sibling -> root. */
    root->GetLinkAndStatus()->SetRight({55, 16});
    sibling->GetLinkAndStatus()->SetRight({55, 15});
    root->SetChecksum();
    sibling->SetChecksum();

    FakeSegmentVerifyPageSource pageSource;
    pageSource.SetRoot({55, 15}, 0);
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage({55, TBS_BITMAP_META_PAGE}, reinterpret_cast<Page *>(bitmapMetaBuffer.data()));
    pageSource.AddPage({55, 3}, reinterpret_cast<Page *>(bitmapBuffer.data()));
    pageSource.AddPage({55, 13}, reinterpret_cast<Page *>(btrMetaBuffer.data()));
    pageSource.AddPage({55, 14}, reinterpret_cast<Page *>(recycleBuffer.data()));
    pageSource.AddPage({55, 15}, reinterpret_cast<Page *>(rootBuffer.data()));
    pageSource.AddPage({55, 16}, reinterpret_cast<Page *>(siblingBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "btree_sibling_cycle"));
}

TEST(UTSegmentVerify, ExtentChainMultiHopBrokenFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    PageBuffer extentBuffer{};
    const PageId segmentMetaPageId{56, 12};
    const PageId extent1{56, 20};
    const PageId extent2Missing{56, 28};
    InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId, extent1);
    /* extent1 chains to extent2Missing which is never registered. */
    InitExtent(extentBuffer, extent1, EXT_SIZE_8, extent2Missing);

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage(extent1, reinterpret_cast<Page *>(extentBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "extent_chain_broken"));
}

TEST(UTSegmentVerify, LeafPageCountMismatchWarns)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
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

namespace {

/*
 * A thread-safe segment verify page source for concurrent UTs.
 * The underlying map is protected by a shared mutex so a mutator thread
 * can flip a next-extent pointer while verifier threads traverse.
 *
 * Thread safety semantics:
 * - ReadPage / ReadSegmentMetaPage / ReadExtentMetaPage / ReadBitmap* are all shared-lock reads.
 * - SwapPage takes an exclusive lock and replaces the Page* atomically for a given PageId.
 * - GetIndexRootInfo honors m_rootPageId under shared lock.
 *
 * Callers must keep the Page buffers alive for the lifetime of the source.
 */
class ThreadSafeSegmentVerifyPageSource : public SegmentVerifyPageSource {
public:
    void AddPage(const PageId &pageId, Page *page)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_pages[PageIdKey(pageId)] = page;
    }

    void SwapPage(const PageId &pageId, Page *page)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_pages[PageIdKey(pageId)] = page;
    }

    void SetRoot(const PageId &rootPageId, uint32 rootLevel)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
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
        std::lock_guard<std::mutex> lock(m_mutex);
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
        std::lock_guard<std::mutex> lock(m_mutex);
        *rootPageId = m_rootPageId;
        *rootLevel = m_rootLevel;
        return m_rootPageId.IsValid();
    }

    void ReleasePage(BufferDesc *bufferDesc) override
    {
        (void)bufferDesc;
    }

private:
    std::mutex m_mutex;
    std::unordered_map<uint64, Page *> m_pages;
    PageId m_rootPageId{INVALID_PAGE_ID};
    uint32 m_rootLevel{0};
};

}  // namespace

TEST(UTSegmentVerify, ConcurrentVerifySharedValidSegment)
{
    /*
     * Rationale: VerifyContext is documented as NOT thread-safe per instance,
     * but independent instances operating on the same page source must not
     * interfere.  Four threads each drive their own Verifier and must all
     * reach DSTORE_SUCC on a well-formed segment.
     */
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer segmentMetaBuffer{};
    PageBuffer bitmapMetaBuffer{};
    PageBuffer bitmapBuffer{};
    PageBuffer btrMetaBuffer{};
    PageBuffer recycleBuffer{};
    PageBuffer rootBuffer{};

    const PageId segmentMetaPageId{80, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    ASSERT_NE(segmentMeta, nullptr);
    segmentMeta->dataBlockCount = 1;
    segmentMeta->dataFirst = {80, 15};
    segmentMeta->dataLast = {80, 15};
    segmentMeta->SetChecksum();

    InitBitmapMeta(bitmapMetaBuffer, 80, EXT_SIZE_8, {80, 3});
    auto *bitmapPage = InitBitmapPage(bitmapBuffer, {80, 3}, {80, 4});
    SetBitmapBit(bitmapPage, 1);

    InitBtreeMetaPage(btrMetaBuffer, {80, 13}, {80, 15}, 0);
    InitRecycleRoot(recycleBuffer, {80, 14});
    BtrPage *root = InitBtreePage(rootBuffer, {80, 15}, {80, 13}, 0, BtrPageType::LEAF_PAGE, true);
    root->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    root->SetChecksum();

    ThreadSafeSegmentVerifyPageSource pageSource;
    pageSource.SetRoot({80, 15}, 0);
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage({80, TBS_BITMAP_META_PAGE}, reinterpret_cast<Page *>(bitmapMetaBuffer.data()));
    pageSource.AddPage({80, 3}, reinterpret_cast<Page *>(bitmapBuffer.data()));
    pageSource.AddPage({80, 13}, reinterpret_cast<Page *>(btrMetaBuffer.data()));
    pageSource.AddPage({80, 14}, reinterpret_cast<Page *>(recycleBuffer.data()));
    pageSource.AddPage({80, 15}, reinterpret_cast<Page *>(rootBuffer.data()));

    constexpr int NUM_THREADS = 4;
    constexpr int ITERATIONS_PER_THREAD = 20;
    std::atomic<int> successCount{0};
    std::atomic<int> failureCount{0};
    std::atomic<int> readyCount{0};
    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&pageSource, &segmentMetaPageId, &successCount, &failureCount, &readyCount]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < NUM_THREADS) {
                /* spin until all threads ready — pattern borrowed from ut_page_verify_registry.cpp */
                std::this_thread::yield();
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int j = 0; j < ITERATIONS_PER_THREAD; ++j) {
                SegmentVerifyOptions options;
                VerifyReport localReport;
                VerifyContext localContext(&localReport, nullptr, 1.0F, false, 1000);
                SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &localContext);
                if (verifier.Verify() == DSTORE_SUCC && !localReport.HasError()) {
                    successCount.fetch_add(1, std::memory_order_relaxed);
                } else {
                    failureCount.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }

    EXPECT_EQ(successCount.load(), NUM_THREADS * ITERATIONS_PER_THREAD);
    EXPECT_EQ(failureCount.load(), 0);
}

TEST(UTSegmentVerify, ConcurrentVerifyWithMutatorNeverCrashes)
{
    /*
     * Scenario: a mutator thread repeatedly swaps the segment meta page between two
     * variants — one with a valid singleton extent chain, another with the next
     * extent pointing to a non-existent page — while verifier threads run.
     *
     * Expectation: no crash; each verifier either succeeds (valid snapshot) or
     * reports extent_chain_broken (dangling pointer snapshot).  No other error
     * should ever appear.  This tests the robustness of the read path when a
     * concurrent allocator is in-flight.
     *
     * CLAUDE.md rule 3: data consistency — readers must never observe a torn page
     * state because SwapPage swaps a whole Page* (pointer) atomically under the lock.
     * CLAUDE.md rule 5: bounds-checked loops, no raw new/delete, all page buffers
     * live on the stack for the entire test scope.
     */
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    /* Two segment meta variants built up front — swapping selects between them. */
    PageBuffer segmentMetaValidBuffer{};
    PageBuffer segmentMetaBrokenBuffer{};
    const PageId segmentMetaPageId{81, 12};
    const PageId danglingExtent{81, 200};
    auto *validMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaValidBuffer, segmentMetaPageId)));
    ASSERT_NE(validMeta, nullptr);
    auto *brokenMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBrokenBuffer, segmentMetaPageId, danglingExtent)));
    ASSERT_NE(brokenMeta, nullptr);
    /* Keep walked block count consistent with declared totalBlockCount to avoid
     * spurious block_count_mismatch on the broken variant prior to the chain break. */

    ThreadSafeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaValidBuffer.data()));

    std::atomic<bool> stopMutator{false};
    std::atomic<int> crashGuard{0};  /* increments if an unknown error is observed */

    std::thread mutator([&]() {
        /* Alternate between the two variants to emulate concurrent extent alloc.
         * yield() at each iteration forces a scheduler turnover so verifiers
         * are not starved on oversubscribed CI; without it this loop can pin a
         * core and degenerate the test into the single-variant path. */
        bool toggle = false;
        while (!stopMutator.load(std::memory_order_acquire)) {
            Page *next = toggle ? reinterpret_cast<Page *>(segmentMetaBrokenBuffer.data())
                                : reinterpret_cast<Page *>(segmentMetaValidBuffer.data());
            pageSource.SwapPage(segmentMetaPageId, next);
            toggle = !toggle;
            std::this_thread::yield();
        }
    });

    constexpr int NUM_VERIFIERS = 4;
    constexpr int ITERATIONS_PER_THREAD = 50;
    std::atomic<int> okCount{0};
    std::atomic<int> brokenChainCount{0};
    std::atomic<int> readyCount{0};
    std::vector<std::thread> verifiers;
    verifiers.reserve(NUM_VERIFIERS);
    for (int i = 0; i < NUM_VERIFIERS; ++i) {
        verifiers.emplace_back([&]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < NUM_VERIFIERS) {
                /* spin until all verifiers ready — aligns with ut_page_verify_registry.cpp */
                std::this_thread::yield();
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int j = 0; j < ITERATIONS_PER_THREAD; ++j) {
                SegmentVerifyOptions options;
                options.checkExtentBitmap = false;   /* Keep the scenario focused on the chain. */
                options.checkPageCounts = false;
                VerifyReport localReport;
                VerifyContext localContext(&localReport, nullptr, 1.0F, false, 1000);
                SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &localContext);
                RetStatus ret = verifier.Verify();
                if (ret == DSTORE_SUCC && !localReport.HasError()) {
                    okCount.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                if (HasCheckName(localReport, "extent_chain_broken")) {
                    brokenChainCount.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                /* Any other failure flavor is a regression. */
                crashGuard.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    /* Let mutator run the full duration of the verifiers.  The mutator lambda
     * has no side effects beyond this test's local pageSource, so stopping it
     * early only shrinks the swap/verify overlap window to the ~µs startup
     * barrier, weakening the data-race pressure we want to exercise. */
    for (auto &t : verifiers) {
        t.join();
    }
    stopMutator.store(true, std::memory_order_release);
    mutator.join();

    EXPECT_EQ(crashGuard.load(), 0);
    EXPECT_EQ(okCount.load() + brokenChainCount.load(), NUM_VERIFIERS * ITERATIONS_PER_THREAD);
    /* Invariant: the mutator must have won at least one race, otherwise the
     * test degenerated to the single-variant path and did not actually
     * exercise the dangling-extent snapshot. */
    EXPECT_GT(brokenChainCount.load(), 0)
        << "mutator never won a race; test degenerated to valid-only path";
}

TEST(UTSegmentVerify, DeepExtentChainCycleBoundedByVisitSet)
{
    /*
     * Build a multi-hop extent chain A -> B -> C -> B that forms a cycle not
     * involving the segment meta page.  The VisitPage set must detect the
     * second arrival at B and abort with extent_chain_cycle before unbounded
     * iteration.  This validates the visited-page guard beyond the simple
     * 1-hop variant already covered by CircularExtentChainFails.
     */
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    PageBuffer extentBBuffer{};
    PageBuffer extentCBuffer{};
    const PageId segmentMetaPageId{82, 12};
    /* ResolveExpectedExtentSize returns EXT_SIZE_8 for indices 0..15, so both extents use 8. */
    const PageId extentB{82, 20};
    const PageId extentC{82, 28};
    InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId, extentB);
    InitExtent(extentBBuffer, extentB, EXT_SIZE_8, extentC);
    /* extentC loops back to extentB. */
    InitExtent(extentCBuffer, extentC, EXT_SIZE_8, extentB);

    FakeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage(extentB, reinterpret_cast<Page *>(extentBBuffer.data()));
    pageSource.AddPage(extentC, reinterpret_cast<Page *>(extentCBuffer.data()));

    SegmentVerifyOptions options;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    /* visitedPages must detect the cycle on the first repeat and abort the walk;
     * a regression that re-emits cycle reports per hop would break this count. */
    EXPECT_EQ(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "extent_chain_cycle"));
}

TEST(UTSegmentVerify, BitmapPageEvictionDuringVerifyNeverCrashes)
{
    /*
     * Emulates a buffer pool eviction racing with segment verify: a mutator
     * thread repeatedly swaps the bitmap page between "bit set" and "bit cleared"
     * while multiple verifier threads run.  Expected outcomes per iteration:
     *   - DSTORE_SUCC (bitmap bit observed set)
     *   - DSTORE_FAIL with extent_not_in_bitmap (bit observed cleared)
     * Any other error means the read path is racy.
     *
     * CLAUDE.md rule 3: 并发一致性 — SwapPage replaces the Page* under the source's
     * mutex so verifier threads never observe a torn bitmap body.
     */
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();

    PageBuffer segmentMetaBuffer{};
    PageBuffer bitmapMetaBuffer{};
    PageBuffer bitmapAllocatedBuffer{};
    PageBuffer bitmapClearedBuffer{};

    const PageId segmentMetaPageId{83, 12};
    const PageId bitmapPageId{83, 3};
    InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId);
    InitBitmapMeta(bitmapMetaBuffer, 83, EXT_SIZE_8, bitmapPageId);

    auto *allocatedBitmap = InitBitmapPage(bitmapAllocatedBuffer, bitmapPageId, {83, 4});
    SetBitmapBit(allocatedBitmap, 1);
    auto *clearedBitmap = InitBitmapPage(bitmapClearedBuffer, bitmapPageId, {83, 4});
    /* clearedBitmap keeps bit 1 at zero to force extent_not_in_bitmap on that snapshot. */
    clearedBitmap->SetChecksum();

    ThreadSafeSegmentVerifyPageSource pageSource;
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage({83, TBS_BITMAP_META_PAGE}, reinterpret_cast<Page *>(bitmapMetaBuffer.data()));
    pageSource.AddPage(bitmapPageId, reinterpret_cast<Page *>(bitmapAllocatedBuffer.data()));

    std::atomic<bool> stopMutator{false};
    std::atomic<int> crashGuard{0};

    std::thread mutator([&]() {
        /* yield() ensures the mutator doesn't starve verifiers on oversubscribed CI. */
        bool toggle = false;
        while (!stopMutator.load(std::memory_order_acquire)) {
            Page *next = toggle ? reinterpret_cast<Page *>(bitmapClearedBuffer.data())
                                : reinterpret_cast<Page *>(bitmapAllocatedBuffer.data());
            pageSource.SwapPage(bitmapPageId, next);
            toggle = !toggle;
            std::this_thread::yield();
        }
    });

    constexpr int NUM_VERIFIERS = 4;
    constexpr int ITERATIONS_PER_THREAD = 50;
    std::atomic<int> okCount{0};
    std::atomic<int> bitmapMissCount{0};
    std::atomic<int> readyCount{0};
    std::vector<std::thread> verifiers;
    verifiers.reserve(NUM_VERIFIERS);
    for (int i = 0; i < NUM_VERIFIERS; ++i) {
        verifiers.emplace_back([&]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < NUM_VERIFIERS) {
                /* spin barrier aligned with ut_page_verify_registry.cpp */
                std::this_thread::yield();
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int j = 0; j < ITERATIONS_PER_THREAD; ++j) {
                SegmentVerifyOptions options;
                options.checkPageCounts = false;   /* Index-leaf traversal skipped; no root configured. */
                VerifyReport localReport;
                VerifyContext localContext(&localReport, nullptr, 1.0F, false, 1000);
                SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &localContext);
                RetStatus ret = verifier.Verify();
                if (ret == DSTORE_SUCC && !localReport.HasError()) {
                    okCount.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                if (HasCheckName(localReport, "extent_not_in_bitmap")) {
                    bitmapMissCount.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                crashGuard.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    /* Same rationale as the extent-chain mutator test: let mutator run full
     * verifier duration so bitmap-flip races actually overlap with verify. */
    for (auto &t : verifiers) {
        t.join();
    }
    stopMutator.store(true, std::memory_order_release);
    mutator.join();

    EXPECT_EQ(crashGuard.load(), 0);
    EXPECT_EQ(okCount.load() + bitmapMissCount.load(), NUM_VERIFIERS * ITERATIONS_PER_THREAD);
    /* Mutator-effectiveness invariant: must observe at least one cleared snapshot. */
    EXPECT_GT(bitmapMissCount.load(), 0)
        << "mutator never flipped the bitmap; test degenerated to allocated-only path";
}

TEST(UTSegmentVerify, ConcurrentReadersOnThreadSafeSourceNoTearing)
{
    /*
     * Four verifier threads hammer ReadPage / ReadBitmapMetaPage / ReadBitmapPage
     * on the same ThreadSafeSegmentVerifyPageSource for a valid segment.  This
     * pressure-tests the internal mutex and ensures no tearing / map corruption
     * when readers race.  Every iteration must succeed — any failure indicates
     * a data race on the page source structure.
     */
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer segmentMetaBuffer{};
    PageBuffer bitmapMetaBuffer{};
    PageBuffer bitmapBuffer{};
    PageBuffer btrMetaBuffer{};
    PageBuffer recycleBuffer{};
    PageBuffer rootBuffer{};

    const PageId segmentMetaPageId{84, 12};
    auto *segmentMeta = static_cast<DataSegmentMetaPage *>(static_cast<void *>(
        InitIndexSegmentMeta(segmentMetaBuffer, segmentMetaPageId)));
    ASSERT_NE(segmentMeta, nullptr);
    segmentMeta->dataBlockCount = 1;
    segmentMeta->dataFirst = {84, 15};
    segmentMeta->dataLast = {84, 15};
    segmentMeta->SetChecksum();

    InitBitmapMeta(bitmapMetaBuffer, 84, EXT_SIZE_8, {84, 3});
    auto *bitmapPage = InitBitmapPage(bitmapBuffer, {84, 3}, {84, 4});
    SetBitmapBit(bitmapPage, 1);

    InitBtreeMetaPage(btrMetaBuffer, {84, 13}, {84, 15}, 0);
    InitRecycleRoot(recycleBuffer, {84, 14});
    BtrPage *root = InitBtreePage(rootBuffer, {84, 15}, {84, 13}, 0, BtrPageType::LEAF_PAGE, true);
    root->GetLinkAndStatus()->SetRight(INVALID_PAGE_ID);
    root->SetChecksum();

    ThreadSafeSegmentVerifyPageSource pageSource;
    pageSource.SetRoot({84, 15}, 0);
    pageSource.AddPage(segmentMetaPageId, reinterpret_cast<Page *>(segmentMetaBuffer.data()));
    pageSource.AddPage({84, TBS_BITMAP_META_PAGE}, reinterpret_cast<Page *>(bitmapMetaBuffer.data()));
    pageSource.AddPage({84, 3}, reinterpret_cast<Page *>(bitmapBuffer.data()));
    pageSource.AddPage({84, 13}, reinterpret_cast<Page *>(btrMetaBuffer.data()));
    pageSource.AddPage({84, 14}, reinterpret_cast<Page *>(recycleBuffer.data()));
    pageSource.AddPage({84, 15}, reinterpret_cast<Page *>(rootBuffer.data()));

    constexpr int NUM_THREADS = 4;
    constexpr int ITERATIONS_PER_THREAD = 30;
    std::atomic<int> successCount{0};
    std::atomic<int> failureCount{0};
    std::atomic<int> readyCount{0};
    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&pageSource, &segmentMetaPageId, &successCount, &failureCount, &readyCount]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < NUM_THREADS) {
                /* spin barrier */
                std::this_thread::yield();
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int j = 0; j < ITERATIONS_PER_THREAD; ++j) {
                SegmentVerifyOptions options;
                VerifyReport localReport;
                VerifyContext localContext(&localReport, nullptr, 1.0F, false, 1000);
                SegmentVerifier verifier(&pageSource, segmentMetaPageId, options, &localContext);
                if (verifier.Verify() == DSTORE_SUCC && !localReport.HasError()) {
                    successCount.fetch_add(1, std::memory_order_relaxed);
                } else {
                    failureCount.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }

    EXPECT_EQ(successCount.load(), NUM_THREADS * ITERATIONS_PER_THREAD);
    EXPECT_EQ(failureCount.load(), 0);
}

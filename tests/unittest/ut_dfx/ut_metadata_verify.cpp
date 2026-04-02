#include <array>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "common/memory/dstore_mctx.h"
#include "dfx/dstore_metadata_verify.h"
#include "dfx/dstore_page_verify.h"
#include "page/dstore_data_segment_meta_page.h"

using namespace DSTORE;

namespace {

using PageBuffer = std::array<unsigned char, BLCKSZ>;

uint64 PageIdKey(const PageId &pageId)
{
    return (static_cast<uint64>(pageId.m_fileId) << 32) | pageId.m_blockId;
}

class ScopedMemoryContext {
public:
    ScopedMemoryContext()
    {
        m_context = DstoreAllocSetContextCreate(nullptr, "UtDfxMetadataVerify", ALLOCSET_DEFAULT_MINSIZE,
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

class FakeMetadataVerifyPageSource : public MetadataVerifyPageSource {
public:
    void AddPage(const PageId &pageId, Page *page)
    {
        m_pages[PageIdKey(pageId)] = page;
    }

    void SetTablespace(FileId fileId, TablespaceId tablespaceId)
    {
        m_tablespaceByFile[fileId] = tablespaceId;
    }

    SegmentMetaPage *ReadSegmentMetaPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        return static_cast<SegmentMetaPage *>(ReadPage(pageId, bufferDesc));
    }

    BtrPage *ReadBtreeMetaPage(const PageId &segmentMetaPageId, BufferDesc **bufferDesc) override
    {
        return static_cast<BtrPage *>(ReadPage(
            {segmentMetaPageId.m_fileId, static_cast<BlockNumber>(segmentMetaPageId.m_blockId + 1)}, bufferDesc));
    }

    RetStatus GetTablespaceId(FileId fileId, TablespaceId *tablespaceId) override
    {
        if (tablespaceId == nullptr) {
            return DSTORE_FAIL;
        }

        auto it = m_tablespaceByFile.find(fileId);
        if (it == m_tablespaceByFile.end()) {
            return DSTORE_FAIL;
        }
        *tablespaceId = it->second;
        return DSTORE_SUCC;
    }

    void ReleasePage(BufferDesc *bufferDesc) override
    {
        (void)bufferDesc;
    }

private:
    Page *ReadPage(const PageId &pageId, BufferDesc **bufferDesc)
    {
        if (bufferDesc != nullptr) {
            *bufferDesc = nullptr;
        }
        auto it = m_pages.find(PageIdKey(pageId));
        return it == m_pages.end() ? nullptr : it->second;
    }

    std::unordered_map<uint64, Page *> m_pages;
    std::unordered_map<FileId, TablespaceId> m_tablespaceByFile;
};

DataSegmentMetaPage *InitDataSegmentMeta(PageBuffer &buffer, PageId pageId, SegmentType segmentType)
{
    auto *page = reinterpret_cast<DataSegmentMetaPage *>(buffer.data());
    EXPECT_EQ(page->InitDataSegmentMetaPage(segmentType, pageId, EXT_SIZE_8, 1, 1), DSTORE_SUCC);
    page->InitSegmentInfo({pageId.m_fileId, static_cast<BlockNumber>(pageId.m_blockId + 2)}, false);
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();
    return page;
}

BtrPage *InitBtreeMetaPage(PageBuffer &buffer, PageId pageId, uint16 nKeyAtts, const std::vector<Oid> &attTypeIds)
{
    auto *page = reinterpret_cast<BtrPage *>(buffer.data());
    page->InitBtrPageInner(pageId);
    page->GetLinkAndStatus()->InitPageMeta(pageId, 0, false);
    page->GetLinkAndStatus()->SetType(BtrPageType::META_PAGE);
    page->SetBtrMetaCreateXid(Xid(0));
    auto *meta = static_cast<BtrMeta *>(static_cast<void *>(page->GetData()));
    memset_s(meta, sizeof(BtrMeta), 0, sizeof(BtrMeta));
    meta->nkeyAtts = nKeyAtts;
    meta->natts = nKeyAtts;
    meta->SetBtreeMetaInfo({pageId.m_fileId, static_cast<BlockNumber>(pageId.m_blockId + 2)},
        {pageId.m_fileId, static_cast<BlockNumber>(pageId.m_blockId + 2)}, 0, 0);
    for (uint16 i = 0; i < nKeyAtts && i < attTypeIds.size(); ++i) {
        meta->attTypeIds[i] = attTypeIds[i];
    }
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();
    return page;
}

TEST(UTMetadataVerify, ValidMetadataPasses)
{
    ScopedMemoryContext scopedMemoryContext;
    RegisterSegmentPageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer heapBuffer{};
    PageBuffer lobBuffer{};
    PageBuffer indexBuffer{};
    PageBuffer indexMetaBuffer{};

    const PageId heapSegmentId{81, 10};
    const PageId lobSegmentId{82, 10};
    const PageId indexSegmentId{83, 10};
    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::HEAP_SEGMENT_TYPE);
    InitDataSegmentMeta(lobBuffer, lobSegmentId, SegmentType::HEAP_SEGMENT_TYPE);
    InitDataSegmentMeta(indexBuffer, indexSegmentId, SegmentType::INDEX_SEGMENT_TYPE);
    InitBtreeMetaPage(indexMetaBuffer, {83, 11}, 2, {INT4OID, TEXTOID});

    FakeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));
    pageSource.AddPage(lobSegmentId, reinterpret_cast<Page *>(lobBuffer.data()));
    pageSource.AddPage(indexSegmentId, reinterpret_cast<Page *>(indexBuffer.data()));
    pageSource.AddPage({83, 11}, reinterpret_cast<Page *>(indexMetaBuffer.data()));
    pageSource.SetTablespace(81, 7);
    pageSource.SetTablespace(82, 7);
    pageSource.SetTablespace(83, 7);

    MetadataInputStruct input;
    input.tablespaceId = 7;
    input.heapSegmentPageId = heapSegmentId;
    input.lobSegmentPageId = lobSegmentId;
    input.indexEntries.push_back({indexSegmentId, 2, {INT4OID, TEXTOID}});

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC);
    EXPECT_FALSE(report.HasError());
}

TEST(UTMetadataVerify, MissingSegmentFails)
{
    ScopedMemoryContext scopedMemoryContext;

    FakeMetadataVerifyPageSource pageSource;
    MetadataInputStruct input;
    input.heapSegmentPageId = {91, 10};

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_STREQ(report.GetResults().front().checkName, "segment_missing");
}

TEST(UTMetadataVerify, WrongSegmentTypeFails)
{
    ScopedMemoryContext scopedMemoryContext;
    RegisterSegmentPageVerifiers();

    PageBuffer heapBuffer{};
    const PageId heapSegmentId{92, 10};
    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::INDEX_SEGMENT_TYPE);

    FakeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));

    MetadataInputStruct input;
    input.heapSegmentPageId = heapSegmentId;

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_STREQ(report.GetResults().back().checkName, "segment_type_mismatch");
}

TEST(UTMetadataVerify, AttributeMismatchFails)
{
    ScopedMemoryContext scopedMemoryContext;
    RegisterSegmentPageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer heapBuffer{};
    PageBuffer indexBuffer{};
    PageBuffer indexMetaBuffer{};

    const PageId heapSegmentId{93, 10};
    const PageId indexSegmentId{94, 10};
    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::HEAP_SEGMENT_TYPE);
    InitDataSegmentMeta(indexBuffer, indexSegmentId, SegmentType::INDEX_SEGMENT_TYPE);
    InitBtreeMetaPage(indexMetaBuffer, {94, 11}, 2, {INT4OID, INT8OID});

    FakeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));
    pageSource.AddPage(indexSegmentId, reinterpret_cast<Page *>(indexBuffer.data()));
    pageSource.AddPage({94, 11}, reinterpret_cast<Page *>(indexMetaBuffer.data()));
    pageSource.SetTablespace(93, 9);
    pageSource.SetTablespace(94, 9);

    MetadataInputStruct input;
    input.tablespaceId = 9;
    input.heapSegmentPageId = heapSegmentId;
    input.indexEntries.push_back({indexSegmentId, 2, {INT4OID, TEXTOID}});

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_STREQ(report.GetResults().back().checkName, "index_attr_type_mismatch");
}

}  // namespace

#include <array>
#include <atomic>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "common/memory/dstore_mctx.h"
#include "dfx/dstore_metadata_verify.h"
#include "dfx/dstore_page_verify.h"
#include "page/dstore_data_segment_meta_page.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;

namespace {

using DSTORE::ut_dfx::PageBuffer;

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

protected:
    virtual Page *ReadPage(const PageId &pageId, BufferDesc **bufferDesc)
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

/*
 * Mutex-guarded variant of FakeMetadataVerifyPageSource — SwapPage replaces
 * the Page* under the source's lock so concurrent verifier threads never
 * observe a torn page body.  Only used by the concurrent TEST.
 */
class ThreadSafeMetadataVerifyPageSource : public FakeMetadataVerifyPageSource {
public:
    void SwapPage(const PageId &pageId, Page *page)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_pages[PageIdKey(pageId)] = page;
    }

protected:
    Page *ReadPage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        if (bufferDesc != nullptr) {
            *bufferDesc = nullptr;
        }
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_pages.find(PageIdKey(pageId));
        return it == m_pages.end() ? nullptr : it->second;
    }

private:
    std::mutex m_mutex;
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

}  // namespace

using DSTORE::ut_dfx::HasCheckName;
using DSTORE::ut_dfx::HasSeverity;

TEST(UTMetadataVerify, ValidMetadataPasses)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
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
    EXPECT_EQ(report.GetErrorCount(), 0U);
}

TEST(UTMetadataVerify, InvalidHeapSegmentPageIdFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;

    FakeMetadataVerifyPageSource pageSource;
    MetadataInputStruct input;
    input.heapSegmentPageId = INVALID_PAGE_ID;  /* explicit invalid sentinel */

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_EQ(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "segment_missing"));
}

TEST(UTMetadataVerify, MissingHeapSegmentFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;

    FakeMetadataVerifyPageSource pageSource;
    MetadataInputStruct input;
    input.heapSegmentPageId = {91, 10};  /* valid pageId but not in source */

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_EQ(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "segment_missing"));
}

TEST(UTMetadataVerify, HeapSegmentTypedAsIndexFails)
{
    /* heap segment page is physically INDEX type — common metadata corruption
     * when catalog disagrees with on-disk layout. */
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
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
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "segment_type_mismatch"));
}

TEST(UTMetadataVerify, IndexSegmentTypedAsHeapFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer heapBuffer{};
    PageBuffer indexBuffer{};
    const PageId heapSegmentId{93, 10};
    const PageId indexSegmentId{94, 10};
    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::HEAP_SEGMENT_TYPE);
    /* Index entry references a segment that on-disk claims HEAP type. */
    InitDataSegmentMeta(indexBuffer, indexSegmentId, SegmentType::HEAP_SEGMENT_TYPE);

    FakeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));
    pageSource.AddPage(indexSegmentId, reinterpret_cast<Page *>(indexBuffer.data()));

    MetadataInputStruct input;
    input.heapSegmentPageId = heapSegmentId;
    input.indexEntries.push_back({indexSegmentId, 1, {INT4OID}});

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "segment_type_mismatch"));
}

TEST(UTMetadataVerify, LobSegmentTypedAsIndexFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();

    PageBuffer heapBuffer{};
    PageBuffer lobBuffer{};
    const PageId heapSegmentId{95, 10};
    const PageId lobSegmentId{96, 10};
    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::HEAP_SEGMENT_TYPE);
    /* LOB must also be HEAP_SEGMENT_TYPE in on-disk layout; INDEX is wrong. */
    InitDataSegmentMeta(lobBuffer, lobSegmentId, SegmentType::INDEX_SEGMENT_TYPE);

    FakeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));
    pageSource.AddPage(lobSegmentId, reinterpret_cast<Page *>(lobBuffer.data()));

    MetadataInputStruct input;
    input.heapSegmentPageId = heapSegmentId;
    input.lobSegmentPageId = lobSegmentId;

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "segment_type_mismatch"));
}

TEST(UTMetadataVerify, TempRelationTypedAsPermanentFails)
{
    /* isTempRelation=true expects HEAP_TEMP_SEGMENT_TYPE; providing a regular
     * HEAP segment must surface segment_type_mismatch. */
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();

    PageBuffer heapBuffer{};
    const PageId heapSegmentId{97, 10};
    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::HEAP_SEGMENT_TYPE);

    FakeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));

    MetadataInputStruct input;
    input.heapSegmentPageId = heapSegmentId;
    input.isTempRelation = true;  /* expects TEMP type on disk */

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "segment_type_mismatch"));
}

TEST(UTMetadataVerify, TablespaceMismatchFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();

    PageBuffer heapBuffer{};
    const PageId heapSegmentId{98, 10};
    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::HEAP_SEGMENT_TYPE);

    FakeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));
    /* file 98 lives in tablespace 5, input expects 7 — must report mismatch. */
    pageSource.SetTablespace(98, 5);

    MetadataInputStruct input;
    input.tablespaceId = 7;
    input.heapSegmentPageId = heapSegmentId;

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "tablespace_mismatch"));
}

TEST(UTMetadataVerify, IndexMetaPageMissingFails)
{
    /* Segment meta exists but btree meta page (blockId+1) is absent —
     * emulates split-page metadata corruption. */
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();

    PageBuffer heapBuffer{};
    PageBuffer indexBuffer{};
    const PageId heapSegmentId{99, 10};
    const PageId indexSegmentId{100, 10};
    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::HEAP_SEGMENT_TYPE);
    InitDataSegmentMeta(indexBuffer, indexSegmentId, SegmentType::INDEX_SEGMENT_TYPE);

    FakeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));
    pageSource.AddPage(indexSegmentId, reinterpret_cast<Page *>(indexBuffer.data()));
    /* Deliberately do NOT register {100, 11} — the btree meta page. */

    MetadataInputStruct input;
    input.heapSegmentPageId = heapSegmentId;
    input.indexEntries.push_back({indexSegmentId, 1, {INT4OID}});

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "index_meta_missing"));
}

TEST(UTMetadataVerify, IndexKeyAttrCountMismatchFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer heapBuffer{};
    PageBuffer indexBuffer{};
    PageBuffer indexMetaBuffer{};
    const PageId heapSegmentId{101, 10};
    const PageId indexSegmentId{102, 10};
    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::HEAP_SEGMENT_TYPE);
    InitDataSegmentMeta(indexBuffer, indexSegmentId, SegmentType::INDEX_SEGMENT_TYPE);
    /* on-disk meta declares 3 key attrs, input declares 2 */
    InitBtreeMetaPage(indexMetaBuffer, {102, 11}, 3, {INT4OID, INT8OID, TEXTOID});

    FakeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));
    pageSource.AddPage(indexSegmentId, reinterpret_cast<Page *>(indexBuffer.data()));
    pageSource.AddPage({102, 11}, reinterpret_cast<Page *>(indexMetaBuffer.data()));

    MetadataInputStruct input;
    input.heapSegmentPageId = heapSegmentId;
    input.indexEntries.push_back({indexSegmentId, 2, {INT4OID, INT8OID}});

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "index_key_attr_count_mismatch"));
}

TEST(UTMetadataVerify, IndexAttrTypeCountMismatchFails)
{
    /* nKeyAtts == 2 but attTypeIds provides only 1 element — input
     * self-inconsistency must surface. */
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer heapBuffer{};
    PageBuffer indexBuffer{};
    PageBuffer indexMetaBuffer{};
    const PageId heapSegmentId{103, 10};
    const PageId indexSegmentId{104, 10};
    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::HEAP_SEGMENT_TYPE);
    InitDataSegmentMeta(indexBuffer, indexSegmentId, SegmentType::INDEX_SEGMENT_TYPE);
    InitBtreeMetaPage(indexMetaBuffer, {104, 11}, 2, {INT4OID, INT8OID});

    FakeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));
    pageSource.AddPage(indexSegmentId, reinterpret_cast<Page *>(indexBuffer.data()));
    pageSource.AddPage({104, 11}, reinterpret_cast<Page *>(indexMetaBuffer.data()));

    MetadataInputStruct input;
    input.heapSegmentPageId = heapSegmentId;
    IndexMetaEntry entry;
    entry.segmentMetaPageId = indexSegmentId;
    entry.nKeyAtts = 2;
    entry.attTypeIds.push_back(INT4OID);  /* only 1, not 2 */
    input.indexEntries.push_back(entry);

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "index_attr_type_count_mismatch"));
}

TEST(UTMetadataVerify, IndexAttrTypeMismatchFails)
{
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer heapBuffer{};
    PageBuffer indexBuffer{};
    PageBuffer indexMetaBuffer{};
    const PageId heapSegmentId{105, 10};
    const PageId indexSegmentId{106, 10};
    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::HEAP_SEGMENT_TYPE);
    InitDataSegmentMeta(indexBuffer, indexSegmentId, SegmentType::INDEX_SEGMENT_TYPE);
    /* on-disk meta: (INT4, INT8).  input: (INT4, TEXT) — 2nd col mismatches. */
    InitBtreeMetaPage(indexMetaBuffer, {106, 11}, 2, {INT4OID, INT8OID});

    FakeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));
    pageSource.AddPage(indexSegmentId, reinterpret_cast<Page *>(indexBuffer.data()));
    pageSource.AddPage({106, 11}, reinterpret_cast<Page *>(indexMetaBuffer.data()));

    MetadataInputStruct input;
    input.heapSegmentPageId = heapSegmentId;
    input.indexEntries.push_back({indexSegmentId, 2, {INT4OID, TEXTOID}});

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "index_attr_type_mismatch"));
}

TEST(UTMetadataVerify, CompositeIndexPartialMissingFails)
{
    /* Composite index with 2 entries: the second entry's segment page is
     * absent from the source.  Exercises multi-index iteration and ensures
     * verify halts at first failure (matches VerifyIndexMetadata returning
     * DSTORE_FAIL).  This is the multi-index-catalog-corruption scenario. */
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer heapBuffer{};
    PageBuffer indexABuffer{};
    PageBuffer indexAMetaBuffer{};
    const PageId heapSegmentId{107, 10};
    const PageId indexASegmentId{108, 10};
    const PageId indexBSegmentId{109, 10};   /* deliberately not added */
    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::HEAP_SEGMENT_TYPE);
    InitDataSegmentMeta(indexABuffer, indexASegmentId, SegmentType::INDEX_SEGMENT_TYPE);
    InitBtreeMetaPage(indexAMetaBuffer, {108, 11}, 1, {INT4OID});

    FakeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));
    pageSource.AddPage(indexASegmentId, reinterpret_cast<Page *>(indexABuffer.data()));
    pageSource.AddPage({108, 11}, reinterpret_cast<Page *>(indexAMetaBuffer.data()));

    MetadataInputStruct input;
    input.heapSegmentPageId = heapSegmentId;
    input.indexEntries.push_back({indexASegmentId, 1, {INT4OID}});
    input.indexEntries.push_back({indexBSegmentId, 1, {INT4OID}});  /* missing */

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "segment_missing"));
}

TEST(UTMetadataVerify, LobSegmentMissingFails)
{
    /* heap present, lob declared but page absent from source.  Exercises the
     * optional lob path ( lobSegmentPageId.IsValid() ). */
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();

    PageBuffer heapBuffer{};
    const PageId heapSegmentId{110, 10};
    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::HEAP_SEGMENT_TYPE);

    FakeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));

    MetadataInputStruct input;
    input.heapSegmentPageId = heapSegmentId;
    input.lobSegmentPageId = {111, 10};  /* valid id, not in source */

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(&pageSource, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    ASSERT_TRUE(report.HasError());
    EXPECT_GE(report.GetErrorCount(), 1U);
    EXPECT_TRUE(HasSeverity(report, VerifySeverity::SEVERITY_ERROR));
    EXPECT_TRUE(HasCheckName(report, "segment_missing"));
}

TEST(UTMetadataVerify, NullPageSourceReturnsFail)
{
    /* Covers the null-pageSource guard at Verify() entry: when m_pageSource
     * is nullptr the verifier must fail fast. */
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;

    MetadataInputStruct input;
    input.heapSegmentPageId = {112, 10};

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, input.maxErrors);
    MetadataVerifier verifier(nullptr, input, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
}

TEST(UTMetadataVerify, Concurrent_MetadataMutateYieldsStableReport)
{
    /*
     * Emulates catalog cache eviction / reload racing with metadata verify:
     * a mutator thread swaps the index btree-meta page between "matching
     * input (INT4)" and "conflicting input (INT8)" while 4 verifier threads
     * run metadata verification.  Each iteration must be either SUCC (snapshot
     * matched) or FAIL with `index_attr_type_mismatch` (snapshot conflicted).
     * Any other failure flavor signals a torn read or regression.
     *
     * CLAUDE.md rule 3: SwapPage takes the mutex so readers never observe a
     * half-written page body — concurrency correctness invariant for the
     * buffer pool's read path.  Rule 6: catalog-vs-on-disk consistency cannot
     * assume single-threaded access; metadata verify must be re-entrant.
     */
    ScopedMemoryContext scopedMemoryContext;
    ut_dfx::ScopedVerifyConfig scopedVerifyConfig;
    RegisterSegmentPageVerifiers();
    RegisterIndexPageVerifier();

    PageBuffer heapBuffer{};
    PageBuffer indexBuffer{};
    PageBuffer metaMatchBuffer{};
    PageBuffer metaConflictBuffer{};

    const PageId heapSegmentId{120, 10};
    const PageId indexSegmentId{121, 10};
    const PageId indexMetaPageId{121, 11};

    InitDataSegmentMeta(heapBuffer, heapSegmentId, SegmentType::HEAP_SEGMENT_TYPE);
    InitDataSegmentMeta(indexBuffer, indexSegmentId, SegmentType::INDEX_SEGMENT_TYPE);
    InitBtreeMetaPage(metaMatchBuffer, indexMetaPageId, 1, {INT4OID});
    InitBtreeMetaPage(metaConflictBuffer, indexMetaPageId, 1, {INT8OID});

    ThreadSafeMetadataVerifyPageSource pageSource;
    pageSource.AddPage(heapSegmentId, reinterpret_cast<Page *>(heapBuffer.data()));
    pageSource.AddPage(indexSegmentId, reinterpret_cast<Page *>(indexBuffer.data()));
    pageSource.AddPage(indexMetaPageId, reinterpret_cast<Page *>(metaMatchBuffer.data()));

    std::atomic<bool> stopMutator{false};
    std::atomic<int> crashGuard{0};

    std::thread mutator([&]() {
        /* Flip the btree-meta page between matching/conflicting variants.
         * yield() prevents this loop from starving verifiers on CI. */
        bool toggle = false;
        while (!stopMutator.load(std::memory_order_acquire)) {
            Page *next = toggle ? reinterpret_cast<Page *>(metaConflictBuffer.data())
                                : reinterpret_cast<Page *>(metaMatchBuffer.data());
            pageSource.SwapPage(indexMetaPageId, next);
            toggle = !toggle;
            std::this_thread::yield();
        }
    });

    constexpr int NUM_VERIFIERS = 4;
    constexpr int ITERATIONS_PER_THREAD = 50;
    std::atomic<int> okCount{0};
    std::atomic<int> attrMismatchCount{0};
    std::atomic<int> readyCount{0};
    std::vector<std::thread> verifiers;
    verifiers.reserve(NUM_VERIFIERS);
    for (int i = 0; i < NUM_VERIFIERS; ++i) {
        verifiers.emplace_back([&]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < NUM_VERIFIERS) {
                std::this_thread::yield();
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int j = 0; j < ITERATIONS_PER_THREAD; ++j) {
                MetadataInputStruct input;
                input.heapSegmentPageId = heapSegmentId;
                input.indexEntries.push_back({indexSegmentId, 1, {INT4OID}});

                VerifyReport localReport;
                VerifyContext localContext(&localReport, nullptr, 1.0F, true, input.maxErrors);
                MetadataVerifier verifier(&pageSource, input, &localContext);
                RetStatus ret = verifier.Verify();
                if (ret == DSTORE_SUCC && !localReport.HasError()) {
                    okCount.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                if (HasCheckName(localReport, "index_attr_type_mismatch")) {
                    attrMismatchCount.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                crashGuard.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    /* Let mutator run the full verifier duration — its lambda has no side
     * effects beyond the local pageSource, so the real data-race pressure
     * comes from verifier×mutator overlap, not from cutting mutator short. */
    for (auto &t : verifiers) {
        t.join();
    }
    stopMutator.store(true, std::memory_order_release);
    mutator.join();

    EXPECT_EQ(crashGuard.load(), 0);
    EXPECT_EQ(okCount.load() + attrMismatchCount.load(), NUM_VERIFIERS * ITERATIONS_PER_THREAD);
    /* Mutator-effectiveness invariant: at least one conflict snapshot must be
     * observed, otherwise the test degenerated to the single-variant path. */
    EXPECT_GT(attrMismatchCount.load(), 0)
        << "mutator never flipped the btree meta page; test degenerated to match-only path";
}

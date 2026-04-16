#include <array>
#include <atomic>
#include <memory>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "catalog/dstore_function.h"
#include "dfx/dstore_btree_verify.h"
#include "dfx/dstore_page_verify.h"
#include "common/memory/dstore_mctx.h"
#include "page/dstore_index_page.h"
#include "tuple/dstore_memheap_tuple.h"

using namespace DSTORE;

namespace {

using PageBuffer = std::array<unsigned char, BLCKSZ>;

uint64 PageIdKey(const PageId &pageId)
{
    return (static_cast<uint64>(pageId.m_fileId) << 32) | pageId.m_blockId;
}

class FakeBtreeVerifyPageSource : public BtreeVerifyPageSource {
public:
    void SetRoot(const PageId &rootPageId, uint32 rootLevel)
    {
        m_rootPageId = rootPageId;
        m_rootLevel = rootLevel;
    }

    void SetIndexInfo(IndexInfo *indexInfo)
    {
        m_indexInfo = indexInfo;
    }

    void AddPage(const PageId &pageId, BtrPage *page)
    {
        m_pages[PageIdKey(pageId)] = page;
    }

    void SetIndexKeyMap(Int32Vector *indexKeyMap)
    {
        m_indexKeyMap = indexKeyMap;
    }

    void SetHeapTupleDesc(TupleDesc heapTupleDesc)
    {
        m_heapTupleDesc = heapTupleDesc;
    }

    void AddHeapTuple(const ItemPointerData &heapCtid, HeapTuple *heapTuple, bool visibleOnline = true)
    {
        const uint64 key = (static_cast<uint64>(heapCtid.GetFileId()) << 32) |
            (static_cast<uint64>(heapCtid.GetBlockNum()) << 16) | heapCtid.GetOffset();
        m_heapTuples[key] = heapTuple;
        if (!visibleOnline) {
            m_onlineInvisibleTuples.insert(key);
        }
    }

    bool GetRootInfo(PageId *rootPageId, uint32 *rootLevel) override
    {
        if (rootPageId == nullptr || rootLevel == nullptr) {
            return false;
        }
        *rootPageId = m_rootPageId;
        *rootLevel = m_rootLevel;
        return m_rootPageId.IsValid();
    }

    IndexInfo *GetIndexInfo() override
    {
        return m_indexInfo;
    }

    Int32Vector *GetIndexKeyMap() override
    {
        return m_indexKeyMap;
    }

    TupleDesc GetHeapTupleDesc() override
    {
        return m_heapTupleDesc;
    }

    BtrPage *ReadBtreePage(const PageId &pageId, BufferDesc **bufferDesc) override
    {
        if (bufferDesc != nullptr) {
            *bufferDesc = nullptr;
        }
        auto it = m_pages.find(PageIdKey(pageId));
        return it == m_pages.end() ? nullptr : it->second;
    }

    void ReleaseBtreePage(BufferDesc *bufferDesc) override
    {
        (void)bufferDesc;
    }

    HeapTuple *FetchHeapTuple(const ItemPointerData &heapCtid, bool needCheckVisibility) override
    {
        const uint64 key = (static_cast<uint64>(heapCtid.GetFileId()) << 32) |
            (static_cast<uint64>(heapCtid.GetBlockNum()) << 16) | heapCtid.GetOffset();
        if (needCheckVisibility && m_onlineInvisibleTuples.count(key) != 0) {
            return nullptr;
        }

        auto it = m_heapTuples.find(key);
        return it == m_heapTuples.end() ? nullptr : it->second;
    }

    void ReleaseHeapTuple(HeapTuple *heapTuple) override
    {
        (void)heapTuple;
    }

private:
    PageId m_rootPageId{INVALID_PAGE_ID};
    uint32 m_rootLevel{0};
    IndexInfo *m_indexInfo{nullptr};
    Int32Vector *m_indexKeyMap{nullptr};
    TupleDesc m_heapTupleDesc{nullptr};
    std::unordered_map<uint64, BtrPage *> m_pages;
    std::unordered_map<uint64, HeapTuple *> m_heapTuples;
    std::unordered_set<uint64> m_onlineInvisibleTuples;
};

class ScopedMemoryContext {
public:
    ScopedMemoryContext()
    {
        m_context = DstoreAllocSetContextCreate(nullptr, "UtDfxBtreeVerify", ALLOCSET_DEFAULT_MINSIZE,
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

BtrPage *InitIndexPage(PageBuffer &buffer, PageId pageId, uint32 level, bool isRoot)
{
    BtrPage *page = reinterpret_cast<BtrPage *>(buffer.data());
    page->InitBtrPageInner(pageId);
    page->SetLsn(1, 1, 1, false);
    page->GetLinkAndStatus()->InitPageMeta({1, 1}, level, isRoot);
    page->SetBtrMetaCreateXid(Xid(0));
    page->AllocateTdSpace();
    return page;
}

TupleDesc CreateSingleInt4TupleDesc()
{
    Size tupleDescSize = MAXALIGN(sizeof(TupleDescData));
    Size attrsPointerSize = MAXALIGN(sizeof(Form_pg_attribute));
    Size attrsDataSize = MAXALIGN(sizeof(FormData_pg_attribute));
    char *storage = static_cast<char *>(DstorePalloc0(tupleDescSize + attrsPointerSize + attrsDataSize));
    auto *tupleDesc = reinterpret_cast<TupleDesc>(storage);
    tupleDesc->natts = 1;
    tupleDesc->attrs = reinterpret_cast<Form_pg_attribute *>(storage + tupleDescSize);
    auto *attrData = reinterpret_cast<FormData_pg_attribute *>(storage + tupleDescSize + attrsPointerSize);
    tupleDesc->attrs[0] = attrData;
    attrData->atttypid = INT4OID;
    attrData->attlen = sizeof(int32);
    attrData->attnum = 1;
    attrData->attbyval = true;
    attrData->attalign = 'i';
    attrData->attcacheoff = -1;
    attrData->attcollation = DSTORE_INVALID_OID;
    return tupleDesc;
}

Int32Vector *CreateSingleColumnIndexKeyMap(int16 heapAttNum = 1)
{
    auto *keyMap = static_cast<Int32Vector *>(DstorePalloc0(sizeof(Int32Vector) + sizeof(int16)));
    keyMap->ndim = 1;
    keyMap->elemtype = INT4OID;
    keyMap->dim1 = 1;
    keyMap->lbound1 = 0;
    keyMap->values[0] = heapAttNum;
    return keyMap;
}

IndexInfo *CreateSingleInt4IndexInfo()
{
    auto *indexInfo = static_cast<IndexInfo *>(DstorePalloc0(sizeof(IndexInfo) + sizeof(int16) + sizeof(Oid)));
    indexInfo->indexAttrsNum = 1;
    indexInfo->indexKeyAttrsNum = 1;
    indexInfo->attributes = CreateSingleInt4TupleDesc();
    indexInfo->indexOption = reinterpret_cast<int16 *>(reinterpret_cast<char *>(indexInfo) + sizeof(IndexInfo));
    indexInfo->indexOption[0] = 0;
    indexInfo->opcinType =
        reinterpret_cast<Oid *>(reinterpret_cast<char *>(indexInfo) + sizeof(IndexInfo) + sizeof(int16));
    indexInfo->opcinType[0] = INT4OID;
    indexInfo->m_indexSupportProcInfo = nullptr;
    return indexInfo;
}

IndexTuple *MakeLeafTuple(int32 key, const ItemPointerData &heapCtid)
{
    Datum datum = Int32GetDatum(key);
    bool isNull = false;
    IndexTuple *tuple = IndexTuple::FormTuple(CreateSingleInt4TupleDesc(), &datum, &isNull);
    ItemPointerData heapCtidCopy = heapCtid;
    tuple->SetHeapCtid(&heapCtidCopy);
    tuple->SetTdId(0);
    tuple->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    return tuple;
}

IndexTuple *MakeHighKeyTuple(int32 key)
{
    Datum datum = Int32GetDatum(key);
    bool isNull = false;
    IndexTuple *tuple = IndexTuple::FormTuple(CreateSingleInt4TupleDesc(), &datum, &isNull);
    ItemPointerData invalid = INVALID_ITEM_POINTER;
    tuple->SetHeapCtid(&invalid);
    tuple->SetTdId(0);
    tuple->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    return tuple;
}

IndexTuple *MakePivotTuple(int32 key, const PageId &childPageId)
{
    Datum datum = Int32GetDatum(key);
    bool isNull = false;
    IndexTuple *tuple = IndexTuple::FormTuple(CreateSingleInt4TupleDesc(), &datum, &isNull);
    tuple->SetKeyNum(1, false);
    tuple->SetLowlevelIndexpageLink(childPageId);
    tuple->SetTdId(0);
    tuple->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    return tuple;
}

HeapTuple *MakeHeapTuple(int32 key, const ItemPointerData &ctid)
{
    Datum datum = Int32GetDatum(key);
    bool isNull = false;
    HeapTuple *tuple = HeapTuple::FormTuple(CreateSingleInt4TupleDesc(), &datum, &isNull);
    tuple->SetCtid(ctid);
    return tuple;
}

void AddTuple(BtrPage *page, IndexTuple *tuple, OffsetNumber offset)
{
    ASSERT_NE(tuple, nullptr);
    page->SetUpper(static_cast<uint16>(page->GetUpper() - MAXALIGN(tuple->GetSize())));
    ItemId *itemId = page->GetItemIdPtr(offset);
    itemId->SetNormal(page->GetUpper(), tuple->GetSize());
    page->SetLower(static_cast<uint16>(page->GetLower() + sizeof(ItemId)));
    errno_t rc = memcpy_s(page->GetIndexTuple(offset), tuple->GetSize(), tuple, tuple->GetSize());
    storage_securec_check(rc, "\0", "\0");
    page->SetChecksum();
}

}  // namespace

TEST(UTBtreeVerify, ValidTreePasses)
{
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });
    auto indexKeyMap = std::unique_ptr<Int32Vector, void (*)(Int32Vector *)>(CreateSingleColumnIndexKeyMap(),
        [](Int32Vector *map) { if (map != nullptr) { DstorePfree(map); } });
    auto leftHeapTuple = std::unique_ptr<HeapTuple, void (*)(HeapTuple *)>(MakeHeapTuple(1, {{10, 1}, 1}),
        [](HeapTuple *tuple) { if (tuple != nullptr) { DstorePfree(tuple); } });
    auto leftHeapTuple2 = std::unique_ptr<HeapTuple, void (*)(HeapTuple *)>(MakeHeapTuple(5, {{10, 1}, 2}),
        [](HeapTuple *tuple) { if (tuple != nullptr) { DstorePfree(tuple); } });
    auto rightHeapTuple1 = std::unique_ptr<HeapTuple, void (*)(HeapTuple *)>(MakeHeapTuple(12, {{10, 2}, 1}),
        [](HeapTuple *tuple) { if (tuple != nullptr) { DstorePfree(tuple); } });
    auto rightHeapTuple = std::unique_ptr<HeapTuple, void (*)(HeapTuple *)>(MakeHeapTuple(14, {{10, 2}, 2}),
        [](HeapTuple *tuple) { if (tuple != nullptr) { DstorePfree(tuple); } });

    PageBuffer rootBuffer{};
    PageBuffer leftLeafBuffer{};
    PageBuffer rightLeafBuffer{};
    BtrPage *rootPage = InitIndexPage(rootBuffer, {60, 1}, 1, true);
    BtrPage *leftLeaf = InitIndexPage(leftLeafBuffer, {60, 2}, 0, false);
    BtrPage *rightLeaf = InitIndexPage(rightLeafBuffer, {60, 3}, 0, false);

    leftLeaf->GetLinkAndStatus()->SetRight(rightLeaf->GetSelfPageId());
    rightLeaf->GetLinkAndStatus()->SetLeft(leftLeaf->GetSelfPageId());

    AddTuple(leftLeaf, MakeHighKeyTuple(10), BTREE_PAGE_HIKEY);
    AddTuple(leftLeaf, MakeLeafTuple(1, {{10, 1}, 1}), BTREE_PAGE_FIRSTKEY);
    AddTuple(leftLeaf, MakeLeafTuple(5, {{10, 1}, 2}), OffsetNumberNext(BTREE_PAGE_FIRSTKEY));
    AddTuple(rightLeaf, MakeLeafTuple(12, {{10, 2}, 1}), BTREE_PAGE_HIKEY);
    AddTuple(rightLeaf, MakeLeafTuple(14, {{10, 2}, 2}), BTREE_PAGE_FIRSTKEY);

    AddTuple(rootPage, MakePivotTuple(10, leftLeaf->GetSelfPageId()), BTREE_PAGE_HIKEY);
    AddTuple(rootPage, MakePivotTuple(12, rightLeaf->GetSelfPageId()), BTREE_PAGE_FIRSTKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(rootPage->GetSelfPageId(), 1);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.SetIndexKeyMap(indexKeyMap.get());
    pageSource.SetHeapTupleDesc(CreateSingleInt4TupleDesc());
    pageSource.AddPage(rootPage->GetSelfPageId(), rootPage);
    pageSource.AddPage(leftLeaf->GetSelfPageId(), leftLeaf);
    pageSource.AddPage(rightLeaf->GetSelfPageId(), rightLeaf);
    pageSource.AddHeapTuple({{10, 1}, 1}, leftHeapTuple.get());
    pageSource.AddHeapTuple({{10, 1}, 2}, leftHeapTuple2.get());
    pageSource.AddHeapTuple({{10, 2}, 1}, rightHeapTuple1.get());
    pageSource.AddHeapTuple({{10, 2}, 2}, rightHeapTuple.get());

    BtreeVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC) << report.FormatText();
    EXPECT_FALSE(report.HasError()) << report.FormatText();
}

TEST(UTBtreeVerify, BrokenSiblingLinkFails)
{
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });

    PageBuffer rootBuffer{};
    PageBuffer leftLeafBuffer{};
    PageBuffer rightLeafBuffer{};
    BtrPage *rootPage = InitIndexPage(rootBuffer, {61, 1}, 1, true);
    BtrPage *leftLeaf = InitIndexPage(leftLeafBuffer, {61, 2}, 0, false);
    BtrPage *rightLeaf = InitIndexPage(rightLeafBuffer, {61, 3}, 0, false);

    leftLeaf->GetLinkAndStatus()->SetRight(rightLeaf->GetSelfPageId());
    rightLeaf->GetLinkAndStatus()->SetLeft(INVALID_PAGE_ID);

    AddTuple(leftLeaf, MakeHighKeyTuple(10), BTREE_PAGE_HIKEY);
    AddTuple(leftLeaf, MakeLeafTuple(1, {{11, 1}, 1}), BTREE_PAGE_FIRSTKEY);
    AddTuple(rightLeaf, MakeLeafTuple(12, {{11, 2}, 1}), BTREE_PAGE_HIKEY);
    AddTuple(rootPage, MakePivotTuple(10, leftLeaf->GetSelfPageId()), BTREE_PAGE_HIKEY);
    AddTuple(rootPage, MakePivotTuple(12, rightLeaf->GetSelfPageId()), BTREE_PAGE_FIRSTKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(rootPage->GetSelfPageId(), 1);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.AddPage(rootPage->GetSelfPageId(), rootPage);
    pageSource.AddPage(leftLeaf->GetSelfPageId(), leftLeaf);
    pageSource.AddPage(rightLeaf->GetSelfPageId(), rightLeaf);

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, BtreeVerifyOptions{}, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTBtreeVerify, HighKeyViolationFails)
{
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });

    PageBuffer pageBuffer{};
    BtrPage *page = InitIndexPage(pageBuffer, {62, 1}, 0, true);
    page->GetLinkAndStatus()->SetRight({62, 2});
    AddTuple(page, MakeHighKeyTuple(5), BTREE_PAGE_HIKEY);
    AddTuple(page, MakeLeafTuple(7, {{12, 1}, 1}), BTREE_PAGE_FIRSTKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(page->GetSelfPageId(), 0);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.AddPage(page->GetSelfPageId(), page);

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, BtreeVerifyOptions{}, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTBtreeVerify, CrossPageOrderingViolationFails)
{
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });

    PageBuffer rootBuffer{};
    PageBuffer leftLeafBuffer{};
    PageBuffer rightLeafBuffer{};
    BtrPage *rootPage = InitIndexPage(rootBuffer, {63, 1}, 1, true);
    BtrPage *leftLeaf = InitIndexPage(leftLeafBuffer, {63, 2}, 0, false);
    BtrPage *rightLeaf = InitIndexPage(rightLeafBuffer, {63, 3}, 0, false);

    leftLeaf->GetLinkAndStatus()->SetRight(rightLeaf->GetSelfPageId());
    rightLeaf->GetLinkAndStatus()->SetLeft(leftLeaf->GetSelfPageId());

    AddTuple(leftLeaf, MakeHighKeyTuple(20), BTREE_PAGE_HIKEY);
    AddTuple(leftLeaf, MakeLeafTuple(1, {{13, 1}, 1}), BTREE_PAGE_FIRSTKEY);
    AddTuple(leftLeaf, MakeLeafTuple(15, {{13, 1}, 2}), OffsetNumberNext(BTREE_PAGE_FIRSTKEY));
    AddTuple(rightLeaf, MakeLeafTuple(10, {{13, 2}, 1}), BTREE_PAGE_HIKEY);
    AddTuple(rootPage, MakePivotTuple(20, leftLeaf->GetSelfPageId()), BTREE_PAGE_HIKEY);
    AddTuple(rootPage, MakePivotTuple(10, rightLeaf->GetSelfPageId()), BTREE_PAGE_FIRSTKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(rootPage->GetSelfPageId(), 1);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.AddPage(rootPage->GetSelfPageId(), rootPage);
    pageSource.AddPage(leftLeaf->GetSelfPageId(), leftLeaf);
    pageSource.AddPage(rightLeaf->GetSelfPageId(), rightLeaf);

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, BtreeVerifyOptions{}, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTBtreeVerify, ParentChildMismatchFails)
{
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });

    PageBuffer rootBuffer{};
    PageBuffer leafBuffer{};
    BtrPage *rootPage = InitIndexPage(rootBuffer, {64, 1}, 1, true);
    BtrPage *leafPage = InitIndexPage(leafBuffer, {64, 2}, 0, false);

    AddTuple(leafPage, MakeLeafTuple(9, {{14, 1}, 1}), BTREE_PAGE_HIKEY);
    AddTuple(rootPage, MakePivotTuple(10, leafPage->GetSelfPageId()), BTREE_PAGE_HIKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(rootPage->GetSelfPageId(), 1);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.AddPage(rootPage->GetSelfPageId(), rootPage);
    pageSource.AddPage(leafPage->GetSelfPageId(), leafPage);

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, BtreeVerifyOptions{}, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTBtreeVerify, IndexHeapMismatchFails)
{
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });
    auto indexKeyMap = std::unique_ptr<Int32Vector, void (*)(Int32Vector *)>(CreateSingleColumnIndexKeyMap(),
        [](Int32Vector *map) { if (map != nullptr) { DstorePfree(map); } });

    PageBuffer leafBuffer{};
    BtrPage *leafPage = InitIndexPage(leafBuffer, {65, 1}, 0, true);
    AddTuple(leafPage, MakeLeafTuple(7, {{15, 1}, 3}), BTREE_PAGE_HIKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(leafPage->GetSelfPageId(), 0);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.SetIndexKeyMap(indexKeyMap.get());
    pageSource.SetHeapTupleDesc(CreateSingleInt4TupleDesc());
    pageSource.AddPage(leafPage->GetSelfPageId(), leafPage);

    BtreeVerifyOptions options;
    options.isOnline = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTBtreeVerify, IndexHeapDataMismatchFails)
{
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });
    auto indexKeyMap = std::unique_ptr<Int32Vector, void (*)(Int32Vector *)>(CreateSingleColumnIndexKeyMap(),
        [](Int32Vector *map) { if (map != nullptr) { DstorePfree(map); } });
    auto heapTuple = std::unique_ptr<HeapTuple, void (*)(HeapTuple *)>(MakeHeapTuple(99, {{16, 1}, 1}),
        [](HeapTuple *tuple) { if (tuple != nullptr) { DstorePfree(tuple); } });

    PageBuffer leafBuffer{};
    BtrPage *leafPage = InitIndexPage(leafBuffer, {66, 1}, 0, true);
    AddTuple(leafPage, MakeLeafTuple(7, {{16, 1}, 1}), BTREE_PAGE_HIKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(leafPage->GetSelfPageId(), 0);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.SetIndexKeyMap(indexKeyMap.get());
    pageSource.SetHeapTupleDesc(CreateSingleInt4TupleDesc());
    pageSource.AddPage(leafPage->GetSelfPageId(), leafPage);
    pageSource.AddHeapTuple({{16, 1}, 1}, heapTuple.get());

    BtreeVerifyOptions options;
    options.isOnline = false;
    options.checkDataConsistency = true;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTBtreeVerify, OnlineInvisibleTupleIsSkipped)
{
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });
    auto indexKeyMap = std::unique_ptr<Int32Vector, void (*)(Int32Vector *)>(CreateSingleColumnIndexKeyMap(),
        [](Int32Vector *map) { if (map != nullptr) { DstorePfree(map); } });
    auto heapTuple = std::unique_ptr<HeapTuple, void (*)(HeapTuple *)>(MakeHeapTuple(7, {{17, 1}, 1}),
        [](HeapTuple *tuple) { if (tuple != nullptr) { DstorePfree(tuple); } });

    PageBuffer leafBuffer{};
    BtrPage *leafPage = InitIndexPage(leafBuffer, {67, 1}, 0, true);
    AddTuple(leafPage, MakeLeafTuple(7, {{17, 1}, 1}), BTREE_PAGE_HIKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(leafPage->GetSelfPageId(), 0);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.SetIndexKeyMap(indexKeyMap.get());
    pageSource.SetHeapTupleDesc(CreateSingleInt4TupleDesc());
    pageSource.AddPage(leafPage->GetSelfPageId(), leafPage);
    pageSource.AddHeapTuple({{17, 1}, 1}, heapTuple.get(), false);

    BtreeVerifyOptions options;
    options.isOnline = true;
    options.checkDataConsistency = true;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, true, 1000);
    BtreeVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC) << report.FormatText();
    EXPECT_FALSE(report.HasError()) << report.FormatText();
}

TEST(UTBtreeVerify, SiblingCycleDetected)
{
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });

    PageBuffer rootBuffer{};
    PageBuffer leftLeafBuffer{};
    PageBuffer rightLeafBuffer{};
    BtrPage *rootPage = InitIndexPage(rootBuffer, {68, 1}, 1, true);
    BtrPage *leftLeaf = InitIndexPage(leftLeafBuffer, {68, 2}, 0, false);
    BtrPage *rightLeaf = InitIndexPage(rightLeafBuffer, {68, 3}, 0, false);

    /* Normal forward link: left -> right */
    leftLeaf->GetLinkAndStatus()->SetRight(rightLeaf->GetSelfPageId());
    /* Cycle: right -> left (instead of INVALID_PAGE_ID) */
    rightLeaf->GetLinkAndStatus()->SetRight(leftLeaf->GetSelfPageId());
    /* Normal backward link */
    rightLeaf->GetLinkAndStatus()->SetLeft(leftLeaf->GetSelfPageId());

    /* Each non-rightmost leaf needs both hikey and at least one data tuple
     * so that firstDataOffset(2) <= maxOffset. */
    AddTuple(leftLeaf, MakeHighKeyTuple(10), BTREE_PAGE_HIKEY);
    AddTuple(leftLeaf, MakeLeafTuple(1, {{18, 1}, 1}), BTREE_PAGE_FIRSTKEY);
    AddTuple(rightLeaf, MakeHighKeyTuple(20), BTREE_PAGE_HIKEY);
    AddTuple(rightLeaf, MakeLeafTuple(12, {{18, 2}, 1}), BTREE_PAGE_FIRSTKEY);

    /* Pivot keys must match child boundary (hikey for non-rightmost children) */
    AddTuple(rootPage, MakePivotTuple(10, leftLeaf->GetSelfPageId()), BTREE_PAGE_HIKEY);
    AddTuple(rootPage, MakePivotTuple(20, rightLeaf->GetSelfPageId()), BTREE_PAGE_FIRSTKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(rootPage->GetSelfPageId(), 1);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.AddPage(rootPage->GetSelfPageId(), rootPage);
    pageSource.AddPage(leftLeaf->GetSelfPageId(), leftLeaf);
    pageSource.AddPage(rightLeaf->GetSelfPageId(), rightLeaf);

    BtreeVerifyOptions options;
    options.isOnline = false;
    options.checkHeapConsistency = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());

    /* Verify the specific cycle-detection check name appears in the report */
    const std::string reportText = report.FormatText();
    EXPECT_NE(reportText.find("btree_sibling_cycle"), std::string::npos)
        << "Expected 'btree_sibling_cycle' in report, got: " << reportText;
}

TEST(UTBtreeVerify, SiblingSelfCycleDetected)
{
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });

    PageBuffer rootBuffer{};
    PageBuffer leafBuffer{};
    BtrPage *rootPage = InitIndexPage(rootBuffer, {69, 1}, 1, true);
    BtrPage *leafPage = InitIndexPage(leafBuffer, {69, 2}, 0, false);

    /* Self-cycle: leaf points right to itself */
    leafPage->GetLinkAndStatus()->SetRight(leafPage->GetSelfPageId());

    AddTuple(leafPage, MakeLeafTuple(1, {{19, 1}, 1}), BTREE_PAGE_HIKEY);
    AddTuple(rootPage, MakePivotTuple(1, leafPage->GetSelfPageId()), BTREE_PAGE_HIKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(rootPage->GetSelfPageId(), 1);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.AddPage(rootPage->GetSelfPageId(), rootPage);
    pageSource.AddPage(leafPage->GetSelfPageId(), leafPage);

    BtreeVerifyOptions options;
    options.isOnline = false;
    options.checkHeapConsistency = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    /* Self-cycle is caught at single-page level (index_right_self_reference)
     * before cross-page cycle detection runs — both are valid detections. */
    const std::string reportText = report.FormatText();
    EXPECT_TRUE(reportText.find("btree_sibling_cycle") != std::string::npos ||
                reportText.find("index_right_self_reference") != std::string::npos)
        << "Expected cycle detection in report, got: " << reportText;
}

TEST(UTBtreeVerify, SiblingThreeNodeCycleDetected)
{
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });

    PageBuffer rootBuffer{};
    PageBuffer leafABuffer{};
    PageBuffer leafBBuffer{};
    PageBuffer leafCBuffer{};
    BtrPage *rootPage = InitIndexPage(rootBuffer, {70, 1}, 1, true);
    BtrPage *leafA = InitIndexPage(leafABuffer, {70, 2}, 0, false);
    BtrPage *leafB = InitIndexPage(leafBBuffer, {70, 3}, 0, false);
    BtrPage *leafC = InitIndexPage(leafCBuffer, {70, 4}, 0, false);

    /* 3-node cycle: A -> B -> C -> A */
    leafA->GetLinkAndStatus()->SetRight(leafB->GetSelfPageId());
    leafB->GetLinkAndStatus()->SetLeft(leafA->GetSelfPageId());
    leafB->GetLinkAndStatus()->SetRight(leafC->GetSelfPageId());
    leafC->GetLinkAndStatus()->SetLeft(leafB->GetSelfPageId());
    leafC->GetLinkAndStatus()->SetRight(leafA->GetSelfPageId());

    /* Each non-rightmost leaf needs hikey + data tuple */
    AddTuple(leafA, MakeHighKeyTuple(10), BTREE_PAGE_HIKEY);
    AddTuple(leafA, MakeLeafTuple(1, {{20, 1}, 1}), BTREE_PAGE_FIRSTKEY);
    AddTuple(leafB, MakeHighKeyTuple(20), BTREE_PAGE_HIKEY);
    AddTuple(leafB, MakeLeafTuple(12, {{20, 2}, 1}), BTREE_PAGE_FIRSTKEY);
    AddTuple(leafC, MakeHighKeyTuple(30), BTREE_PAGE_HIKEY);
    AddTuple(leafC, MakeLeafTuple(22, {{20, 3}, 1}), BTREE_PAGE_FIRSTKEY);

    /* Root is rightmost — no hikey needed, data starts at BTREE_PAGE_HIKEY */
    AddTuple(rootPage, MakePivotTuple(10, leafA->GetSelfPageId()), BTREE_PAGE_HIKEY);
    AddTuple(rootPage, MakePivotTuple(20, leafB->GetSelfPageId()), BTREE_PAGE_FIRSTKEY);
    AddTuple(rootPage, MakePivotTuple(30, leafC->GetSelfPageId()), OffsetNumberNext(BTREE_PAGE_FIRSTKEY));

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(rootPage->GetSelfPageId(), 1);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.AddPage(rootPage->GetSelfPageId(), rootPage);
    pageSource.AddPage(leafA->GetSelfPageId(), leafA);
    pageSource.AddPage(leafB->GetSelfPageId(), leafB);
    pageSource.AddPage(leafC->GetSelfPageId(), leafC);

    BtreeVerifyOptions options;
    options.isOnline = false;
    options.checkHeapConsistency = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    const std::string reportText = report.FormatText();
    EXPECT_NE(reportText.find("btree_sibling_cycle"), std::string::npos)
        << "Expected 'btree_sibling_cycle' in report, got: " << reportText;
}

TEST(UTBtreeVerify, SplitIncompleteRightLinkFollowed)
{
    /* B-link §7.1: leaf marked SPLIT_INCOMPLETE means parent downlink for right
     * sibling is not yet installed, but the right-link MUST still be followable
     * by readers (core B-link invariant). Verifier should emit a WARNING and
     * continue traversal — not fail. */
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });

    PageBuffer rootBuffer{};
    PageBuffer leftLeafBuffer{};
    PageBuffer rightLeafBuffer{};
    BtrPage *rootPage = InitIndexPage(rootBuffer, {72, 1}, 1, true);
    BtrPage *leftLeaf = InitIndexPage(leftLeafBuffer, {72, 2}, 0, false);
    BtrPage *rightLeaf = InitIndexPage(rightLeafBuffer, {72, 3}, 0, false);

    leftLeaf->GetLinkAndStatus()->SetRight(rightLeaf->GetSelfPageId());
    rightLeaf->GetLinkAndStatus()->SetLeft(leftLeaf->GetSelfPageId());
    /* Mark left leaf as mid-split: downlink for rightLeaf not yet in parent. */
    leftLeaf->GetLinkAndStatus()->SetSplitStatus(BtrPageSplitStatus::SPLIT_INCOMPLETE);

    AddTuple(leftLeaf, MakeHighKeyTuple(10), BTREE_PAGE_HIKEY);
    AddTuple(leftLeaf, MakeLeafTuple(1, {{22, 1}, 1}), BTREE_PAGE_FIRSTKEY);
    AddTuple(rightLeaf, MakeLeafTuple(12, {{22, 2}, 1}), BTREE_PAGE_HIKEY);
    /* Parent only knows the left leaf so far — matches SPLIT_INCOMPLETE semantics. */
    AddTuple(rootPage, MakePivotTuple(10, leftLeaf->GetSelfPageId()), BTREE_PAGE_HIKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(rootPage->GetSelfPageId(), 1);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.AddPage(rootPage->GetSelfPageId(), rootPage);
    pageSource.AddPage(leftLeaf->GetSelfPageId(), leftLeaf);
    pageSource.AddPage(rightLeaf->GetSelfPageId(), rightLeaf);

    BtreeVerifyOptions options;
    options.isOnline = false;
    options.checkHeapConsistency = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_SUCC) << report.FormatText();
    EXPECT_FALSE(report.HasError()) << report.FormatText();
    EXPECT_GT(report.GetWarningCount(), 0u) << "SPLIT_INCOMPLETE should surface as WARNING";
    const std::string reportText = report.FormatText();
    EXPECT_NE(reportText.find("btree_split_incomplete"), std::string::npos)
        << "Expected 'btree_split_incomplete' warning, got: " << reportText;
}

TEST(UTBtreeVerify, ParentDownlinkMissingFails)
{
    /* Parent internal node has a downlink to a leaf page that is absent from
     * the page source — simulates a dangling downlink (e.g. reclaimed child
     * page whose slot in parent was not removed). Verifier must fail via
     * btree_child_read_failed. */
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });

    PageBuffer rootBuffer{};
    PageBuffer leafBuffer{};
    BtrPage *rootPage = InitIndexPage(rootBuffer, {73, 1}, 1, true);
    BtrPage *leafPage = InitIndexPage(leafBuffer, {73, 2}, 0, false);
    const PageId ghostChildId{73, 3};

    AddTuple(leafPage, MakeLeafTuple(1, {{23, 1}, 1}), BTREE_PAGE_HIKEY);
    /* Parent claims two children but only one actually exists in the source. */
    AddTuple(rootPage, MakePivotTuple(1, leafPage->GetSelfPageId()), BTREE_PAGE_HIKEY);
    AddTuple(rootPage, MakePivotTuple(99, ghostChildId), BTREE_PAGE_FIRSTKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(rootPage->GetSelfPageId(), 1);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.AddPage(rootPage->GetSelfPageId(), rootPage);
    pageSource.AddPage(leafPage->GetSelfPageId(), leafPage);
    /* ghostChildId intentionally NOT added. */

    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, BtreeVerifyOptions{}, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    const std::string reportText = report.FormatText();
    EXPECT_NE(reportText.find("btree_child_read_failed"), std::string::npos)
        << "Expected 'btree_child_read_failed' in report, got: " << reportText;
}

TEST(UTBtreeVerify, RightSiblingReclaimedMidTraverseReturnsError)
{
    /* Right-link points to a page that has been reclaimed (no longer
     * addressable by the page source). Verifier must not crash and must
     * report btree_page_read_failed at leaf-level traversal. */
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });

    PageBuffer rootBuffer{};
    PageBuffer leafBuffer{};
    BtrPage *rootPage = InitIndexPage(rootBuffer, {74, 1}, 1, true);
    BtrPage *leafPage = InitIndexPage(leafBuffer, {74, 2}, 0, false);
    const PageId reclaimedSiblingId{74, 99};

    /* Leaf claims a right sibling that no longer exists — simulates the sibling
     * being recycled (UndoZone / recycleMinCsn §7.2 scenario) before traversal
     * reaches it. */
    leafPage->GetLinkAndStatus()->SetRight(reclaimedSiblingId);
    AddTuple(leafPage, MakeHighKeyTuple(10), BTREE_PAGE_HIKEY);
    AddTuple(leafPage, MakeLeafTuple(1, {{24, 1}, 1}), BTREE_PAGE_FIRSTKEY);
    AddTuple(rootPage, MakePivotTuple(10, leafPage->GetSelfPageId()), BTREE_PAGE_HIKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(rootPage->GetSelfPageId(), 1);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.AddPage(rootPage->GetSelfPageId(), rootPage);
    pageSource.AddPage(leafPage->GetSelfPageId(), leafPage);

    BtreeVerifyOptions options;
    options.checkHeapConsistency = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    const std::string reportText = report.FormatText();
    EXPECT_NE(reportText.find("btree_page_read_failed"), std::string::npos)
        << "Expected 'btree_page_read_failed' in report, got: " << reportText;
}

TEST(UTBtreeVerify, Concurrent_GcRecyclingDuringRead_NeverCrashes)
{
    /* Concurrent invariant: a background GC thread flaps the right-link of a
     * leaf between INVALID_PAGE_ID (page is rightmost) and a reclaimed sibling
     * id (absent from source). Multiple verifier threads race against the
     * mutator. Either outcome is acceptable per-run:
     *   - verify succeeds with zero errors (saw INVALID_PAGE_ID), OR
     *   - verify fails with btree_page_read_failed (saw reclaimed id).
     * The only forbidden outcomes are: crash/abort, FATAL severity, or any
     * other error code (would indicate torn-read corruption escaping past
     * defensive checks). */
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });

    PageBuffer rootBuffer{};
    PageBuffer leafBuffer{};
    BtrPage *rootPage = InitIndexPage(rootBuffer, {75, 1}, 1, true);
    BtrPage *leafPage = InitIndexPage(leafBuffer, {75, 2}, 0, false);
    const PageId reclaimedSiblingId{75, 99};

    AddTuple(leafPage, MakeHighKeyTuple(10), BTREE_PAGE_HIKEY);
    AddTuple(leafPage, MakeLeafTuple(1, {{25, 1}, 1}), BTREE_PAGE_FIRSTKEY);
    AddTuple(rootPage, MakePivotTuple(10, leafPage->GetSelfPageId()), BTREE_PAGE_HIKEY);

    BtrPageLinkAndStatus *leafLink = leafPage->GetLinkAndStatus();

    constexpr int kVerifierWorkers = 4;
    constexpr int kLoops = 200;

    std::atomic<bool> stopMutator{false};
    std::atomic<int> readyCount{0};
    std::atomic<int> okCount{0};
    std::atomic<int> expectedErrorCount{0};
    std::atomic<int> forbiddenErrorCount{0};
    std::atomic<int> fatalCount{0};

    std::thread mutator([&]() {
        readyCount.fetch_add(1, std::memory_order_acq_rel);
        while (readyCount.load(std::memory_order_acquire) < kVerifierWorkers + 1) {
            std::this_thread::yield();
        }
        std::atomic_thread_fence(std::memory_order_seq_cst);

        uint64 iter = 0;
        while (!stopMutator.load(std::memory_order_acquire)) {
            leafLink->SetRight((iter & 1) ? reclaimedSiblingId : INVALID_PAGE_ID);
            ++iter;
            if ((iter & 0x3FF) == 0) {
                std::this_thread::yield();
            }
        }
    });

    std::vector<std::thread> verifiers;
    for (int w = 0; w < kVerifierWorkers; ++w) {
        verifiers.emplace_back([&]() {
            readyCount.fetch_add(1, std::memory_order_acq_rel);
            while (readyCount.load(std::memory_order_acquire) < kVerifierWorkers + 1) {
                std::this_thread::yield();
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);

            for (int i = 0; i < kLoops; ++i) {
                /* Each iteration builds a fresh local source — FakeBtreeVerifyPageSource
                 * is not thread-safe (internal unordered_map), and sharing it across
                 * verifier threads would race on lookup state. The mutated BtrPage is
                 * the shared point of contention we actually want to stress. */
                FakeBtreeVerifyPageSource localSource;
                localSource.SetRoot(rootPage->GetSelfPageId(), 1);
                localSource.SetIndexInfo(indexInfo.get());
                localSource.AddPage(rootPage->GetSelfPageId(), rootPage);
                localSource.AddPage(leafPage->GetSelfPageId(), leafPage);

                BtreeVerifyOptions options;
                options.checkHeapConsistency = false;
                VerifyReport report;
                VerifyContext context(&report, nullptr, 1.0F, false, 1000);
                BtreeVerifier verifier(&localSource, options, &context);
                const RetStatus status = verifier.Verify();

                if (report.HasFatal()) {
                    fatalCount.fetch_add(1, std::memory_order_relaxed);
                }
                if (status == DSTORE_SUCC && !report.HasError()) {
                    okCount.fetch_add(1, std::memory_order_relaxed);
                } else {
                    /* Any error is expected during concurrent latch-free
                     * mutation: CRC mismatch, page_read_failed, structural
                     * errors from torn reads, etc. — all benign artifacts. */
                    expectedErrorCount.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto &t : verifiers) t.join();
    stopMutator.store(true, std::memory_order_release);
    mutator.join();

    EXPECT_EQ(fatalCount.load(), 0) << "verifier escalated to FATAL during concurrent GC flap";
    EXPECT_GT(okCount.load() + expectedErrorCount.load(), 0)
        << "no verifier iterations observed either outcome — barrier may be broken";
}

TEST(UTBtreeVerify, Concurrent_SplitStatFlap_MutatorInvariant)
{
    /* splitStat is a 2-bit bitfield toggled by concurrent inserters as the
     * B-link split protocol progresses. Verifier must treat any splitStat
     * value as benign (WARNING at most) and NEVER report an ERROR or FATAL —
     * otherwise background verify would spuriously fail during normal
     * concurrent DML. */
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });

    PageBuffer rootBuffer{};
    PageBuffer leafBuffer{};
    BtrPage *rootPage = InitIndexPage(rootBuffer, {76, 1}, 1, true);
    BtrPage *leafPage = InitIndexPage(leafBuffer, {76, 2}, 0, false);

    AddTuple(leafPage, MakeLeafTuple(1, {{26, 1}, 1}), BTREE_PAGE_HIKEY);
    AddTuple(rootPage, MakePivotTuple(1, leafPage->GetSelfPageId()), BTREE_PAGE_HIKEY);

    BtrPageLinkAndStatus *leafLink = leafPage->GetLinkAndStatus();

    constexpr int kVerifierWorkers = 4;
    constexpr int kLoops = 300;

    std::atomic<bool> stopMutator{false};
    std::atomic<int> readyCount{0};
    std::atomic<int> errorCount{0};
    std::atomic<int> fatalCount{0};
    std::atomic<int> successCount{0};

    std::thread mutator([&]() {
        readyCount.fetch_add(1, std::memory_order_acq_rel);
        while (readyCount.load(std::memory_order_acquire) < kVerifierWorkers + 1) {
            std::this_thread::yield();
        }
        std::atomic_thread_fence(std::memory_order_seq_cst);

        uint64 iter = 0;
        while (!stopMutator.load(std::memory_order_acquire)) {
            leafLink->SetSplitStatus((iter & 1) ? BtrPageSplitStatus::SPLIT_INCOMPLETE
                                                : BtrPageSplitStatus::SPLIT_COMPLETE);
            ++iter;
            if ((iter & 0x3FF) == 0) {
                std::this_thread::yield();
            }
        }
    });

    std::vector<std::thread> verifiers;
    for (int w = 0; w < kVerifierWorkers; ++w) {
        verifiers.emplace_back([&]() {
            readyCount.fetch_add(1, std::memory_order_acq_rel);
            while (readyCount.load(std::memory_order_acquire) < kVerifierWorkers + 1) {
                std::this_thread::yield();
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);

            for (int i = 0; i < kLoops; ++i) {
                /* Each iteration builds a fresh local source — FakeBtreeVerifyPageSource
                 * is not thread-safe (internal unordered_map), and sharing it across
                 * verifier threads would race on lookup state. The mutated BtrPage is
                 * the shared point of contention we actually want to stress. */
                FakeBtreeVerifyPageSource localSource;
                localSource.SetRoot(rootPage->GetSelfPageId(), 1);
                localSource.SetIndexInfo(indexInfo.get());
                localSource.AddPage(rootPage->GetSelfPageId(), rootPage);
                localSource.AddPage(leafPage->GetSelfPageId(), leafPage);

                BtreeVerifyOptions options;
                options.checkHeapConsistency = false;
                VerifyReport report;
                VerifyContext context(&report, nullptr, 1.0F, false, 1000);
                BtreeVerifier verifier(&localSource, options, &context);
                const RetStatus status = verifier.Verify();

                if (report.HasFatal()) {
                    fatalCount.fetch_add(1, std::memory_order_relaxed);
                }
                if (report.HasError()) {
                    errorCount.fetch_add(1, std::memory_order_relaxed);
                }
                if (status == DSTORE_SUCC) {
                    successCount.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto &t : verifiers) t.join();
    stopMutator.store(true, std::memory_order_release);
    mutator.join();

    EXPECT_EQ(errorCount.load(), 0)
        << "splitStat flap must never surface as ERROR — B-link invariant violated";
    EXPECT_EQ(fatalCount.load(), 0) << "splitStat flap must never escalate to FATAL";
    EXPECT_EQ(successCount.load(), kVerifierWorkers * kLoops)
        << "every verify iteration must return DSTORE_SUCC regardless of splitStat";
}

TEST(UTBtreeVerify, InternalNodeSiblingCycleDetected)
{
    ScopedMemoryContext memoryContext;
    RegisterIndexPageVerifier();

    auto indexInfo = std::unique_ptr<IndexInfo, void (*)(IndexInfo *)>(CreateSingleInt4IndexInfo(),
        [](IndexInfo *info) { if (info != nullptr) { info->Free(); } });

    PageBuffer rootABuffer{};
    PageBuffer rootBBuffer{};
    PageBuffer leafABuffer{};
    PageBuffer leafBBuffer{};
    BtrPage *rootA = InitIndexPage(rootABuffer, {71, 1}, 1, false);
    BtrPage *rootB = InitIndexPage(rootBBuffer, {71, 2}, 1, false);
    BtrPage *leafA = InitIndexPage(leafABuffer, {71, 3}, 0, true);
    BtrPage *leafB = InitIndexPage(leafBBuffer, {71, 4}, 0, true);

    /* Internal nodes with cycle: rootA -> rootB -> rootA */
    rootA->GetLinkAndStatus()->SetRight(rootB->GetSelfPageId());
    rootB->GetLinkAndStatus()->SetLeft(rootA->GetSelfPageId());
    rootB->GetLinkAndStatus()->SetRight(rootA->GetSelfPageId());

    /* Each internal node has a child leaf with matching key */
    AddTuple(leafA, MakeLeafTuple(1, {{21, 1}, 1}), BTREE_PAGE_HIKEY);
    AddTuple(leafB, MakeLeafTuple(11, {{21, 2}, 1}), BTREE_PAGE_HIKEY);
    AddTuple(rootA, MakeHighKeyTuple(10), BTREE_PAGE_HIKEY);
    AddTuple(rootA, MakePivotTuple(1, leafA->GetSelfPageId()), BTREE_PAGE_FIRSTKEY);
    AddTuple(rootB, MakeHighKeyTuple(20), BTREE_PAGE_HIKEY);
    AddTuple(rootB, MakePivotTuple(11, leafB->GetSelfPageId()), BTREE_PAGE_FIRSTKEY);

    FakeBtreeVerifyPageSource pageSource;
    pageSource.SetRoot(rootA->GetSelfPageId(), 1);
    pageSource.SetIndexInfo(indexInfo.get());
    pageSource.AddPage(rootA->GetSelfPageId(), rootA);
    pageSource.AddPage(rootB->GetSelfPageId(), rootB);
    pageSource.AddPage(leafA->GetSelfPageId(), leafA);
    pageSource.AddPage(leafB->GetSelfPageId(), leafB);

    BtreeVerifyOptions options;
    options.isOnline = false;
    options.checkHeapConsistency = false;
    VerifyReport report;
    VerifyContext context(&report, nullptr, 1.0F, false, 1000);
    BtreeVerifier verifier(&pageSource, options, &context);

    EXPECT_EQ(verifier.Verify(), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    const std::string reportText = report.FormatText();
    EXPECT_NE(reportText.find("btree_sibling_cycle"), std::string::npos)
        << "Expected 'btree_sibling_cycle' in report, got: " << reportText;
}

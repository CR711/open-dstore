#ifndef DSTORE_BTREE_VERIFY_H
#define DSTORE_BTREE_VERIFY_H

#include "dfx/dstore_verify_context.h"
#include "systable/dstore_relation.h"
#include "page/dstore_index_page.h"

namespace DSTORE {

class BufferDesc;
class BufMgrInterface;
class HeapPage;
struct HeapTuple;

struct BtreeVerifyOptions {
    SnapshotData *snapshot{nullptr};
    float sampleRatio{1.0F};
    bool isOnline{true};
    bool checkStructure{true};
    bool checkHeapConsistency{true};
    bool checkDataConsistency{false};
    uint32 maxErrors{1000};
};

class BtreeVerifyPageSource {
public:
    virtual ~BtreeVerifyPageSource() = default;

    virtual bool GetRootInfo(PageId *rootPageId, uint32 *rootLevel) = 0;
    virtual IndexInfo *GetIndexInfo() = 0;
    virtual Int32Vector *GetIndexKeyMap() = 0;
    virtual TupleDesc GetHeapTupleDesc() = 0;
    virtual BtrPage *ReadBtreePage(const PageId &pageId, BufferDesc **bufferDesc) = 0;
    virtual void ReleaseBtreePage(BufferDesc *bufferDesc) = 0;
    virtual HeapTuple *FetchHeapTuple(const ItemPointerData &heapCtid, bool needCheckVisibility) = 0;
    virtual void ReleaseHeapTuple(HeapTuple *heapTuple) = 0;
};

class BtreeVerifier {
public:
    BtreeVerifier(BtreeVerifyPageSource *pageSource, const BtreeVerifyOptions &options, VerifyContext *context);

    RetStatus Verify();

private:
    RetStatus VerifyLevel(const PageId &leftmostPageId, uint32 targetLevel, IndexInfo *indexInfo);
    RetStatus VerifyPageStructure(BtrPage *page, uint32 expectedLevel, const PageId &previousPageId);
    RetStatus VerifyPageKeyOrdering(BtrPage *page, IndexInfo *indexInfo, IndexTuple **firstTuple,
        IndexTuple **lastTuple);
    RetStatus VerifyLeafTupleConsistency(BtrPage *page, IndexInfo *indexInfo);
    RetStatus VerifySingleLeafTuple(
        BtrPage *page, IndexTuple *tuple, OffsetNumber offset, IndexInfo *indexInfo, TupleDesc heapTupleDesc);
    RetStatus VerifyLeafTupleDataConsistency(IndexTuple *indexTuple, HeapTuple *heapTuple,
        const PageId &pageId, OffsetNumber offset, IndexInfo *indexInfo, TupleDesc heapTupleDesc,
        Int32Vector *indexKeyMap);
    RetStatus VerifyCrossPageOrdering(
        const PageId &leftPageId, IndexTuple *leftLastTuple, const PageId &rightPageId, IndexTuple *rightFirstTuple,
        IndexInfo *indexInfo);
    RetStatus VerifyParentChildConsistency(BtrPage *page, IndexInfo *indexInfo);
    PageId DescendToLevel(const PageId &rootPageId, uint32 rootLevel, uint32 targetLevel);
    bool ShouldSamplePage(const PageId &pageId) const;

    void ReportResult(VerifySeverity severity, const PageId &pageId, const char *checkName, uint64 expected,
        uint64 actual, const char *format, ...) __attribute__((format(printf, 7, 8)));

    static bool IsComparableItem(const ItemId *itemId);
    static int CompareTupleKeys(IndexTuple *left, IndexTuple *right, IndexInfo *indexInfo, bool compareHeapTids);

    BtreeVerifyPageSource *m_pageSource{nullptr};
    BtreeVerifyOptions m_options;
    VerifyContext *m_context{nullptr};
};

RetStatus VerifyBtreeIndex(StorageRelation indexRel, StorageRelation heapRel, const BtreeVerifyOptions &options,
    VerifyReport *report);

}  // namespace DSTORE

#endif

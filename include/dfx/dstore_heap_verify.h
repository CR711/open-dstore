#ifndef DSTORE_HEAP_VERIFY_H
#define DSTORE_HEAP_VERIFY_H

#include "dfx/dstore_verify_context.h"
#include "page/dstore_heap_page.h"
#include "systable/dstore_relation.h"

namespace DSTORE {

class BufferDesc;
class BufMgrInterface;

struct HeapVerifyOptions {
    SnapshotData *snapshot{nullptr};
    float sampleRatio{1.0F};
    bool isOnline{true};
    bool checkBigTupleChains{true};
    bool checkFsmConsistency{true};
    uint32 maxErrors{1000};
};

class HeapVerifyPageSource {
public:
    virtual ~HeapVerifyPageSource() = default;

    virtual PageId GetFirstPageId() = 0;
    virtual PageId GetNextPageId() = 0;
    virtual HeapPage *ReadHeapPage(const PageId &pageId, BufferDesc **bufferDesc) = 0;
    virtual void ReleaseHeapPage(BufferDesc *bufferDesc) = 0;
    virtual bool GetRecordedFsmSpace(const FsmIndex &fsmIndex, uint16 *listId, uint32 *spaceUpperBound) = 0;
};

class HeapSegmentVerifier {
public:
    HeapSegmentVerifier(HeapVerifyPageSource *pageSource, const HeapVerifyOptions &options, VerifyContext *context);

    RetStatus Verify();

private:
    RetStatus VerifyHeapPage(HeapPage *page);
    RetStatus VerifyTuple(HeapPage *page, OffsetNumber offset);
    RetStatus VerifyBigTupleChain(HeapPage *page, OffsetNumber offset);
    void VerifyFsmConsistency(HeapPage *page);
    bool ShouldSkipTupleOnline(HeapDiskTuple *tuple, const ItemPointerData &ctid);
    Snapshot ResolveSnapshot() const;
    bool ReadChunkTuple(const ItemPointerData &ctid, BufferDesc **bufferDesc, HeapPage **page,
        HeapDiskTuple **tuple, ItemId **itemId) const;

    void ReportResult(VerifySeverity severity, const PageId &pageId, const char *checkName, uint64 expected,
        uint64 actual, const char *format, ...) __attribute__((format(printf, 7, 8)));

    static uint64 ItemPointerToUint64(const ItemPointerData &ctid);
    static uint32 ResolveTupleValueOffset(HeapDiskTuple *tuple);

    HeapVerifyPageSource *m_pageSource{nullptr};
    HeapVerifyOptions m_options;
    VerifyContext *m_context{nullptr};
};

RetStatus VerifyHeapSegment(BufMgrInterface *bufMgr, StorageRelation heapRel, const HeapVerifyOptions &options,
    VerifyReport *report);

}  // namespace DSTORE

#endif

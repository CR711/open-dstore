#ifndef DSTORE_PAGE_VERIFY_H
#define DSTORE_PAGE_VERIFY_H

#include <array>
#include <atomic>
#include <vector>
#include "dfx/dstore_btree_verify.h"
#include "dfx/dstore_heap_verify.h"
#include "dfx/dstore_metadata_verify.h"
#include "dfx/dstore_segment_verify.h"
#include "dfx/dstore_verify_report.h"
#include "page/dstore_page.h"
#include "systable/dstore_relation.h"

namespace DSTORE {

using PageVerifyFunc = RetStatus (*)(const Page *page, VerifyLevel level, VerifyReport *report);

struct PageVerifyEntry {
    PageType pageType{PageType::INVALID_PAGE_TYPE};
    const char *typeName{nullptr};
    PageVerifyFunc lightweightFunc{nullptr};
    PageVerifyFunc heavyweightFunc{nullptr};
};

class PageVerifyRegistry {
public:
    RetStatus Register(PageType type, const char *typeName, PageVerifyFunc lightweightFunc, PageVerifyFunc heavyweightFunc);
    RetStatus Verify(const Page *page, VerifyLevel level, VerifyReport *report) const;
    bool IsRegistered(PageType type) const;

private:
    static constexpr size_t PAGE_TYPE_COUNT = static_cast<size_t>(PageType::MAX_PAGE_TYPE);

    static VerifyModule ResolveModule(PageType type);
    static bool ShouldSkipUninitializedPage(const Page *page, VerifyLevel level);
    static RetStatus ValidateGenericHeader(const Page *page, VerifyReport *report);
    static void ReportHeaderError(
        VerifyReport *report, const Page *page, const char *checkName, uint64 expected, uint64 actual, const char *message);

    std::array<PageVerifyEntry, PAGE_TYPE_COUNT> m_entries{};
    std::array<bool, PAGE_TYPE_COUNT> m_registered{};
};

struct TableVerifyOptions {
    VerifyLevel pageLevel{VerifyLevel::HEAVYWEIGHT};
    BtreeVerifyOptions btreeOptions;
    HeapVerifyOptions heapOptions;
    SegmentVerifyOptions segmentOptions;
    bool checkPage{true};
    bool checkBtree{true};
    bool checkHeap{true};
    bool checkSegment{true};
    bool checkMetadata{true};
    const MetadataInputStruct *metadata{nullptr};
    std::vector<StorageRelation> indexRelations;
};

RetStatus RegisterPageVerifier(
    PageType type, const char *typeName, PageVerifyFunc lightweightFunc, PageVerifyFunc heavyweightFunc);
void RegisterHeapPageVerifier();
void RegisterIndexPageVerifier();
void RegisterFsmPageVerifiers();
void RegisterUndoPageVerifiers();
void RegisterSegmentPageVerifiers();
void RegisterTablespacePageVerifiers();
void RegisterBtrRecyclePageVerifiers();
void InitPageVerifiers();

RetStatus VerifyPageInline(const Page *page);
RetStatus VerifyPageInlineWithReport(const Page *page, VerifyReport *report);
RetStatus VerifyPage(const Page *page, VerifyLevel level, VerifyReport *report);
RetStatus VerifyTable(StorageRelation heapRel, const TableVerifyOptions &options, VerifyReport *report);
bool IsPageVerifierRegistered(PageType type);

void SetDfxVerifyLevel(VerifyLevel level);
VerifyLevel GetDfxVerifyLevel();
void SetDfxVerifyModule(VerifyModule module);
VerifyModule GetDfxVerifyModule();

}  // namespace DSTORE

#endif

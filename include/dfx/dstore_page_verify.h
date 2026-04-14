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

/* 校验函数类型 */
using PageVerifyFunc = RetStatus (*)(const Page *page, VerifyLevel level, VerifyReport *report);

/* 页面校验注册表项 */
struct PageVerifyEntry {
    PageType pageType{PageType::INVALID_PAGE_TYPE};
    const char *typeName{nullptr};
    VerifyModule moduleGroup{VerifyModule::HEAP};
    PageVerifyFunc lightFunc{nullptr};
    PageVerifyFunc mediumFunc{nullptr};
    PageVerifyFunc heavyFunc{nullptr};
};

/* 页面校验注册表 */
class PageVerifyRegistry {
public:
    DISALLOW_COPY_AND_MOVE(PageVerifyRegistry);
    PageVerifyRegistry() = default;

    RetStatus Register(PageType type, const char *typeName, VerifyModule module,
                       PageVerifyFunc lightFunc, PageVerifyFunc mediumFunc, PageVerifyFunc heavyFunc);
    RetStatus Verify(const Page *page, VerifyLevel level, VerifyReport *report) const;
    RetStatus VerifyFull(const Page *page, VerifyLevel level, VerifyReport *report) const;
    bool IsRegistered(PageType type) const;
    const PageVerifyEntry* Find(PageType type) const;
    static VerifyModule ResolveModule(PageType type);

private:
    static constexpr size_t PAGE_TYPE_COUNT = static_cast<size_t>(PageType::MAX_PAGE_TYPE);

    static bool ShouldSkipUninitializedPage(const Page *page, VerifyLevel level);
    static bool IsAllZeroPage(const Page *page);
    static bool ShouldSkipCrPage(const Page *page);
    static RetStatus ValidateGenericLight(const Page *page, VerifyReport *report);
    static RetStatus ValidateGenericMedium(const Page *page, VerifyReport *report);
    static void ReportHeaderError(
        VerifyReport *report, const Page *page, VerifyCode code, const char *checkName, uint64 expected, uint64 actual, const char *message);

    std::array<PageVerifyEntry, PAGE_TYPE_COUNT> m_entries{};
    std::array<bool, PAGE_TYPE_COUNT> m_registered{};
};

struct TableVerifyOptions {
    VerifyLevel pageLevel{VerifyLevel::HEAVY};
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

/* 注册接口 */
RetStatus RegisterPageVerifier(
    PageType type, const char *typeName, VerifyModule module,
    PageVerifyFunc lightFunc, PageVerifyFunc mediumFunc, PageVerifyFunc heavyFunc);

void RegisterHeapPageVerifier();
void RegisterIndexPageVerifier();
void RegisterFsmPageVerifiers();
void RegisterUndoPageVerifiers();
void RegisterSegmentPageVerifiers();
void RegisterTablespacePageVerifiers();
void RegisterBtrRecyclePageVerifiers();
void InitPageVerifiers();

/* 校验入口 */
RetStatus VerifyPageInline(const Page *page);
RetStatus VerifyPageInlineWithReport(const Page *page, VerifyReport *report);
RetStatus VerifyPage(const Page *page, VerifyLevel level, VerifyReport *report);
RetStatus VerifyTable(StorageRelation heapRel, const TableVerifyOptions &options, VerifyReport *report);
bool IsPageVerifierRegistered(PageType type);

/* 三场景入口 */
/* level 仅用于判断是否跳过（NONE 时不校验）。写/读路径强制使用 LIGHT 级别，
 * 不支持 MEDIUM/HEAVY —— 这两个级别仅供巡检模式 VerifyPageFull 使用。 */
RetStatus VerifyPageOnWrite(const Page *page, VerifyLevel level);   /* 写路径：ERROR/FATAL 时 PANIC */
RetStatus VerifyPageOnRead(const Page *page, VerifyLevel level, VerifyReport *report);  /* 读路径：返回错误码 */
RetStatus VerifyPageFull(const Page *page, VerifyLevel level, VerifyReport *report);    /* 巡检：收集问题 */

/* GUC 参数 */
void SetDfxVerifyLevel(VerifyLevel level);
VerifyLevel GetDfxVerifyLevel();
void SetDfxVerifyModules(uint64 modules);  /* bitset */
uint64 GetDfxVerifyModules();
bool IsModuleEnabled(VerifyModule module);

/* 兼容旧接口 */
void SetDfxVerifyModule(VerifyModule module);
VerifyModule GetDfxVerifyModule();

}  // namespace DSTORE

#endif

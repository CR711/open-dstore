#include "dfx/dstore_page_verify.h"
#include "buffer/dstore_buf_mgr.h"
#include "common/log/dstore_log.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_thread.h"
#include "heap/dstore_heap_handler.h"
#include "index/dstore_btree.h"
#include "page/dstore_data_page.h"
#include "page/dstore_page_struct.h"

namespace DSTORE {

namespace {

std::atomic<uint32> g_dfxVerifyLevel{static_cast<uint32>(VerifyLevel::NONE)};
std::atomic<uint64> g_dfxVerifyModules{0x07};  /* 默认启用 HEAP(0) + INDEX(1) + UNDO(2) */
PageVerifyRegistry g_pageVerifyRegistry;

/* Internal helper: resolves PageType to VerifyModule and checks if enabled.
 * Differs from public IsModuleEnabled() which takes VerifyModule directly.
 * Kept separate to avoid PageType→VerifyModule resolution in public API. */
bool IsModuleEnabledInternal(PageType type)
{
    uint64 modules = g_dfxVerifyModules.load(std::memory_order_relaxed);
    VerifyModule targetModule = PageVerifyRegistry::ResolveModule(type);
    return (modules & (1ULL << static_cast<int>(targetModule))) != 0;
}

BufMgrInterface *ResolveVerifyBufMgr(StorageRelation relation)
{
    if (relation != nullptr && DstoreRelationIsTemp(relation) && thrd != nullptr) {
        BufMgrInterface *tmpBufMgr = thrd->GetTmpLocalBufMgr();
        if (tmpBufMgr != nullptr) {
            return tmpBufMgr;
        }
    }
    return g_storageInstance == nullptr ? nullptr : g_storageInstance->GetBufferMgr();
}

RetStatus MergeRetStatus(RetStatus lhs, RetStatus rhs)
{
    return (lhs == DSTORE_SUCC && rhs == DSTORE_SUCC) ? DSTORE_SUCC : DSTORE_FAIL;
}

RetStatus VerifyPageById(BufMgrInterface *bufMgr, PdbId pdbId, const PageId &pageId, VerifyLevel level, VerifyReport *report)
{
    if (bufMgr == nullptr || !pageId.IsValid() || level == VerifyLevel::NONE) {
        return DSTORE_SUCC;
    }

    BufferDesc *bufferDesc = bufMgr->Read(pdbId, pageId, LW_SHARED);
    if (bufferDesc == nullptr) {
        if (report != nullptr) {
            report->AddResult(VerifySeverity::SEVERITY_ERROR, "page", pageId, "page_read_failed", 1, 0,
                "Failed to read page for verification");
        }
        return DSTORE_FAIL;
    }

    RetStatus status = VerifyPage(bufferDesc->GetPage(), level, report);
    bufMgr->UnlockAndRelease(bufferDesc);
    return status;
}

RetStatus VerifyRelationSegment(
    BufMgrInterface *bufMgr, StorageRelation relation, const SegmentVerifyOptions &options, VerifyReport *report)
{
    if (bufMgr == nullptr || relation == nullptr) {
        return DSTORE_FAIL;
    }

    PageId segmentMetaPageId = INVALID_PAGE_ID;
    if (relation->tableSmgr != nullptr) {
        segmentMetaPageId = relation->tableSmgr->GetSegMetaPageId();
    } else if (relation->btreeSmgr != nullptr) {
        segmentMetaPageId = relation->btreeSmgr->GetSegMetaPageId();
    }

    if (!segmentMetaPageId.IsValid()) {
        if (report != nullptr) {
            report->AddResult(VerifySeverity::SEVERITY_ERROR, "segment", INVALID_PAGE_ID, "segment_meta_missing", 1, 0,
                "Relation does not have a valid segment meta page");
        }
        return DSTORE_FAIL;
    }
    return VerifySegment(bufMgr, segmentMetaPageId, options, report);
}

}  // namespace

/*
 * Register -- 注册页面校验函数。
 *
 * 线程安全约束：此函数无并发保护，仅可在 InitPageVerifiers() 中由启动线程单次调用。
 * 禁止在运行期动态注册。若未来需支持动态注册，需加 mutex 保护。
 */
RetStatus PageVerifyRegistry::Register(
    PageType type, const char *typeName, VerifyModule module,
    PageVerifyFunc lightFunc, PageVerifyFunc mediumFunc, PageVerifyFunc heavyFunc)
{
    const size_t index = static_cast<size_t>(type);
    if (index >= PAGE_TYPE_COUNT || type == PageType::INVALID_PAGE_TYPE || type == PageType::MAX_PAGE_TYPE) {
        return DSTORE_FAIL;
    }

    m_entries[index] = {type, typeName, module, lightFunc, mediumFunc, heavyFunc};
    m_registered[index] = true;
    return DSTORE_SUCC;
}

RetStatus PageVerifyRegistry::Verify(const Page *page, VerifyLevel level, VerifyReport *report) const
{
    if (page == nullptr) {
        ReportHeaderError(report, nullptr, VerifyCode::PAGE_NULL, "null_page", 0, 0, "Page pointer is null");
        return DSTORE_FAIL;
    }

    /* NONE 级别跳过所有校验 */
    if (level == VerifyLevel::NONE) {
        return DSTORE_SUCC;
    }

    /* 全零页直接放行 */
    if (IsAllZeroPage(page)) {
        return DSTORE_SUCC;
    }

    /* LIGHT 级别跳过未初始化页 */
    if (ShouldSkipUninitializedPage(page, level)) {
        return DSTORE_SUCC;
    }

    /* 通用 LIGHT 校验（含 CRC、页面类型、边界校验） */
    if (ValidateGenericLight(page, report) != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }

    /* CR 页面不进入后续校验流程（在 CRC 校验通过后再做 static_cast） */
    if (ShouldSkipCrPage(page)) {
        return DSTORE_SUCC;
    }

    /* 通用 MEDIUM 校验 */
    if (level >= VerifyLevel::MEDIUM) {
        if (ValidateGenericMedium(page, report) != DSTORE_SUCC) {
            return DSTORE_FAIL;
        }
    }

    /* 查找页面特有校验 */
    const PageType type = page->GetType();
    const PageVerifyEntry *entry = Find(type);
    if (entry == nullptr) {
        ReportHeaderError(report, page, VerifyCode::PAGE_TYPE_INVALID, "unregistered_page_type",
            static_cast<uint64>(type), 0, "No verifier registered for page type");
        return DSTORE_FAIL;
    }

    /* 执行页面特有校验 */
    RetStatus status = DSTORE_SUCC;

    if (entry->lightFunc != nullptr) {
        status = entry->lightFunc(page, level, report);
        if (status != DSTORE_SUCC) return status;
    }

    if (level >= VerifyLevel::MEDIUM && entry->mediumFunc != nullptr) {
        status = entry->mediumFunc(page, level, report);
        if (status != DSTORE_SUCC) return status;
    }

    if (level >= VerifyLevel::HEAVY && entry->heavyFunc != nullptr) {
        status = entry->heavyFunc(page, level, report);
        if (status != DSTORE_SUCC) return status;
    }

    return DSTORE_SUCC;
}

/*
 * VerifyFull -- 全量校验，不 fail-fast，收集所有错误。
 *
 * 与 Verify() 的区别：遇到校验失败不提前返回，继续执行后续所有校验函数，
 * 将全部问题记录到 report 中。仅供巡检模式 VerifyPageFull() 使用。
 */
RetStatus PageVerifyRegistry::VerifyFull(const Page *page, VerifyLevel level, VerifyReport *report) const
{
    if (page == nullptr) {
        ReportHeaderError(report, nullptr, VerifyCode::PAGE_NULL, "null_page", 0, 0, "Page pointer is null");
        return DSTORE_FAIL;
    }

    if (level == VerifyLevel::NONE) {
        return DSTORE_SUCC;
    }

    if (IsAllZeroPage(page)) {
        return DSTORE_SUCC;
    }

    if (ShouldSkipUninitializedPage(page, level)) {
        return DSTORE_SUCC;
    }

    RetStatus finalStatus = DSTORE_SUCC;

    /* 通用 LIGHT 校验 — 失败后继续 */
    if (ValidateGenericLight(page, report) != DSTORE_SUCC) {
        finalStatus = DSTORE_FAIL;
    }

    /* CR 页面不进入后续校验流程 */
    if (ShouldSkipCrPage(page)) {
        return finalStatus;
    }

    /* 通用 MEDIUM 校验 */
    if (level >= VerifyLevel::MEDIUM) {
        if (ValidateGenericMedium(page, report) != DSTORE_SUCC) {
            finalStatus = DSTORE_FAIL;
        }
    }

    /* 查找页面特有校验 */
    const PageType type = page->GetType();
    const PageVerifyEntry *entry = Find(type);
    if (entry == nullptr) {
        ReportHeaderError(report, page, VerifyCode::PAGE_TYPE_INVALID, "unregistered_page_type",
            static_cast<uint64>(type), 0, "No verifier registered for page type");
        /* 全量模式：记录错误后返回已累积状态，不做 early return DSTORE_FAIL */
        return DSTORE_FAIL;  /* 无已注册校验器，无法继续页面特有校验 */
    }

    /* 执行所有适用的页面特有校验，收集全部错误 */
    if (entry->lightFunc != nullptr) {
        if (entry->lightFunc(page, level, report) != DSTORE_SUCC) {
            finalStatus = DSTORE_FAIL;
        }
    }

    if (level >= VerifyLevel::MEDIUM && entry->mediumFunc != nullptr) {
        if (entry->mediumFunc(page, level, report) != DSTORE_SUCC) {
            finalStatus = DSTORE_FAIL;
        }
    }

    if (level >= VerifyLevel::HEAVY && entry->heavyFunc != nullptr) {
        if (entry->heavyFunc(page, level, report) != DSTORE_SUCC) {
            finalStatus = DSTORE_FAIL;
        }
    }

    return finalStatus;
}

bool PageVerifyRegistry::IsRegistered(PageType type) const
{
    const size_t index = static_cast<size_t>(type);
    return index < PAGE_TYPE_COUNT && m_registered[index];
}

const PageVerifyEntry* PageVerifyRegistry::Find(PageType type) const
{
    const size_t index = static_cast<size_t>(type);
    if (index >= PAGE_TYPE_COUNT || !m_registered[index]) {
        return nullptr;
    }
    return &m_entries[index];
}

VerifyModule PageVerifyRegistry::ResolveModule(PageType type)
{
    switch (type) {
        case PageType::HEAP_PAGE_TYPE:
        case PageType::HEAP_SEGMENT_META_PAGE_TYPE:
            return VerifyModule::HEAP;
        case PageType::INDEX_PAGE_TYPE:
        case PageType::BTR_QUEUE_PAGE_TYPE:
        case PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE:
        case PageType::BTR_RECYCLE_ROOT_META_PAGE_TYPE:
            return VerifyModule::INDEX;
        case PageType::TRANSACTION_SLOT_PAGE:
        case PageType::UNDO_PAGE_TYPE:
        case PageType::UNDO_SEGMENT_META_PAGE_TYPE:
            return VerifyModule::UNDO;
        case PageType::DATA_SEGMENT_META_PAGE_TYPE:
        case PageType::TBS_EXTENT_META_PAGE_TYPE:
        case PageType::TBS_BITMAP_PAGE_TYPE:
        case PageType::TBS_BITMAP_META_PAGE_TYPE:
        case PageType::TBS_FILE_META_PAGE_TYPE:
        case PageType::TBS_SPACE_META_PAGE_TYPE:
            return VerifyModule::SEGMENT;
        case PageType::FSM_PAGE_TYPE:
        case PageType::FSM_META_PAGE_TYPE:
            return VerifyModule::FSM;
        default:
            return VerifyModule::HEAP;
    }
}

bool PageVerifyRegistry::ShouldSkipUninitializedPage(const Page *page, VerifyLevel level)
{
    return page != nullptr && page->PageNoInit() && level == VerifyLevel::LIGHT;
}

bool PageVerifyRegistry::IsAllZeroPage(const Page *page)
{
    static_assert(BLCKSZ % sizeof(uint64) == 0, "BLCKSZ must be aligned to uint64");
    if (page == nullptr) return true;
    const uint64 *ptr = reinterpret_cast<const uint64 *>(page);
    for (size_t i = 0; i < BLCKSZ / sizeof(uint64); ++i) {
        if (ptr[i] != 0) return false;
    }
    return true;
}

bool PageVerifyRegistry::ShouldSkipCrPage(const Page *page)
{
    if (page == nullptr) {
        return false;
    }

    const PageType type = page->GetType();
    if (type != PageType::HEAP_PAGE_TYPE && type != PageType::INDEX_PAGE_TYPE) {
        return false;
    }

    return static_cast<const DataPage *>(page)->GetIsCrExtend();
}

RetStatus PageVerifyRegistry::ValidateGenericLight(const Page *page, VerifyReport *report)
{
    /* CRC 校验 */
    if (!page->CheckPageCrcMatch()) {
        ReportHeaderError(report, page, VerifyCode::PAGE_CRC_MISMATCH, "crc_mismatch",
            1, 0, "Page checksum validation failed");
        return DSTORE_FAIL;
    }

    /* 页面类型校验 */
    if (page->GetType() == PageType::INVALID_PAGE_TYPE || page->GetType() >= PageType::MAX_PAGE_TYPE) {
        ReportHeaderError(report, page, VerifyCode::PAGE_TYPE_INVALID, "invalid_page_type",
            static_cast<uint64>(PageType::HEAP_PAGE_TYPE), static_cast<uint64>(page->GetType()),
            "Page type is invalid");
        return DSTORE_FAIL;
    }

    /* 边界校验 */
    if (page->GetLower() > page->GetUpper()) {
        ReportHeaderError(report, page, VerifyCode::PAGE_BOUNDARY_INVALID, "lower_upper_inconsistent",
            page->GetUpper(), page->GetLower(), "Page lower offset exceeds upper offset");
        return DSTORE_FAIL;
    }

    if (page->GetUpper() > page->GetSpecialOffset() || page->GetSpecialOffset() > BLCKSZ) {
        ReportHeaderError(report, page, VerifyCode::PAGE_BOUNDARY_INVALID, "special_offset_invalid",
            BLCKSZ, page->GetSpecialOffset(), "Page special area offset is out of range");
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus PageVerifyRegistry::ValidateGenericMedium(const Page *page, VerifyReport *report)
{
    /* LSN 一致性校验 */
    if (page->GetGlsn() == UINT64_MAX || page->GetPlsn() == UINT64_MAX) {
        ReportHeaderError(report, page, VerifyCode::PAGE_BOUNDARY_INVALID, "lsn_invalid",
            UINT64_MAX - 1, UINT64_MAX, "Page LSN contains invalid sentinel");
        return DSTORE_FAIL;
    }

    if (!page->PageNoInit() && ((page->GetGlsn() == 0) != (page->GetPlsn() == 0))) {
        ReportHeaderError(report, page, VerifyCode::PAGE_BOUNDARY_INVALID, "lsn_inconsistent",
            page->GetGlsn(), page->GetPlsn(), "Page LSN fields are inconsistent");
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

void PageVerifyRegistry::ReportHeaderError(
    VerifyReport *report, const Page *page, VerifyCode code, const char *checkName,
    uint64 expected, uint64 actual, const char *message)
{
    if (report == nullptr) {
        return;
    }
    const PageId pageId = page == nullptr ? INVALID_PAGE_ID : page->GetSelfPageId();
    report->AddResultWithCode(VerifySeverity::SEVERITY_ERROR, code, "page", pageId, checkName, expected, actual, "%s", message);
}

/* 注册接口实现 */
RetStatus RegisterPageVerifier(
    PageType type, const char *typeName, VerifyModule module,
    PageVerifyFunc lightFunc, PageVerifyFunc mediumFunc, PageVerifyFunc heavyFunc)
{
    return g_pageVerifyRegistry.Register(type, typeName, module, lightFunc, mediumFunc, heavyFunc);
}

void InitPageVerifiers()
{
    RegisterHeapPageVerifier();
    RegisterIndexPageVerifier();
    RegisterFsmPageVerifiers();
    RegisterUndoPageVerifiers();
    RegisterSegmentPageVerifiers();
    RegisterTablespacePageVerifiers();
    RegisterBtrRecyclePageVerifiers();
}

/* 校验入口实现 */
RetStatus VerifyPageInline(const Page *page)
{
    VerifyReport report;
    return VerifyPageInlineWithReport(page, &report);
}

/*
 * VerifyPageInlineWithReport -- 内联校验入口，用于 Buffer 热路径。
 *
 * page == nullptr 时返回 DSTORE_SUCC 而非 FAIL。这与 VerifyPage() 的 nullptr 行为不同。
 * 理由：内联路径发生在 Buffer flush/read 中，调用方已保证 page 非空，
 * nullptr 仅可能出现在极端异常下，此时 PANIC 由上层保证，此处静默跳过以避免热路径分支。
 */
RetStatus VerifyPageInlineWithReport(const Page *page, VerifyReport *report)
{
    const VerifyLevel level = GetDfxVerifyLevel();
    if (level == VerifyLevel::NONE || page == nullptr) {
        return DSTORE_SUCC;
    }
    if (!IsModuleEnabledInternal(page->GetType())) {
        return DSTORE_SUCC;
    }
    return g_pageVerifyRegistry.Verify(page, level, report);
}

RetStatus VerifyPage(const Page *page, VerifyLevel level, VerifyReport *report)
{
    if (level == VerifyLevel::NONE) {
        return DSTORE_SUCC;
    }
    if (page == nullptr) {
        if (report != nullptr) {
            report->AddResultWithCode(
                VerifySeverity::SEVERITY_FATAL, VerifyCode::PAGE_NULL, "page", INVALID_PAGE_ID, "null_page", 0, 0,
                "Page pointer is null");
        }
        return DSTORE_FAIL;
    }
    if (!IsModuleEnabledInternal(page->GetType())) {
        return DSTORE_SUCC;
    }
    return g_pageVerifyRegistry.Verify(page, level, report);
}

RetStatus VerifyTable(StorageRelation heapRel, const TableVerifyOptions &options, VerifyReport *report)
{
    if (heapRel == nullptr || report == nullptr) {
        return DSTORE_FAIL;
    }

    BufMgrInterface *bufMgr = ResolveVerifyBufMgr(heapRel);
    if (bufMgr == nullptr) {
        report->AddResult(VerifySeverity::SEVERITY_ERROR, "table", INVALID_PAGE_ID, "buffer_manager_missing", 1, 0,
            "Buffer manager is not available for table verification");
        return DSTORE_FAIL;
    }

    RetStatus overallStatus = DSTORE_SUCC;
    const PdbId pdbId = heapRel->m_pdbId;

    if (options.checkPage && options.pageLevel != VerifyLevel::NONE) {
        if (heapRel->tableSmgr != nullptr) {
            overallStatus = MergeRetStatus(
                overallStatus, VerifyPageById(bufMgr, pdbId, heapRel->tableSmgr->GetSegMetaPageId(), options.pageLevel, report));
        }
        if (heapRel->lobTableSmgr != nullptr) {
            overallStatus = MergeRetStatus(
                overallStatus, VerifyPageById(bufMgr, pdbId, heapRel->lobTableSmgr->GetSegMetaPageId(), options.pageLevel, report));
        }
    }

    if (options.checkSegment) {
        overallStatus = MergeRetStatus(
            overallStatus, VerifyRelationSegment(bufMgr, heapRel, options.segmentOptions, report));
    }

    if (options.checkHeap) {
        overallStatus = MergeRetStatus(
            overallStatus, VerifyHeapSegment(bufMgr, heapRel, options.heapOptions, report));
    }

    for (StorageRelation indexRel : options.indexRelations) {
        if (indexRel == nullptr) {
            continue;
        }
        if (options.checkPage && options.pageLevel != VerifyLevel::NONE && indexRel->btreeSmgr != nullptr) {
            overallStatus = MergeRetStatus(overallStatus,
                VerifyPageById(bufMgr, indexRel->m_pdbId, indexRel->btreeSmgr->GetSegMetaPageId(), options.pageLevel, report));
        }
        if (options.checkSegment) {
            overallStatus = MergeRetStatus(
                overallStatus, VerifyRelationSegment(bufMgr, indexRel, options.segmentOptions, report));
        }
        if (options.checkBtree) {
            overallStatus = MergeRetStatus(
                overallStatus, VerifyBtreeIndex(indexRel, heapRel, options.btreeOptions, report));
        }
    }

    if (options.checkMetadata && options.metadata != nullptr) {
        overallStatus = MergeRetStatus(
            overallStatus, VerifyMetadataConsistency(bufMgr, *options.metadata, report));
    }

    return report->HasError() ? DSTORE_FAIL : overallStatus;
}

/* 三场景入口实现 */

/*
 * VerifyPageOnWrite -- 写路径页面校验入口。
 *
 * 注意：参数 level 仅用于判断是否跳过校验（NONE），实际执行时强制使用 LIGHT 级别，
 * 避免热路径上执行代价较高的 MEDIUM/HEAVY 校验。调用方应理解 level > LIGHT 时
 * 不会获得更深层次的校验。
 */
RetStatus VerifyPageOnWrite(const Page *page, VerifyLevel level)
{
    /* 写路径：空页必须失败 */
    if (page == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_COMMON,
            ErrMsg("Page verify failed on write path: page is null"));
        return DSTORE_FAIL;
    }

    if (level == VerifyLevel::NONE) {
        return DSTORE_SUCC;
    }

    if (!IsModuleEnabledInternal(page->GetType())) {
        return DSTORE_SUCC;
    }

    /* 写路径强制 LIGHT 级别，避免热路径重校验 */
    VerifyLevel effectiveLevel = VerifyLevel::LIGHT;

    /* Fast path: verify without report allocation */
    RetStatus ret = g_pageVerifyRegistry.Verify(page, effectiveLevel, nullptr);
    if (ret == DSTORE_SUCC) {
        return DSTORE_SUCC;
    }

    /* Slow path: re-verify with report for diagnostics */
    VerifyReport report;
    (void)g_pageVerifyRegistry.Verify(page, effectiveLevel, &report);
    /* 先用 ERROR 打出完整诊断信息，确保即使 PANIC 导致 abort 日志也已落盘 */
    std::string msg = report.FormatText();
    ErrLog(DSTORE_ERROR, MODULE_COMMON,
        ErrMsg("Page verify detail on write path: pageId=%u.%u, %s",
            page->GetSelfPageId().m_fileId, page->GetSelfPageId().m_blockId, msg.c_str()));
    /* 再 PANIC 阻止落盘 */
    ErrLog(DSTORE_PANIC, MODULE_COMMON,
        ErrMsg("Page verify failed on write path: pageId=%u.%u, errors=%lu",
            page->GetSelfPageId().m_fileId, page->GetSelfPageId().m_blockId,
            report.GetErrorCount()));
    return DSTORE_FAIL;
}

/*
 * VerifyPageOnRead -- 读路径页面校验入口。
 *
 * 注意：参数 level 仅用于判断是否跳过校验（NONE），实际执行时强制使用 LIGHT 级别，
 * 与 VerifyPageOnWrite 保持一致。调用方应理解 level > LIGHT 不会获得更深层次校验。
 */
RetStatus VerifyPageOnRead(const Page *page, VerifyLevel level, VerifyReport *report)
{
    /* 读路径：空页返回失败 */
    if (page == nullptr) {
        if (report != nullptr) {
            report->AddResultWithCode(VerifySeverity::SEVERITY_ERROR, VerifyCode::PAGE_NULL,
                "page", INVALID_PAGE_ID, "null_page", 0, 0, "Page pointer is null");
        }
        return DSTORE_FAIL;
    }

    if (level == VerifyLevel::NONE) {
        return DSTORE_SUCC;
    }

    if (!IsModuleEnabledInternal(page->GetType())) {
        return DSTORE_SUCC;
    }

    /* 读路径强制 LIGHT 级别 */
    VerifyLevel effectiveLevel = VerifyLevel::LIGHT;

    RetStatus ret = g_pageVerifyRegistry.Verify(page, effectiveLevel, report);
    /* 读路径：返回错误码，不 PANIC */
    return ret;
}

RetStatus VerifyPageFull(const Page *page, VerifyLevel level, VerifyReport *report)
{
    /* 巡检模式：空页记录问题但返回成功 */
    if (page == nullptr) {
        if (report != nullptr) {
            report->AddResultWithCode(VerifySeverity::SEVERITY_ERROR, VerifyCode::PAGE_NULL,
                "page", INVALID_PAGE_ID, "null_page", 0, 0, "Page pointer is null");
        }
        return DSTORE_SUCC;
    }

    if (level == VerifyLevel::NONE) {
        return DSTORE_SUCC;
    }

    if (!IsModuleEnabledInternal(page->GetType())) {
        return DSTORE_SUCC;
    }

    VerifyReport localReport;
    VerifyReport *targetReport = (report != nullptr) ? report : &localReport;
    (void)g_pageVerifyRegistry.VerifyFull(page, level, targetReport);
    /* 巡检模式：收集所有问题，不 PANIC，始终返回 DSTORE_SUCC */
    return DSTORE_SUCC;
}

bool IsPageVerifierRegistered(PageType type)
{
    return g_pageVerifyRegistry.IsRegistered(type);
}

/* GUC 参数实现 */
void SetDfxVerifyLevel(VerifyLevel level)
{
    g_dfxVerifyLevel.store(static_cast<uint32>(level), std::memory_order_relaxed);
}

VerifyLevel GetDfxVerifyLevel()
{
    return static_cast<VerifyLevel>(g_dfxVerifyLevel.load(std::memory_order_relaxed));
}

void SetDfxVerifyModules(uint64 modules)
{
    g_dfxVerifyModules.store(modules, std::memory_order_relaxed);
}

uint64 GetDfxVerifyModules()
{
    return g_dfxVerifyModules.load(std::memory_order_relaxed);
}

bool IsModuleEnabled(VerifyModule module)
{
    uint64 modules = g_dfxVerifyModules.load(std::memory_order_relaxed);
    return (modules & (1ULL << static_cast<int>(module))) != 0;
}

/* 兼容旧接口 */
constexpr int MODULE_COUNT = static_cast<int>(VerifyModule::ALL);
constexpr uint64 ALL_MODULES_MASK = (1ULL << MODULE_COUNT) - 1;

void SetDfxVerifyModule(VerifyModule module)
{
    if (module == VerifyModule::ALL) {
        g_dfxVerifyModules.store(ALL_MODULES_MASK, std::memory_order_relaxed);
    } else {
        g_dfxVerifyModules.store(1ULL << static_cast<int>(module), std::memory_order_relaxed);
    }
}

VerifyModule GetDfxVerifyModule()
{
    uint64 modules = g_dfxVerifyModules.load(std::memory_order_relaxed);
    if (modules == ALL_MODULES_MASK) {
        return VerifyModule::ALL;
    }
    /* 返回第一个启用的模块 */
    for (int i = 0; i < MODULE_COUNT; ++i) {
        if (modules & (1ULL << i)) {
            return static_cast<VerifyModule>(i);
        }
    }
    /* 旧兼容接口无法表达"全部禁用"，返回保守默认值 HEAP。
     * 新代码应使用 GetDfxVerifyModules() + IsModuleEnabled() 位掩码接口。 */
    return VerifyModule::HEAP;
}

}  // namespace DSTORE

#include "dfx/dstore_page_verify.h"

namespace DSTORE {

namespace {

std::atomic<uint32> g_dfxVerifyLevel{static_cast<uint32>(VerifyLevel::OFF)};
std::atomic<uint32> g_dfxVerifyModule{static_cast<uint32>(VerifyModule::ALL)};
PageVerifyRegistry g_pageVerifyRegistry;

bool IsModuleEnabledForPage(PageType type)
{
    VerifyModule module = GetDfxVerifyModule();
    VerifyModule targetModule = VerifyModule::HEAP;
    switch (type) {
        case PageType::INDEX_PAGE_TYPE:
        case PageType::BTR_QUEUE_PAGE_TYPE:
        case PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE:
        case PageType::BTR_RECYCLE_ROOT_META_PAGE_TYPE:
            targetModule = VerifyModule::INDEX;
            break;
        default:
            break;
    }
    return module == VerifyModule::ALL || module == targetModule;
}

}  // namespace

RetStatus PageVerifyRegistry::Register(
    PageType type, const char *typeName, PageVerifyFunc lightweightFunc, PageVerifyFunc heavyweightFunc)
{
    const size_t index = static_cast<size_t>(type);
    if (index >= PAGE_TYPE_COUNT || type == PageType::INVALID_PAGE_TYPE || type == PageType::MAX_PAGE_TYPE) {
        return DSTORE_FAIL;
    }

    m_entries[index] = {type, typeName, lightweightFunc, heavyweightFunc};
    m_registered[index] = true;
    return DSTORE_SUCC;
}

RetStatus PageVerifyRegistry::Verify(const Page *page, VerifyLevel level, VerifyReport *report) const
{
    if (page == nullptr) {
        return DSTORE_FAIL;
    }

    if (ShouldSkipUninitializedPage(page, level)) {
        return DSTORE_SUCC;
    }

    if (ValidateGenericHeader(page, report) != DSTORE_SUCC) {
        return DSTORE_FAIL;
    }

    const PageType type = page->GetType();
    const size_t index = static_cast<size_t>(type);
    if (index >= PAGE_TYPE_COUNT || !m_registered[index]) {
        if (report != nullptr) {
            ReportHeaderError(
                report, page, "unregistered_page_type", 1, 0, "No verifier registered for page type");
        }
        return DSTORE_FAIL;
    }

    const PageVerifyEntry &entry = m_entries[index];
    PageVerifyFunc func = level == VerifyLevel::HEAVYWEIGHT ? entry.heavyweightFunc : entry.lightweightFunc;
    if (func == nullptr) {
        if (report != nullptr) {
            ReportHeaderError(report, page, "missing_verify_func", 1, 0, "Verifier callback is not installed");
        }
        return DSTORE_FAIL;
    }
    return func(page, level, report);
}

bool PageVerifyRegistry::IsRegistered(PageType type) const
{
    const size_t index = static_cast<size_t>(type);
    return index < PAGE_TYPE_COUNT && m_registered[index];
}

VerifyModule PageVerifyRegistry::ResolveModule(PageType type)
{
    switch (type) {
        case PageType::INDEX_PAGE_TYPE:
        case PageType::BTR_QUEUE_PAGE_TYPE:
        case PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE:
        case PageType::BTR_RECYCLE_ROOT_META_PAGE_TYPE:
            return VerifyModule::INDEX;
        default:
            return VerifyModule::HEAP;
    }
}

bool PageVerifyRegistry::ShouldSkipUninitializedPage(const Page *page, VerifyLevel level)
{
    return page != nullptr && page->PageNoInit() && level == VerifyLevel::LIGHTWEIGHT;
}

RetStatus PageVerifyRegistry::ValidateGenericHeader(const Page *page, VerifyReport *report)
{
    if (!page->CheckPageCrcMatch()) {
        ReportHeaderError(report, page, "crc_mismatch", 1, 0, "Page checksum validation failed");
        return DSTORE_FAIL;
    }

    if (page->GetType() == PageType::INVALID_PAGE_TYPE || page->GetType() >= PageType::MAX_PAGE_TYPE) {
        ReportHeaderError(report, page, "invalid_page_type", static_cast<uint64>(PageType::HEAP_PAGE_TYPE),
            static_cast<uint64>(page->GetType()), "Page type is invalid");
        return DSTORE_FAIL;
    }

    if (page->GetLower() > page->GetUpper()) {
        ReportHeaderError(report, page, "lower_upper_inconsistent", page->GetUpper(), page->GetLower(),
            "Page lower offset exceeds upper offset");
        return DSTORE_FAIL;
    }

    if (page->GetUpper() > page->GetSpecialOffset() || page->GetSpecialOffset() > BLCKSZ) {
        ReportHeaderError(report, page, "special_offset_invalid", BLCKSZ, page->GetSpecialOffset(),
            "Page special area offset is out of range");
        return DSTORE_FAIL;
    }

    if (page->GetGlsn() == UINT64_MAX || page->GetPlsn() == UINT64_MAX) {
        ReportHeaderError(report, page, "lsn_invalid", UINT64_MAX - 1, UINT64_MAX, "Page LSN contains invalid sentinel");
        return DSTORE_FAIL;
    }

    if (!page->PageNoInit() && ((page->GetGlsn() == 0) != (page->GetPlsn() == 0))) {
        ReportHeaderError(report, page, "lsn_inconsistent", page->GetGlsn(), page->GetPlsn(),
            "Page LSN fields are inconsistent");
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

void PageVerifyRegistry::ReportHeaderError(
    VerifyReport *report, const Page *page, const char *checkName, uint64 expected, uint64 actual, const char *message)
{
    if (report == nullptr) {
        return;
    }
    const PageId pageId = page == nullptr ? INVALID_PAGE_ID : page->GetSelfPageId();
    report->AddResult(VerifySeverity::ERROR_LEVEL, "page", pageId, checkName, expected, actual, "%s", message);
}

RetStatus RegisterPageVerifier(
    PageType type, const char *typeName, PageVerifyFunc lightweightFunc, PageVerifyFunc heavyweightFunc)
{
    return g_pageVerifyRegistry.Register(type, typeName, lightweightFunc, heavyweightFunc);
}

void InitPageVerifiers()
{
    RegisterHeapPageVerifier();
    RegisterIndexPageVerifier();
}

RetStatus VerifyPageInline(const Page *page)
{
    VerifyReport report;
    return VerifyPageInlineWithReport(page, &report);
}

RetStatus VerifyPageInlineWithReport(const Page *page, VerifyReport *report)
{
    const VerifyLevel level = GetDfxVerifyLevel();
    if (level == VerifyLevel::OFF || page == nullptr) {
        return DSTORE_SUCC;
    }
    if (!IsModuleEnabledForPage(page->GetType())) {
        return DSTORE_SUCC;
    }
    return g_pageVerifyRegistry.Verify(page, level, report);
}

RetStatus VerifyPage(const Page *page, VerifyLevel level, VerifyReport *report)
{
    if (page == nullptr || level == VerifyLevel::OFF) {
        return DSTORE_SUCC;
    }
    if (!IsModuleEnabledForPage(page->GetType())) {
        return DSTORE_SUCC;
    }
    return g_pageVerifyRegistry.Verify(page, level, report);
}

void SetDfxVerifyLevel(VerifyLevel level)
{
    g_dfxVerifyLevel.store(static_cast<uint32>(level));
}

VerifyLevel GetDfxVerifyLevel()
{
    return static_cast<VerifyLevel>(g_dfxVerifyLevel.load());
}

void SetDfxVerifyModule(VerifyModule module)
{
    g_dfxVerifyModule.store(static_cast<uint32>(module));
}

VerifyModule GetDfxVerifyModule()
{
    return static_cast<VerifyModule>(g_dfxVerifyModule.load());
}

}  // namespace DSTORE

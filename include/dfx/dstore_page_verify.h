#ifndef DSTORE_PAGE_VERIFY_H
#define DSTORE_PAGE_VERIFY_H

#include <array>
#include <atomic>
#include "dfx/dstore_verify_report.h"
#include "page/dstore_page.h"

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

RetStatus RegisterPageVerifier(
    PageType type, const char *typeName, PageVerifyFunc lightweightFunc, PageVerifyFunc heavyweightFunc);
void InitPageVerifiers();

RetStatus VerifyPageInline(const Page *page);
RetStatus VerifyPageInlineWithReport(const Page *page, VerifyReport *report);
RetStatus VerifyPage(const Page *page, VerifyLevel level, VerifyReport *report);

void SetDfxVerifyLevel(VerifyLevel level);
VerifyLevel GetDfxVerifyLevel();
void SetDfxVerifyModule(VerifyModule module);
VerifyModule GetDfxVerifyModule();

}  // namespace DSTORE

#endif

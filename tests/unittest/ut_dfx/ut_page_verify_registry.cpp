#include <gtest/gtest.h>
#include <array>
#include "dfx/dstore_page_verify.h"

using namespace DSTORE;

namespace {

RetStatus VerifyHeapLightweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    if (report != nullptr) {
        report->AddResult(VerifySeverity::INFO_LEVEL, "page", page->GetSelfPageId(), "lightweight_called", 1, 1,
            "lightweight verifier invoked");
    }
    return DSTORE_SUCC;
}

RetStatus VerifyHeapHeavyweight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    if (report != nullptr) {
        report->AddResult(VerifySeverity::INFO_LEVEL, "page", page->GetSelfPageId(), "heavyweight_called", 1, 1,
            "heavyweight verifier invoked");
    }
    return DSTORE_SUCC;
}

using PageBuffer = std::array<unsigned char, BLCKSZ>;

Page *InitPage(PageBuffer &buffer, PageType type, PageId pageId)
{
    Page *page = reinterpret_cast<Page *>(buffer.data());
    page->Init(0, type, pageId);
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();
    return page;
}

}  // namespace

TEST(UTPageVerifyRegistry, RegisterAndDispatch)
{
    PageVerifyRegistry registry;
    VerifyReport report;
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {10, 20});

    EXPECT_EQ(registry.Register(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyHeapLightweight, VerifyHeapHeavyweight),
        DSTORE_SUCC);
    EXPECT_TRUE(registry.IsRegistered(PageType::HEAP_PAGE_TYPE));
    EXPECT_EQ(registry.Verify(page, VerifyLevel::LIGHTWEIGHT, &report), DSTORE_SUCC);
    ASSERT_EQ(report.GetResults().size(), 1U);
    EXPECT_STREQ(report.GetResults()[0].checkName, "lightweight_called");
}

TEST(UTPageVerifyRegistry, UnregisteredPageTypeFails)
{
    PageVerifyRegistry registry;
    VerifyReport report;
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {11, 21});

    EXPECT_EQ(registry.Verify(page, VerifyLevel::LIGHTWEIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
}

TEST(UTPageVerifyRegistry, VerifyPageInlineHonorsModuleFilter)
{
    VerifyReport report;
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {12, 22});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyHeapLightweight, VerifyHeapHeavyweight),
        DSTORE_SUCC);

    SetDfxVerifyLevel(VerifyLevel::LIGHTWEIGHT);
    SetDfxVerifyModule(VerifyModule::INDEX);
    EXPECT_EQ(VerifyPageInlineWithReport(page, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 0U);

    SetDfxVerifyModule(VerifyModule::HEAP);
    EXPECT_EQ(VerifyPageInlineWithReport(page, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 1U);

    SetDfxVerifyLevel(VerifyLevel::OFF);
    SetDfxVerifyModule(VerifyModule::ALL);
}

TEST(UTPageVerifyRegistry, LightweightSkipsAllZeroPage)
{
    VerifyReport report;
    PageBuffer pageBuffer{};
    Page *page = reinterpret_cast<Page *>(pageBuffer.data());

    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHTWEIGHT, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 0U);
}

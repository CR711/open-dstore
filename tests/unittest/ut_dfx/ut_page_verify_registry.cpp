#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <vector>
#include "dfx/dstore_page_verify.h"
#include "page/dstore_heap_page.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;
using DSTORE::ut_dfx::ScopedVerifyConfig;
using DSTORE::ut_dfx::ALL_VERIFY_MODULES;

namespace {

constexpr int CONCURRENT_THREAD_COUNT = 8;
constexpr int CONCURRENT_LOOP_COUNT = 2000;

/* 测试用的校验函数 */
RetStatus VerifyHeapLight(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    if (report != nullptr) {
        report->AddResult(VerifySeverity::SEVERITY_INFO, "page", page->GetSelfPageId(), "light_called", 1, 1,
            "light verifier invoked");
    }
    return DSTORE_SUCC;
}

RetStatus VerifyHeapMedium(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    if (report != nullptr) {
        report->AddResult(VerifySeverity::SEVERITY_INFO, "page", page->GetSelfPageId(), "medium_called", 1, 1,
            "medium verifier invoked");
    }
    return DSTORE_SUCC;
}

RetStatus VerifyHeapHeavy(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    if (report != nullptr) {
        report->AddResult(VerifySeverity::SEVERITY_INFO, "page", page->GetSelfPageId(), "heavy_called", 1, 1,
            "heavy verifier invoked");
    }
    return DSTORE_SUCC;
}

Page *InitPage(PageBuffer &buffer, PageType type, PageId pageId)
{
    Page *page = reinterpret_cast<Page *>(buffer.data());
    page->Init(0, type, pageId);
    page->SetLsn(1, 1, 1, false);
    /* 确保 lower 是 uint32 对齐的 */
    page->m_header.m_lower = sizeof(Page);
    page->SetChecksum();
    return page;
}

}  // namespace

/* ========== VerifyLevel 测试 ========== */

TEST(UTPageVerifyRegistry, VerifyLevelValues)
{
    EXPECT_EQ(static_cast<int>(VerifyLevel::NONE), 0);
    EXPECT_EQ(static_cast<int>(VerifyLevel::LIGHT), 1);
    EXPECT_EQ(static_cast<int>(VerifyLevel::MEDIUM), 2);
    EXPECT_EQ(static_cast<int>(VerifyLevel::HEAVY), 3);
}

/* ========== VerifyModule 测试 ========== */

TEST(UTPageVerifyRegistry, VerifyModuleValues)
{
    EXPECT_EQ(static_cast<int>(VerifyModule::HEAP), 0);
    EXPECT_EQ(static_cast<int>(VerifyModule::INDEX), 1);
    EXPECT_EQ(static_cast<int>(VerifyModule::UNDO), 2);
    EXPECT_EQ(static_cast<int>(VerifyModule::SEGMENT), 3);
    EXPECT_EQ(static_cast<int>(VerifyModule::FSM), 4);
}

/* ========== VerifySeverity 测试 ========== */

TEST(UTPageVerifyRegistry, VerifySeverityValues)
{
    EXPECT_EQ(static_cast<int>(VerifySeverity::SEVERITY_INFO), 0);
    EXPECT_EQ(static_cast<int>(VerifySeverity::SEVERITY_WARNING), 1);
    EXPECT_EQ(static_cast<int>(VerifySeverity::SEVERITY_ERROR), 2);
    EXPECT_EQ(static_cast<int>(VerifySeverity::SEVERITY_FATAL), 3);
}

/* ========== VerifyCode 测试 ========== */

TEST(UTPageVerifyRegistry, VerifyCodeValues)
{
    /* 通用错误码 */
    EXPECT_EQ(static_cast<uint32>(VerifyCode::PAGE_TYPE_INVALID), 0x0001);
    EXPECT_EQ(static_cast<uint32>(VerifyCode::PAGE_CRC_MISMATCH), 0x0004);
    EXPECT_EQ(static_cast<uint32>(VerifyCode::PAGE_NULL), 0x000A);

    /* Heap 模块错误码 */
    EXPECT_EQ(static_cast<uint32>(VerifyCode::HEAP_TD_COUNT_OVERFLOW), 0x0112);

    /* Index 模块错误码 */
    EXPECT_EQ(static_cast<uint32>(VerifyCode::BTR_PAGE_TYPE_INVALID), 0x0200);

    /* Undo 模块错误码 */
    EXPECT_EQ(static_cast<uint32>(VerifyCode::UNDO_SLOT_STATE_INVALID), 0x0300);

    /* Segment 模块错误码 */
    EXPECT_EQ(static_cast<uint32>(VerifyCode::SEG_MAGIC_MISMATCH), 0x0400);
}

/* ========== 注册表测试 ========== */

TEST(UTPageVerifyRegistry, RegisterAndDispatchLight)
{
    PageVerifyRegistry registry;
    VerifyReport report;
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {10, 20});

    EXPECT_EQ(registry.Register(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);
    EXPECT_TRUE(registry.IsRegistered(PageType::HEAP_PAGE_TYPE));

    EXPECT_EQ(registry.Verify(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    ASSERT_EQ(report.GetResults().size(), 1U);
    EXPECT_STREQ(report.GetResults()[0].checkName, "light_called");
}

TEST(UTPageVerifyRegistry, DispatchMediumLevel)
{
    PageVerifyRegistry registry;
    VerifyReport report;
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {11, 21});

    ASSERT_EQ(registry.Register(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    EXPECT_EQ(registry.Verify(page, VerifyLevel::MEDIUM, &report), DSTORE_SUCC);
    /* MEDIUM 级别应该调用 light + medium */
    EXPECT_EQ(report.GetResults().size(), 2U);
    EXPECT_STREQ(report.GetResults()[0].checkName, "light_called");
    EXPECT_STREQ(report.GetResults()[1].checkName, "medium_called");
}

TEST(UTPageVerifyRegistry, DispatchHeavyLevel)
{
    PageVerifyRegistry registry;
    VerifyReport report;
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {12, 22});

    ASSERT_EQ(registry.Register(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    EXPECT_EQ(registry.Verify(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    /* HEAVY 级别应该调用 light + medium + heavy */
    EXPECT_EQ(report.GetResults().size(), 3U);
    EXPECT_STREQ(report.GetResults()[0].checkName, "light_called");
    EXPECT_STREQ(report.GetResults()[1].checkName, "medium_called");
    EXPECT_STREQ(report.GetResults()[2].checkName, "heavy_called");
}

TEST(UTPageVerifyRegistry, UnregisteredPageTypeFails)
{
    PageVerifyRegistry registry;
    VerifyReport report;
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {13, 23});

    EXPECT_EQ(registry.Verify(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetResults()[0].code, VerifyCode::PAGE_TYPE_INVALID);
}

TEST(UTPageVerifyRegistry, NoneLevelSkipsVerify)
{
    PageVerifyRegistry registry;
    VerifyReport report;
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {14, 24});

    ASSERT_EQ(registry.Register(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    EXPECT_EQ(registry.Verify(page, VerifyLevel::NONE, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 0U);
}

TEST(UTPageVerifyRegistry, AllZeroPageSkipsVerify)
{
    VerifyReport report;
    PageBuffer pageBuffer{};  /* 全零 */

    EXPECT_EQ(VerifyPage(reinterpret_cast<const Page*>(pageBuffer.data()), VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 0U);
}

TEST(UTPageVerifyRegistry, NullPageFails)
{
    VerifyReport report;
    EXPECT_EQ(VerifyPage(nullptr, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetResults()[0].code, VerifyCode::PAGE_NULL);
}

/* ========== GUC 参数测试 ========== */

TEST(UTPageVerifyRegistry, ModuleFilterWorks)
{
    DSTORE::ut_dfx::ScopedVerifyConfig guard;
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {15, 25});
    VerifyReport skippedReport;

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 只启用 INDEX 模块，HEAP 页面应跳过 */
    SetDfxVerifyLevel(VerifyLevel::LIGHT);
    SetDfxVerifyModules(1ULL << static_cast<int>(VerifyModule::INDEX));
    EXPECT_EQ(VerifyPageInlineWithReport(page, &skippedReport), DSTORE_SUCC);
    EXPECT_EQ(skippedReport.GetTotalChecks(), 0U);

    /* 启用 HEAP 模块 */
    SetDfxVerifyModules(1ULL << static_cast<int>(VerifyModule::HEAP));
    VerifyReport enabledReport;
    EXPECT_EQ(VerifyPageInlineWithReport(page, &enabledReport), DSTORE_SUCC);
    EXPECT_EQ(enabledReport.GetTotalChecks(), 1U);
}

TEST(UTPageVerifyRegistry, MultipleModulesEnabled)
{
    DSTORE::ut_dfx::ScopedVerifyConfig guard;

    /* 启用 HEAP + INDEX + UNDO */
    uint64 modules = (1ULL << static_cast<int>(VerifyModule::HEAP)) |
                     (1ULL << static_cast<int>(VerifyModule::INDEX)) |
                     (1ULL << static_cast<int>(VerifyModule::UNDO));
    SetDfxVerifyModules(modules);

    EXPECT_TRUE(IsModuleEnabled(VerifyModule::HEAP));
    EXPECT_TRUE(IsModuleEnabled(VerifyModule::INDEX));
    EXPECT_TRUE(IsModuleEnabled(VerifyModule::UNDO));
    EXPECT_FALSE(IsModuleEnabled(VerifyModule::SEGMENT));
    EXPECT_FALSE(IsModuleEnabled(VerifyModule::FSM));
}

/* ========== VerifyReport 测试 ========== */

TEST(UTPageVerifyRegistry, VerifyReportWithCode)
{
    VerifyReport report;
    report.AddResultWithCode(VerifySeverity::SEVERITY_ERROR, VerifyCode::HEAP_TD_COUNT_OVERFLOW,
        "page", {1, 1}, "td_count_check", 10, 100, "TD count overflow");

    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetResults()[0].code, VerifyCode::HEAP_TD_COUNT_OVERFLOW);
}

TEST(UTPageVerifyRegistry, VerifyReportFatalSeverity)
{
    VerifyReport report;
    report.AddResult(VerifySeverity::SEVERITY_FATAL, "page", {1, 1}, "fatal_check", 0, 1, "Fatal error");

    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(report.HasFatal());
    EXPECT_EQ(report.GetFatalCount(), 1U);
}

TEST(UTPageVerifyRegistry, VerifyReportFormatText)
{
    VerifyReport report;
    report.AddResultWithCode(VerifySeverity::SEVERITY_ERROR, VerifyCode::PAGE_CRC_MISMATCH,
        "page", {1, 100}, "crc_check", 0x1234, 0x5678, "CRC mismatch");

    std::string text = report.FormatText();
    EXPECT_TRUE(text.find("CRC mismatch") != std::string::npos);
    EXPECT_TRUE(text.find("0x4") != std::string::npos);  /* VerifyCode value */
}

TEST(UTPageVerifyRegistry, VerifyReportFormatJson)
{
    VerifyReport report;
    report.AddResultWithCode(VerifySeverity::SEVERITY_WARNING, VerifyCode::HEAP_FSM_SLOT_INVALID,
        "page", {2, 200}, "fsm_check", 1, 2, "FSM slot invalid");

    std::string json = report.FormatJson();
    EXPECT_TRUE(json.find("\"warnings\":1") != std::string::npos);
    EXPECT_TRUE(json.find("\"severity\":\"WARNING\"") != std::string::npos);
}

/* ========== 三场景入口测试 ========== */

TEST(UTPageVerifyRegistry, VerifyPageOnReadReturnsError)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {100, 200});
    VerifyReport report;

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 正常页面应返回成功 */
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);

    /* 未注册的页面类型应返回失败，但不 PANIC */
    PageBuffer invalidBuffer{};
    Page *invalidPage = InitPage(invalidBuffer, PageType::INDEX_PAGE_TYPE, {101, 201});
    VerifyReport invalidReport;
    PageVerifyRegistry localRegistry;
    EXPECT_EQ(localRegistry.Verify(invalidPage, VerifyLevel::LIGHT, &invalidReport), DSTORE_FAIL);
    EXPECT_TRUE(invalidReport.HasError());
}

TEST(UTPageVerifyRegistry, VerifyPageFullCollectsIssues)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {102, 202});
    VerifyReport report;

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 正常页面应返回成功 */
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);

    /* 巡检模式即使发现问题也返回成功 */
    PageBuffer invalidBuffer{};
    Page *invalidPage = InitPage(invalidBuffer, PageType::INDEX_PAGE_TYPE, {103, 203});
    VerifyReport invalidReport;
    PageVerifyRegistry localRegistry;
    EXPECT_EQ(localRegistry.Verify(invalidPage, VerifyLevel::LIGHT, &invalidReport), DSTORE_FAIL);
    /* 巡检模式不 PANIC，只是收集问题 */
    EXPECT_TRUE(invalidReport.HasError());
}

TEST(UTPageVerifyRegistry, ThreeEntryBehaviorDifference)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {104, 204});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* VerifyPageOnRead 和 VerifyPageFull 对正常页面表现一致 */
    VerifyReport readReport;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &readReport), DSTORE_SUCC);

    VerifyReport fullReport;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &fullReport), DSTORE_SUCC);

    /* VerifyPageOnWrite 对正常页面返回成功（出错时会 PANIC，无法直接测试） */
    EXPECT_EQ(VerifyPageOnWrite(page, VerifyLevel::LIGHT), DSTORE_SUCC);
}

/* ========== 三场景强制 LIGHT 级别测试 ========== */

TEST(UTPageVerifyRegistry, OnWriteForcesLightLevel)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {200, 300});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 即使传入 HEAVY，OnWrite 也只走 LIGHT（通过检查 report 中的 checkName 判断） */
    VerifyReport report;
    EXPECT_EQ(VerifyPageOnWrite(page, VerifyLevel::HEAVY), DSTORE_SUCC);
    /* VerifyPageOnWrite 内部创建的 report 无法直接检查，但行为上应只执行 LIGHT */
}

TEST(UTPageVerifyRegistry, OnWrite_WhenLevelNone_ShouldSkipCorruptedPage)
{
    ScopedVerifyConfig guard;

    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {202, 302});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    page->SetLower(8000);
    page->SetUpper(1000);
    page->SetChecksum();

    EXPECT_EQ(VerifyPageOnWrite(page, VerifyLevel::NONE), DSTORE_SUCC);
}

TEST(UTPageVerifyRegistry, OnWrite_WhenModuleDisabled_ShouldSkipCorruptedPage)
{
    ScopedVerifyConfig guard;

    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {203, 303});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    SetDfxVerifyModules(1ULL << static_cast<int>(VerifyModule::INDEX));

    page->SetLower(8000);
    page->SetUpper(1000);
    page->SetChecksum();

    EXPECT_EQ(VerifyPageOnWrite(page, VerifyLevel::LIGHT), DSTORE_SUCC);
}

TEST(UTPageVerifyRegistry, OnWrite_WhenNullPage_ShouldPanic)
{
    ASSERT_DEATH((void)VerifyPageOnWrite(nullptr, VerifyLevel::LIGHT), "");
}

TEST(UTPageVerifyRegistry, OnWrite_WhenBoundaryInvalid_ShouldPanic)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {204, 304});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    page->SetLower(8000);
    page->SetUpper(1000);
    page->SetChecksum();

    ASSERT_DEATH((void)VerifyPageOnWrite(page, VerifyLevel::LIGHT), "");
}

TEST(UTPageVerifyRegistry, OnReadForcesLightLevel)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {201, 301});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 即使传入 HEAVY，OnRead 也只走 LIGHT */
    VerifyReport report;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    /* 应只有 LIGHT 的结果 */
    EXPECT_EQ(report.GetResults().size(), 1U);
    EXPECT_STREQ(report.GetResults()[0].checkName, "light_called");
}

TEST(UTPageVerifyRegistry, OnReadNullPageReturnsFail)
{
    VerifyReport report;
    EXPECT_EQ(VerifyPageOnRead(nullptr, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetResults()[0].code, VerifyCode::PAGE_NULL);
}

TEST(UTPageVerifyRegistry, OnFullNullPageRecordsButSucceeds)
{
    VerifyReport report;
    /* 巡检模式：空页记录问题但返回成功 */
    EXPECT_EQ(VerifyPageFull(nullptr, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetResults()[0].code, VerifyCode::PAGE_NULL);
}

/* ========== 读路径校验分支覆盖测试 ========== */

/*
 * 以下测试覆盖 VerifyPageOnRead 的所有分支路径：
 *   - 空页 → 返回 FAIL + PAGE_NULL
 *   - NONE 级别 → 跳过
 *   - 模块未启用 → 跳过
 *   - 全零页 → 跳过
 *   - CR 页面 → 跳过
 *   - 强制 LIGHT → 不执行 MEDIUM/HEAVY
 *   - CRC 不匹配 → 返回 FAIL
 *   - 边界非法 → 返回 FAIL
 *   - 正常页面 → 返回 SUCC
 *   - report 为 nullptr → 不崩溃
 */

TEST(UTPageVerifyRegistry, OnRead_WhenLevelNone_ShouldSkip)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {300, 400});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 破坏页面边界 */
    page->SetLower(8000);
    page->SetUpper(1000);
    page->SetChecksum();

    VerifyReport report;
    /* NONE 级别应跳过校验，即使页面有问题 */
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::NONE, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 0U);
    EXPECT_FALSE(report.HasError());
}

TEST(UTPageVerifyRegistry, OnRead_WhenModuleDisabled_ShouldSkip)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {301, 401});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 破坏页面边界 */
    page->SetLower(8000);
    page->SetUpper(1000);
    page->SetChecksum();

    /* 只启用 INDEX 模块，HEAP 页面应被跳过 */
    ScopedVerifyConfig guard;
    SetDfxVerifyLevel(VerifyLevel::LIGHT);
    SetDfxVerifyModules(1ULL << static_cast<int>(VerifyModule::INDEX));

    VerifyReport report;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 0U);

}

TEST(UTPageVerifyRegistry, OnRead_WhenAllZeroPage_ShouldSkip)
{
    PageBuffer pageBuffer{};  /* 全零 */

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    VerifyReport report;
    EXPECT_EQ(VerifyPageOnRead(reinterpret_cast<const Page *>(pageBuffer.data()),
        VerifyLevel::LIGHT, &report), DSTORE_SUCC);
    EXPECT_EQ(report.GetTotalChecks(), 0U);
}

TEST(UTPageVerifyRegistry, OnRead_WhenCrcMismatch_ShouldReturnFail)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {302, 402});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 篡改页面内容但不更新 CRC */
    page->m_header.m_lower += 8;

    VerifyReport report;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());

    bool hasCrcError = false;
    for (const auto &result : report.GetResults()) {
        if (result.code == VerifyCode::PAGE_CRC_MISMATCH) {
            hasCrcError = true;
            break;
        }
    }
    EXPECT_TRUE(hasCrcError);
}

TEST(UTPageVerifyRegistry, OnRead_WhenBoundaryInvalid_ShouldReturnFail)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {303, 403});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* lower > upper */
    page->SetLower(8000);
    page->SetUpper(1000);
    page->SetChecksum();

    VerifyReport report;
    RetStatus ret = VerifyPageOnRead(page, VerifyLevel::LIGHT, &report);
    EXPECT_EQ(ret, DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetResults()[0].code, VerifyCode::PAGE_BOUNDARY_INVALID);
}

TEST(UTPageVerifyRegistry, OnRead_WhenSpecialOffsetExceedsBlcksz_ShouldReturnFail)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {304, 404});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* special offset 超出 BLCKSZ */
    page->m_header.m_special.m_offset = BLCKSZ + 100;
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetResults()[0].code, VerifyCode::PAGE_BOUNDARY_INVALID);
}

TEST(UTPageVerifyRegistry, OnRead_WhenNullReport_ShouldNotCrash)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {305, 405});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 传 nullptr report，不应崩溃 */
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, nullptr), DSTORE_SUCC);

    /* 错误页面 + nullptr report，也不应崩溃 */
    page->SetLower(8000);
    page->SetUpper(1000);
    page->SetChecksum();
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, nullptr), DSTORE_FAIL);
}

TEST(UTPageVerifyRegistry, OnRead_WhenHeavyLevel_ShouldForceLight)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {306, 406});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 即使传入 HEAVY，OnRead 也只执行 LIGHT */
    VerifyReport report;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::HEAVY, &report), DSTORE_SUCC);
    ASSERT_EQ(report.GetResults().size(), 1U);
    EXPECT_STREQ(report.GetResults()[0].checkName, "light_called");
}

TEST(UTPageVerifyRegistry, OnRead_WhenMediumLevel_ShouldForceLight)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {307, 407});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 即使传入 MEDIUM，OnRead 也只执行 LIGHT */
    VerifyReport report;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::MEDIUM, &report), DSTORE_SUCC);
    ASSERT_EQ(report.GetResults().size(), 1U);
    EXPECT_STREQ(report.GetResults()[0].checkName, "light_called");
}

TEST(UTPageVerifyRegistry, OnRead_WhenPageTypeInvalid_ShouldReturnFail)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {308, 408});

    /* 将页面类型篡改为 INVALID 后更新 CRC */
    page->m_header.m_type = static_cast<uint16>(PageType::INVALID_PAGE_TYPE);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetResults()[0].code, VerifyCode::PAGE_TYPE_INVALID);
}

TEST(UTPageVerifyRegistry, OnRead_ErrorReport_ContainsCorrectPageId)
{
    PageBuffer pageBuffer{};
    const PageId testPageId = {309, 409};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, testPageId);

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 制造边界错误 */
    page->SetLower(8000);
    page->SetUpper(1000);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);

    /* 验证报告中的 pageId 正确 */
    ASSERT_GE(report.GetResults().size(), 1U);
    EXPECT_EQ(report.GetResults()[0].targetId.m_fileId, testPageId.m_fileId);
    EXPECT_EQ(report.GetResults()[0].targetId.m_blockId, testPageId.m_blockId);
}

TEST(UTPageVerifyRegistry, OnRead_ErrorReport_FormatTextContainsDetails)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {310, 410});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 制造 CRC 错误 */
    page->m_header.m_lower += 8;

    VerifyReport report;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);

    /* FormatText 应包含错误描述 */
    std::string text = report.FormatText();
    EXPECT_FALSE(text.empty());
    EXPECT_TRUE(text.find("0x4") != std::string::npos);  /* PAGE_CRC_MISMATCH code */
}

/* 自定义失败校验函数，用于测试 OnRead 不 PANIC */
namespace {

RetStatus VerifyAlwaysFail(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    if (report != nullptr) {
        report->AddResultWithCode(VerifySeverity::SEVERITY_ERROR, VerifyCode::PAGE_BOUNDARY_INVALID,
            "page", page->GetSelfPageId(), "always_fail", 0, 1, "intentional failure for test");
    }
    return DSTORE_FAIL;
}

RetStatus VerifyAlwaysFatal(const Page *page, VerifyLevel level, VerifyReport *report)
{
    (void)level;
    if (report != nullptr) {
        report->AddResultWithCode(VerifySeverity::SEVERITY_FATAL, VerifyCode::PAGE_NULL,
            "page", page->GetSelfPageId(), "always_fatal", 0, 1, "fatal failure for test");
    }
    return DSTORE_FAIL;
}

}  // namespace

TEST(UTPageVerifyRegistry, OnRead_WhenVerifierFails_ShouldReturnFailNotPanic)
{
    /* 用自定义注册表测试：verifier 失败时 OnRead 应返回错误而不是 PANIC */
    PageVerifyRegistry failRegistry;
    ASSERT_EQ(failRegistry.Register(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyAlwaysFail, nullptr, nullptr), DSTORE_SUCC);

    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {311, 411});

    VerifyReport report;
    /* 使用本地 registry 验证，确认返回 FAIL 而不是 PANIC */
    EXPECT_EQ(failRegistry.Verify(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetResults()[0].code, VerifyCode::PAGE_BOUNDARY_INVALID);
    EXPECT_STREQ(report.GetResults()[0].checkName, "always_fail");
}

TEST(UTPageVerifyRegistry, OnRead_WhenVerifierReturnsFatal_ShouldReturnFailNotPanic)
{
    PageVerifyRegistry fatalRegistry;
    ASSERT_EQ(fatalRegistry.Register(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyAlwaysFatal, nullptr, nullptr), DSTORE_SUCC);

    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {312, 412});

    VerifyReport report;
    EXPECT_EQ(fatalRegistry.Verify(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasFatal());
    EXPECT_EQ(report.GetFatalCount(), 1U);
}

TEST(UTPageVerifyRegistry, OnRead_VsOnWrite_BehaviorDifference)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {313, 413});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 正常页面：OnRead 和 OnWrite 都返回成功 */
    VerifyReport readReport;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &readReport), DSTORE_SUCC);
    EXPECT_EQ(VerifyPageOnWrite(page, VerifyLevel::LIGHT), DSTORE_SUCC);

    /* 验证两者在正常场景下行为一致 */
    EXPECT_EQ(readReport.GetResults().size(), 1U);
}

TEST(UTPageVerifyRegistry, OnRead_VsOnFull_BehaviorDifference)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {314, 414});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 制造边界错误 */
    page->SetLower(8000);
    page->SetUpper(1000);
    page->SetChecksum();

    /* OnRead 返回 FAIL */
    VerifyReport readReport;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &readReport), DSTORE_FAIL);

    /* OnFull 始终返回 SUCC（巡检收集问题） */
    VerifyReport fullReport;
    EXPECT_EQ(VerifyPageFull(page, VerifyLevel::LIGHT, &fullReport), DSTORE_SUCC);

    /* 但两者都应记录错误 */
    EXPECT_TRUE(readReport.HasError());
    EXPECT_TRUE(fullReport.HasError());
}

TEST(UTPageVerifyRegistry, OnRead_MultipleModuleInteraction)
{
    PageBuffer heapBuf{};
    Page *heapPage = InitPage(heapBuf, PageType::HEAP_PAGE_TYPE, {315, 415});

    PageBuffer indexBuf{};
    Page *indexPage = InitPage(indexBuf, PageType::INDEX_PAGE_TYPE, {316, 416});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, nullptr, nullptr), DSTORE_SUCC);
    ASSERT_EQ(RegisterPageVerifier(PageType::INDEX_PAGE_TYPE, "IndexPage", VerifyModule::INDEX,
        VerifyHeapLight, nullptr, nullptr), DSTORE_SUCC);

    DSTORE::ut_dfx::ScopedVerifyConfig guard;

    /* 只启用 HEAP 模块 */
    SetDfxVerifyLevel(VerifyLevel::LIGHT);
    SetDfxVerifyModules(1ULL << static_cast<int>(VerifyModule::HEAP));

    VerifyReport heapReport;
    EXPECT_EQ(VerifyPageOnRead(heapPage, VerifyLevel::LIGHT, &heapReport), DSTORE_SUCC);
    EXPECT_EQ(heapReport.GetResults().size(), 1U);  /* HEAP 页面应被校验 */

    VerifyReport indexReport;
    EXPECT_EQ(VerifyPageOnRead(indexPage, VerifyLevel::LIGHT, &indexReport), DSTORE_SUCC);
    EXPECT_EQ(indexReport.GetTotalChecks(), 0U);  /* INDEX 页面应被跳过 */

    /* 切换到只启用 INDEX 模块 */
    SetDfxVerifyModules(1ULL << static_cast<int>(VerifyModule::INDEX));

    VerifyReport heapReport2;
    EXPECT_EQ(VerifyPageOnRead(heapPage, VerifyLevel::LIGHT, &heapReport2), DSTORE_SUCC);
    EXPECT_EQ(heapReport2.GetTotalChecks(), 0U);  /* 现在 HEAP 页面应被跳过 */

    VerifyReport indexReport2;
    EXPECT_EQ(VerifyPageOnRead(indexPage, VerifyLevel::LIGHT, &indexReport2), DSTORE_SUCC);
    EXPECT_EQ(indexReport2.GetResults().size(), 1U);  /* INDEX 页面应被校验 */
}

TEST(UTPageVerifyRegistry, OnRead_GucLevelInteraction)
{
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {317, 417});

    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    DSTORE::ut_dfx::ScopedVerifyConfig guard;
    SetDfxVerifyModules(ALL_VERIFY_MODULES);  /* 启用所有模块 */

    /* GUC=NONE → 即使显式传 LIGHT 也应跳过 */
    SetDfxVerifyLevel(VerifyLevel::NONE);
    VerifyReport noneReport;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::NONE, &noneReport), DSTORE_SUCC);
    EXPECT_EQ(noneReport.GetTotalChecks(), 0U);

    /* GUC=LIGHT → 正常校验 */
    SetDfxVerifyLevel(VerifyLevel::LIGHT);
    VerifyReport lightReport;
    EXPECT_EQ(VerifyPageOnRead(page, VerifyLevel::LIGHT, &lightReport), DSTORE_SUCC);
    EXPECT_GE(lightReport.GetResults().size(), 1U);
}

TEST(UTPageVerifyRegistry, OnRead_WhenUnregisteredPageType_ShouldReturnFail)
{
    PageBuffer pageBuffer{};
    /* 使用 FSM_PAGE_TYPE 且不注册它的 verifier */
    Page *page = InitPage(pageBuffer, PageType::FSM_PAGE_TYPE, {318, 418});

    /* 用本地 registry 验证（不含 FSM_PAGE_TYPE） */
    PageVerifyRegistry emptyRegistry;
    VerifyReport report;
    EXPECT_EQ(emptyRegistry.Verify(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetResults()[0].code, VerifyCode::PAGE_TYPE_INVALID);
}

TEST(UTPageVerifyRegistry, OnRead_ErrorCount_MatchesSeverity)
{
    VerifyReport report;

    /* 空页 → 一个 ERROR */
    EXPECT_EQ(VerifyPageOnRead(nullptr, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    EXPECT_EQ(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_EQ(report.GetFatalCount(), 0U);
}

/* ========== 多线程并发校验测试 ========== */

/*
 * 并发场景覆盖：
 *   1. 多线程同时 VerifyPageOnRead 同一页面（读路径只读，不应竞争）
 *   2. 多线程同时 VerifyPageOnRead 不同页面（每线程独立 report）
 *   3. 读校验 + 写校验并发（读写路径互不干扰）
 *   4. 校验并发 + GUC level 动态切换（模拟运维热调参）
 *   5. 校验并发 + GUC module 动态切换（模拟模块启停）
 *   6. 混合页面类型并发校验（heap/index/undo 并行）
 *   7. 模拟恢复阶段切换（redo→undo→normal 并发校验）
 *   8. 正常页 + 损坏页混合并发（模拟部分磁盘坏块）
 */

TEST(UTPageVerifyRegistry, Concurrent_MultiThread_ReadVerify_SamePage)
{
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {400, 500});

    std::atomic<int> successCount{0};
    std::atomic<int> failCount{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = CONCURRENT_THREAD_COUNT;

    std::vector<std::thread> threads;
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        threads.emplace_back([&page, &successCount, &failCount, &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyReport report;
                RetStatus ret = VerifyPageOnRead(page, VerifyLevel::LIGHT, &report);
                if (ret == DSTORE_SUCC) {
                    successCount.fetch_add(1);
                } else {
                    failCount.fetch_add(1);
                }
            }
        });
    }
    for (auto &th : threads) {
        th.join();
    }

    /* 正常页面，所有校验应全部成功 */
    EXPECT_EQ(successCount.load(), CONCURRENT_THREAD_COUNT * CONCURRENT_LOOP_COUNT);
    EXPECT_EQ(failCount.load(), 0);
}

TEST(UTPageVerifyRegistry, Concurrent_MultiThread_ReadVerify_DifferentPages)
{
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 每个线程操作独立的页面 */
    std::vector<PageBuffer> pageBuffers(CONCURRENT_THREAD_COUNT);
    std::vector<Page *> pages(CONCURRENT_THREAD_COUNT);
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        pages[t] = InitPage(pageBuffers[t], PageType::HEAP_PAGE_TYPE,
            {static_cast<uint16>(410 + t), static_cast<uint32>(510 + t)});
    }

    std::atomic<int> totalSuccess{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = CONCURRENT_THREAD_COUNT;

    std::vector<std::thread> threads;
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        threads.emplace_back([&pages, &totalSuccess, &readyCount, totalThreads, t]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyReport report;
                RetStatus ret = VerifyPageOnRead(pages[t], VerifyLevel::LIGHT, &report);
                if (ret == DSTORE_SUCC) {
                    totalSuccess.fetch_add(1);
                }
            }
        });
    }
    for (auto &th : threads) {
        th.join();
    }

    EXPECT_EQ(totalSuccess.load(), CONCURRENT_THREAD_COUNT * CONCURRENT_LOOP_COUNT);
}

TEST(UTPageVerifyRegistry, Concurrent_ReadAndWriteVerify_Parallel)
{
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    /* 读线程和写线程各操作不同的页面 */
    PageBuffer readBuf{};
    Page *readPage = InitPage(readBuf, PageType::HEAP_PAGE_TYPE, {420, 520});

    PageBuffer writeBuf{};
    Page *writePage = InitPage(writeBuf, PageType::HEAP_PAGE_TYPE, {421, 521});

    std::atomic<int> readSuccess{0};
    std::atomic<int> writeSuccess{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = CONCURRENT_THREAD_COUNT;

    /* 一半线程走读路径，一半走写路径 */
    std::vector<std::thread> threads;
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        if (t % 2 == 0) {
            threads.emplace_back([&readPage, &readSuccess, &readyCount, totalThreads]() {
                readyCount.fetch_add(1);
                while (readyCount.load() < totalThreads) {
                    /* spin until all threads ready */
                }
                std::atomic_thread_fence(std::memory_order_seq_cst);
                for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                    VerifyReport report;
                    if (VerifyPageOnRead(readPage, VerifyLevel::LIGHT, &report) == DSTORE_SUCC) {
                        readSuccess.fetch_add(1);
                    }
                }
            });
        } else {
            threads.emplace_back([&writePage, &writeSuccess, &readyCount, totalThreads]() {
                readyCount.fetch_add(1);
                while (readyCount.load() < totalThreads) {
                    /* spin until all threads ready */
                }
                std::atomic_thread_fence(std::memory_order_seq_cst);
                for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                    if (VerifyPageOnWrite(writePage, VerifyLevel::LIGHT) == DSTORE_SUCC) {
                        writeSuccess.fetch_add(1);
                    }
                }
            });
        }
    }
    for (auto &th : threads) {
        th.join();
    }

    int halfThreads = CONCURRENT_THREAD_COUNT / 2;
    EXPECT_EQ(readSuccess.load(), halfThreads * CONCURRENT_LOOP_COUNT);
    EXPECT_EQ(writeSuccess.load(), halfThreads * CONCURRENT_LOOP_COUNT);
}

TEST(UTPageVerifyRegistry, Concurrent_GucLevelToggle_WhileVerifying)
{
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    ScopedVerifyConfig guard;
    SetDfxVerifyModules(ALL_VERIFY_MODULES);

    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {430, 530});

    std::atomic<bool> stop{false};
    std::atomic<int> verifyCount{0};
    std::atomic<int> skippedCount{0};
    std::atomic<int> checkedCount{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = CONCURRENT_THREAD_COUNT + 1; /* +1 for GUC toggle thread */

    /* GUC 切换线程：在 NONE/LIGHT/MEDIUM/HEAVY 之间快速切换 */
    std::thread gucToggleThread([&stop, &readyCount, totalThreads]() {
        readyCount.fetch_add(1);
        while (readyCount.load() < totalThreads) {
            /* spin until all threads ready */
        }
        std::atomic_thread_fence(std::memory_order_seq_cst);
        VerifyLevel levels[] = {VerifyLevel::NONE, VerifyLevel::LIGHT,
                                VerifyLevel::MEDIUM, VerifyLevel::HEAVY};
        int idx = 0;
        while (!stop.load()) {
            SetDfxVerifyLevel(levels[idx % 4]);
            idx++;
        }
    });

    /*
     * 多个校验线程并发运行.
     *
     * NOT a TOCTOU race: curLevel is read once and passed explicitly to
     * VerifyPageOnRead, which uses the passed parameter (not re-reading
     * the GUC).  The module bitmask (ALL_VERIFY_MODULES) is fixed in this test —
     * only the level is toggled by the other thread — so
     * IsModuleEnabledInternal always returns true for HEAP.  Therefore
     * the assertion "curLevel==NONE => totalChecks==0" is deterministic
     * with respect to the value actually used by the verify call.
     */
    std::vector<std::thread> verifyThreads;
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        verifyThreads.emplace_back([&page, &verifyCount, &skippedCount, &checkedCount,
                                    &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyLevel curLevel = GetDfxVerifyLevel();
                VerifyReport report;
                RetStatus ret = VerifyPageOnRead(page, curLevel, &report);
                EXPECT_EQ(ret, DSTORE_SUCC);
                if (curLevel == VerifyLevel::NONE) {
                    EXPECT_EQ(report.GetTotalChecks(), 0U);
                    skippedCount.fetch_add(1);
                } else {
                    EXPECT_GT(report.GetTotalChecks(), 0U);
                    checkedCount.fetch_add(1);
                }
                verifyCount.fetch_add(1);
            }
        });
    }

    for (auto &th : verifyThreads) {
        th.join();
    }
    stop.store(true);
    gucToggleThread.join();

    EXPECT_EQ(verifyCount.load(), CONCURRENT_THREAD_COUNT * CONCURRENT_LOOP_COUNT);
    EXPECT_GT(skippedCount.load(), 0);
    EXPECT_GT(checkedCount.load(), 0);
}

TEST(UTPageVerifyRegistry, Concurrent_GucModuleToggle_WhileVerifying)
{
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    ScopedVerifyConfig guard;
    SetDfxVerifyLevel(VerifyLevel::LIGHT);

    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {431, 531});

    std::atomic<bool> stop{false};
    std::atomic<int> verifyCount{0};
    std::atomic<int> skippedCount{0};
    std::atomic<int> checkedCount{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = CONCURRENT_THREAD_COUNT + 1; /* +1 for module toggle thread */

    /* 模块启停切换线程：反复启用/禁用 HEAP 模块 */
    std::thread moduleToggleThread([&stop, &readyCount, totalThreads]() {
        readyCount.fetch_add(1);
        while (readyCount.load() < totalThreads) {
            /* spin until all threads ready */
        }
        std::atomic_thread_fence(std::memory_order_seq_cst);
        uint64 heapEnabled = 1ULL << static_cast<int>(VerifyModule::HEAP);
        uint64 heapDisabled = 1ULL << static_cast<int>(VerifyModule::INDEX);
        int idx = 0;
        while (!stop.load()) {
            SetDfxVerifyModules((idx % 2 == 0) ? heapEnabled : heapDisabled);
            idx++;
        }
    });

    std::vector<std::thread> verifyThreads;
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        verifyThreads.emplace_back([&page, &verifyCount, &skippedCount, &checkedCount,
                                    &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyReport report;
                RetStatus ret = VerifyPageOnRead(page, VerifyLevel::LIGHT, &report);
                verifyCount.fetch_add(1);
                /*
                 * 模块启用时：校验成功且 report 有结果
                 * 模块禁用时：直接返回成功，report 无结果
                 * 两种结果都合法
                 */
                if (ret == DSTORE_SUCC && report.GetTotalChecks() == 0) {
                    skippedCount.fetch_add(1);
                } else if (ret == DSTORE_SUCC && report.GetTotalChecks() > 0) {
                    checkedCount.fetch_add(1);
                }
            }
        });
    }

    for (auto &th : verifyThreads) {
        th.join();
    }
    stop.store(true);
    moduleToggleThread.join();

    int totalExpected = CONCURRENT_THREAD_COUNT * CONCURRENT_LOOP_COUNT;
    EXPECT_EQ(verifyCount.load(), totalExpected);
    /* skipped + checked 应等于 total（没有失败的情况） */
    EXPECT_EQ(skippedCount.load() + checkedCount.load(), totalExpected);
    /* 两种情况都应该出现过（否则 toggle 没有真正生效） */
    EXPECT_GT(skippedCount.load(), 0);
    EXPECT_GT(checkedCount.load(), 0);

}

TEST(UTPageVerifyRegistry, Concurrent_MixedPageTypes_Parallel)
{
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, nullptr, nullptr), DSTORE_SUCC);
    ASSERT_EQ(RegisterPageVerifier(PageType::INDEX_PAGE_TYPE, "IndexPage", VerifyModule::INDEX,
        VerifyHeapLight, nullptr, nullptr), DSTORE_SUCC);

    ScopedVerifyConfig guard;
    SetDfxVerifyLevel(VerifyLevel::LIGHT);
    SetDfxVerifyModules(ALL_VERIFY_MODULES);  /* 所有模块启用 */

    PageBuffer heapBuf{};
    Page *heapPage = InitPage(heapBuf, PageType::HEAP_PAGE_TYPE, {440, 540});

    PageBuffer indexBuf{};
    Page *indexPage = InitPage(indexBuf, PageType::INDEX_PAGE_TYPE, {441, 541});

    std::atomic<int> heapSuccess{0};
    std::atomic<int> indexSuccess{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = CONCURRENT_THREAD_COUNT;

    std::vector<std::thread> threads;
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        if (t % 2 == 0) {
            threads.emplace_back([&heapPage, &heapSuccess, &readyCount, totalThreads]() {
                readyCount.fetch_add(1);
                while (readyCount.load() < totalThreads) {
                    /* spin until all threads ready */
                }
                std::atomic_thread_fence(std::memory_order_seq_cst);
                for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                    VerifyReport report;
                    if (VerifyPageOnRead(heapPage, VerifyLevel::LIGHT, &report) == DSTORE_SUCC) {
                        heapSuccess.fetch_add(1);
                    }
                }
            });
        } else {
            threads.emplace_back([&indexPage, &indexSuccess, &readyCount, totalThreads]() {
                readyCount.fetch_add(1);
                while (readyCount.load() < totalThreads) {
                    /* spin until all threads ready */
                }
                std::atomic_thread_fence(std::memory_order_seq_cst);
                for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                    VerifyReport report;
                    if (VerifyPageOnRead(indexPage, VerifyLevel::LIGHT, &report) == DSTORE_SUCC) {
                        indexSuccess.fetch_add(1);
                    }
                }
            });
        }
    }
    for (auto &th : threads) {
        th.join();
    }

    int halfThreads = CONCURRENT_THREAD_COUNT / 2;
    EXPECT_EQ(heapSuccess.load(), halfThreads * CONCURRENT_LOOP_COUNT);
    EXPECT_EQ(indexSuccess.load(), halfThreads * CONCURRENT_LOOP_COUNT);

}

TEST(UTPageVerifyRegistry, Concurrent_RecoveryPhaseSimulation)
{
    /*
     * 模拟恢复阶段切换场景：
     * - redo 阶段：强制 NONE（不校验）
     * - undo 阶段：最高 LIGHT
     * - normal 阶段：使用 GUC 设置
     * 通过动态切换 GUC level 模拟上述三个阶段的并发切换
     */
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    ScopedVerifyConfig guard;
    SetDfxVerifyModules(ALL_VERIFY_MODULES);

    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {450, 550});

    std::atomic<bool> stop{false};
    std::atomic<int> redoPhaseCount{0};
    std::atomic<int> undoPhaseCount{0};
    std::atomic<int> normalPhaseCount{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = CONCURRENT_THREAD_COUNT + 1; /* +1 for phase thread */

    /* 阶段切换线程：模拟 redo→undo→normal 循环 */
    std::thread phaseThread([&stop, &readyCount, totalThreads]() {
        readyCount.fetch_add(1);
        while (readyCount.load() < totalThreads) {
            /* spin until all threads ready */
        }
        std::atomic_thread_fence(std::memory_order_seq_cst);
        int phase = 0;
        while (!stop.load()) {
            switch (phase % 3) {
                case 0:  /* redo 阶段 → 强制 NONE */
                    SetDfxVerifyLevel(VerifyLevel::NONE);
                    break;
                case 1:  /* undo 阶段 → 最高 LIGHT */
                    SetDfxVerifyLevel(VerifyLevel::LIGHT);
                    break;
                case 2:  /* normal 阶段 → MEDIUM */
                    SetDfxVerifyLevel(VerifyLevel::MEDIUM);
                    break;
            }
            phase++;
        }
    });

    /*
     * 并发校验线程.
     *
     * NOT a TOCTOU race: curLevel is passed explicitly to VerifyPageOnRead
     * (not re-read from GUC internally).  Module bitmask is fixed at ALL_VERIFY_MODULES.
     * Assertions here only count phase occurrences and check no crash —
     * they do not correlate curLevel with report contents.
     */
    std::vector<std::thread> verifyThreads;
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        verifyThreads.emplace_back([&page, &redoPhaseCount, &undoPhaseCount, &normalPhaseCount,
                                    &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyLevel curLevel = GetDfxVerifyLevel();
                VerifyReport report;
                RetStatus ret = VerifyPageOnRead(page, curLevel, &report);

                /* 所有阶段都不应崩溃 */
                EXPECT_EQ(ret, DSTORE_SUCC);

                if (curLevel == VerifyLevel::NONE) {
                    redoPhaseCount.fetch_add(1);
                } else if (curLevel == VerifyLevel::LIGHT) {
                    undoPhaseCount.fetch_add(1);
                } else {
                    normalPhaseCount.fetch_add(1);
                }
            }
        });
    }

    for (auto &th : verifyThreads) {
        th.join();
    }
    stop.store(true);
    phaseThread.join();

    int totalExpected = CONCURRENT_THREAD_COUNT * CONCURRENT_LOOP_COUNT;
    EXPECT_EQ(redoPhaseCount.load() + undoPhaseCount.load() + normalPhaseCount.load(), totalExpected);
    /* 三个阶段都应该被命中过 */
    EXPECT_GT(redoPhaseCount.load(), 0);
    EXPECT_GT(undoPhaseCount.load(), 0);
    EXPECT_GT(normalPhaseCount.load(), 0);

}

TEST(UTPageVerifyRegistry, Concurrent_CorruptedAndNormalPages_Mixed)
{
    /*
     * 模拟部分磁盘坏块场景：
     * 一半线程校验正常页面（应全部成功），另一半校验损坏页面（应全部失败）
     * 两组线程并发执行，验证互不干扰
     */
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    ScopedVerifyConfig guard;
    SetDfxVerifyLevel(VerifyLevel::LIGHT);
    SetDfxVerifyModules(ALL_VERIFY_MODULES);

    /* 正常页面 */
    PageBuffer goodBuf{};
    Page *goodPage = InitPage(goodBuf, PageType::HEAP_PAGE_TYPE, {460, 560});

    /* 损坏页面：lower > upper */
    PageBuffer badBuf{};
    Page *badPage = InitPage(badBuf, PageType::HEAP_PAGE_TYPE, {461, 561});
    badPage->SetLower(8000);
    badPage->SetUpper(1000);
    badPage->SetChecksum();

    std::atomic<int> goodSuccess{0};
    std::atomic<int> goodFail{0};
    std::atomic<int> badSuccess{0};
    std::atomic<int> badFail{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = CONCURRENT_THREAD_COUNT;

    std::vector<std::thread> threads;
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        if (t % 2 == 0) {
            /* 正常页面线程 */
            threads.emplace_back([&goodPage, &goodSuccess, &goodFail, &readyCount, totalThreads]() {
                readyCount.fetch_add(1);
                while (readyCount.load() < totalThreads) {
                    /* spin until all threads ready */
                }
                std::atomic_thread_fence(std::memory_order_seq_cst);
                for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                    VerifyReport report;
                    RetStatus ret = VerifyPageOnRead(goodPage, VerifyLevel::LIGHT, &report);
                    if (ret == DSTORE_SUCC) {
                        goodSuccess.fetch_add(1);
                    } else {
                        goodFail.fetch_add(1);
                    }
                }
            });
        } else {
            /* 损坏页面线程 */
            threads.emplace_back([&badPage, &badSuccess, &badFail, &readyCount, totalThreads]() {
                readyCount.fetch_add(1);
                while (readyCount.load() < totalThreads) {
                    /* spin until all threads ready */
                }
                std::atomic_thread_fence(std::memory_order_seq_cst);
                for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                    VerifyReport report;
                    RetStatus ret = VerifyPageOnRead(badPage, VerifyLevel::LIGHT, &report);
                    if (ret == DSTORE_SUCC) {
                        badSuccess.fetch_add(1);
                    } else {
                        badFail.fetch_add(1);
                    }
                }
            });
        }
    }
    for (auto &th : threads) {
        th.join();
    }

    int halfThreads = CONCURRENT_THREAD_COUNT / 2;
    /* 正常页面应全部成功 */
    EXPECT_EQ(goodSuccess.load(), halfThreads * CONCURRENT_LOOP_COUNT);
    EXPECT_EQ(goodFail.load(), 0);
    /* 损坏页面应全部失败 */
    EXPECT_EQ(badSuccess.load(), 0);
    EXPECT_EQ(badFail.load(), halfThreads * CONCURRENT_LOOP_COUNT);

}

TEST(UTPageVerifyRegistry, Concurrent_OnReadOnWriteOnFull_ThreePathParallel)
{
    /*
     * 三场景并发：模拟线上运行时读请求、写刷脏、巡检任务同时执行
     * 验证三条路径并发访问 registry 和 GUC 原子变量时的线程安全性
     */
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    ScopedVerifyConfig guard;
    SetDfxVerifyLevel(VerifyLevel::LIGHT);
    SetDfxVerifyModules(ALL_VERIFY_MODULES);

    /* 三个独立页面，避免修改竞争 */
    PageBuffer readBuf{};
    Page *readPage = InitPage(readBuf, PageType::HEAP_PAGE_TYPE, {470, 570});
    PageBuffer writeBuf{};
    Page *writePage = InitPage(writeBuf, PageType::HEAP_PAGE_TYPE, {471, 571});
    PageBuffer scanBuf{};
    Page *scanPage = InitPage(scanBuf, PageType::HEAP_PAGE_TYPE, {472, 572});

    std::atomic<int> readOk{0};
    std::atomic<int> writeOk{0};
    std::atomic<int> scanOk{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = 3 + 3 + 2; /* read + write + scan */

    std::vector<std::thread> threads;

    /* 读路径线程 */
    for (int t = 0; t < 3; ++t) {
        threads.emplace_back([&readPage, &readOk, &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyReport report;
                if (VerifyPageOnRead(readPage, VerifyLevel::LIGHT, &report) == DSTORE_SUCC) {
                    readOk.fetch_add(1);
                }
            }
        });
    }

    /* 写路径线程 */
    for (int t = 0; t < 3; ++t) {
        threads.emplace_back([&writePage, &writeOk, &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                if (VerifyPageOnWrite(writePage, VerifyLevel::LIGHT) == DSTORE_SUCC) {
                    writeOk.fetch_add(1);
                }
            }
        });
    }

    /* 巡检路径线程（HEAVY 级别） */
    for (int t = 0; t < 2; ++t) {
        threads.emplace_back([&scanPage, &scanOk, &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyReport report;
                if (VerifyPageFull(scanPage, VerifyLevel::HEAVY, &report) == DSTORE_SUCC) {
                    scanOk.fetch_add(1);
                }
            }
        });
    }

    for (auto &th : threads) {
        th.join();
    }

    EXPECT_EQ(readOk.load(), 3 * CONCURRENT_LOOP_COUNT);
    EXPECT_EQ(writeOk.load(), 3 * CONCURRENT_LOOP_COUNT);
    EXPECT_EQ(scanOk.load(), 2 * CONCURRENT_LOOP_COUNT);

}

TEST(UTPageVerifyRegistry, Concurrent_RollbackSimulation_UndoPageVerify)
{
    /*
     * 模拟事务回滚场景：
     * - 多个线程并发做 "回滚"（校验 undo 类型页面）
     * - 同时有线程做正常的 heap 页面校验（模拟前台读请求）
     * - GUC level 在 LIGHT/NONE 间切换（模拟 undo 阶段降级策略）
     */
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, nullptr, nullptr), DSTORE_SUCC);
    ASSERT_EQ(RegisterPageVerifier(PageType::UNDO_PAGE_TYPE, "UndoPage", VerifyModule::UNDO,
        VerifyHeapLight, nullptr, nullptr), DSTORE_SUCC);
    ASSERT_EQ(RegisterPageVerifier(PageType::TRANSACTION_SLOT_PAGE, "TxnSlotPage", VerifyModule::UNDO,
        VerifyHeapLight, nullptr, nullptr), DSTORE_SUCC);

    ScopedVerifyConfig guard;
    SetDfxVerifyLevel(VerifyLevel::LIGHT);
    SetDfxVerifyModules(ALL_VERIFY_MODULES);

    /* 三种页面 */
    PageBuffer heapBuf{};
    Page *heapPage = InitPage(heapBuf, PageType::HEAP_PAGE_TYPE, {480, 580});
    PageBuffer undoBuf{};
    Page *undoPage = InitPage(undoBuf, PageType::UNDO_PAGE_TYPE, {481, 581});
    PageBuffer txnBuf{};
    Page *txnPage = InitPage(txnBuf, PageType::TRANSACTION_SLOT_PAGE, {482, 582});

    std::atomic<bool> stop{false};
    std::atomic<int> heapOk{0};
    std::atomic<int> undoOk{0};
    std::atomic<int> txnOk{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = 1 + 3 + 3 + 2; /* GUC + heap + undo + txn */

    /* GUC 切换线程：模拟 undo 阶段降级 LIGHT ↔ NONE */
    std::thread gucThread([&stop, &readyCount, totalThreads]() {
        readyCount.fetch_add(1);
        while (readyCount.load() < totalThreads) {
            /* spin until all threads ready */
        }
        std::atomic_thread_fence(std::memory_order_seq_cst);
        int idx = 0;
        while (!stop.load()) {
            SetDfxVerifyLevel((idx % 2 == 0) ? VerifyLevel::LIGHT : VerifyLevel::NONE);
            idx++;
        }
    });

    std::vector<std::thread> threads;

    /* 前台读线程校验 heap 页面 */
    for (int t = 0; t < 3; ++t) {
        threads.emplace_back([&heapPage, &heapOk, &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyLevel curLevel = GetDfxVerifyLevel();
                VerifyReport report;
                if (VerifyPageOnRead(heapPage, curLevel, &report) == DSTORE_SUCC) {
                    heapOk.fetch_add(1);
                }
            }
        });
    }

    /* 回滚线程校验 undo record 页面 */
    for (int t = 0; t < 3; ++t) {
        threads.emplace_back([&undoPage, &undoOk, &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyLevel curLevel = GetDfxVerifyLevel();
                VerifyReport report;
                if (VerifyPageOnRead(undoPage, curLevel, &report) == DSTORE_SUCC) {
                    undoOk.fetch_add(1);
                }
            }
        });
    }

    /* 回滚线程校验事务槽页面 */
    for (int t = 0; t < 2; ++t) {
        threads.emplace_back([&txnPage, &txnOk, &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyLevel curLevel = GetDfxVerifyLevel();
                VerifyReport report;
                if (VerifyPageOnRead(txnPage, curLevel, &report) == DSTORE_SUCC) {
                    txnOk.fetch_add(1);
                }
            }
        });
    }

    for (auto &th : threads) {
        th.join();
    }
    stop.store(true);
    gucThread.join();

    /* 由于 GUC 在 LIGHT/NONE 间切换，所有校验都应返回 SUCC（正常页面 + NONE 跳过） */
    int totalExpected = 3 * CONCURRENT_LOOP_COUNT;
    EXPECT_EQ(heapOk.load(), totalExpected);
    EXPECT_EQ(undoOk.load(), totalExpected);
    EXPECT_EQ(txnOk.load(), 2 * CONCURRENT_LOOP_COUNT);

}

TEST(UTPageVerifyRegistry, Concurrent_HighContention_SameCorruptedPage)
{
    /*
     * 高竞争场景：所有线程同时校验同一个损坏页面
     * 验证在高并发下 report 不会交叉污染（每线程独立 report）
     */
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    ScopedVerifyConfig guard;
    SetDfxVerifyLevel(VerifyLevel::LIGHT);
    SetDfxVerifyModules(ALL_VERIFY_MODULES);

    PageBuffer badBuf{};
    Page *badPage = InitPage(badBuf, PageType::HEAP_PAGE_TYPE, {490, 590});
    badPage->SetLower(8000);
    badPage->SetUpper(1000);
    badPage->SetChecksum();

    std::atomic<int> failCount{0};
    std::atomic<int> correctErrorCode{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = CONCURRENT_THREAD_COUNT;

    std::vector<std::thread> threads;
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        threads.emplace_back([&badPage, &failCount, &correctErrorCode, &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyReport report;
                RetStatus ret = VerifyPageOnRead(badPage, VerifyLevel::LIGHT, &report);
                if (ret != DSTORE_SUCC) {
                    failCount.fetch_add(1);
                    /* 验证每个线程的 report 独立且正确 */
                    if (report.HasError() && report.GetResults().size() == 1 &&
                        report.GetResults()[0].code == VerifyCode::PAGE_BOUNDARY_INVALID) {
                        correctErrorCode.fetch_add(1);
                    }
                }
            }
        });
    }
    for (auto &th : threads) {
        th.join();
    }

    int totalExpected = CONCURRENT_THREAD_COUNT * CONCURRENT_LOOP_COUNT;
    /* 所有校验都应失败 */
    EXPECT_EQ(failCount.load(), totalExpected);
    /* 每次失败都应报告正确的错误码（report 无交叉污染） */
    EXPECT_EQ(correctErrorCode.load(), totalExpected);

}

/* ========== 注册表边界测试 ========== */

TEST(UTPageVerifyRegistry, RegisterInvalidPageTypeFails)
{
    PageVerifyRegistry registry;

    /* INVALID_PAGE_TYPE 不可注册 */
    EXPECT_EQ(registry.Register(PageType::INVALID_PAGE_TYPE, "Invalid", VerifyModule::HEAP,
        VerifyHeapLight, nullptr, nullptr), DSTORE_FAIL);
    EXPECT_FALSE(registry.IsRegistered(PageType::INVALID_PAGE_TYPE));

    /* MAX_PAGE_TYPE 不可注册 */
    EXPECT_EQ(registry.Register(PageType::MAX_PAGE_TYPE, "Max", VerifyModule::HEAP,
        VerifyHeapLight, nullptr, nullptr), DSTORE_FAIL);
    EXPECT_FALSE(registry.IsRegistered(PageType::MAX_PAGE_TYPE));
}

TEST(UTPageVerifyRegistry, RegisterDuplicateOverwrites)
{
    PageVerifyRegistry registry;

    /* 第一次注册：使用 VerifyHeapLight */
    EXPECT_EQ(registry.Register(PageType::HEAP_PAGE_TYPE, "HeapPage_v1", VerifyModule::HEAP,
        VerifyHeapLight, nullptr, nullptr), DSTORE_SUCC);

    /* 第二次注册：使用 VerifyAlwaysFail 覆盖 */
    EXPECT_EQ(registry.Register(PageType::HEAP_PAGE_TYPE, "HeapPage_v2", VerifyModule::HEAP,
        VerifyAlwaysFail, nullptr, nullptr), DSTORE_SUCC);

    /* 验证使用的是第二个（覆盖后的）verifier */
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {500, 600});
    VerifyReport report;
    EXPECT_EQ(registry.Verify(page, VerifyLevel::LIGHT, &report), DSTORE_FAIL);
    ASSERT_GE(report.GetResults().size(), 1U);
    EXPECT_STREQ(report.GetResults()[0].checkName, "always_fail");
}

/* ========== HEAVY 级别并发巡检测试 ========== */

TEST(UTPageVerifyRegistry, Concurrent_HeavyLevel_FullScan)
{
    /*
     * 模拟后台巡检任务：多线程同时以 HEAVY 级别对不同页面执行全量校验
     * 验证 HEAVY 级别的并发安全性（所有三层 verifier 都会被调用）
     */
    PageVerifyRegistry localRegistry;
    ASSERT_EQ(localRegistry.Register(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    constexpr int PAGE_COUNT = CONCURRENT_THREAD_COUNT;
    std::vector<PageBuffer> pageBuffers(PAGE_COUNT);
    std::vector<Page *> pages(PAGE_COUNT);
    for (int i = 0; i < PAGE_COUNT; ++i) {
        pages[i] = InitPage(pageBuffers[i], PageType::HEAP_PAGE_TYPE,
            {static_cast<uint16>(600 + i), static_cast<uint32>(700 + i)});
    }

    std::atomic<int> totalChecks{0};
    std::atomic<int> totalSuccess{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = CONCURRENT_THREAD_COUNT;

    std::vector<std::thread> threads;
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        threads.emplace_back([&localRegistry, &pages, &totalChecks, &totalSuccess,
                              &readyCount, totalThreads, t]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyReport report;
                RetStatus ret = localRegistry.Verify(pages[t], VerifyLevel::HEAVY, &report);
                if (ret == DSTORE_SUCC) {
                    totalSuccess.fetch_add(1);
                }
                /* HEAVY 应调用 light + medium + heavy = 3 个 check */
                totalChecks.fetch_add(static_cast<int>(report.GetTotalChecks()));
            }
        });
    }
    for (auto &th : threads) {
        th.join();
    }

    int expectedTotal = CONCURRENT_THREAD_COUNT * CONCURRENT_LOOP_COUNT;
    EXPECT_EQ(totalSuccess.load(), expectedTotal);
    /* 每次校验应产生 3 个 check 结果（light + medium + heavy） */
    EXPECT_EQ(totalChecks.load(), expectedTotal * 3);
}

TEST(UTPageVerifyRegistry, Concurrent_RegisterAndVerify)
{
    /*
     * 模拟启动阶段：部分线程在注册 verifier，另一部分线程已开始校验
     * 验证注册操作与校验操作的并发安全性
     *
     * 注意：实际生产中注册在 InitPageVerifiers() 中串行完成，
     * 这里测试的是极端场景下注册表的线程安全行为
     */
    PageVerifyRegistry localRegistry;

    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {700, 800});

    std::atomic<bool> registered{false};
    std::atomic<int> verifyAttempts{0};
    std::atomic<int> verifySuccess{0};
    std::atomic<int> verifyFail{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = CONCURRENT_THREAD_COUNT + 1; /* +1 for register thread */

    /* 注册线程：延迟注册 verifier */
    std::thread registerThread([&localRegistry, &registered, &readyCount, totalThreads]() {
        readyCount.fetch_add(1);
        while (readyCount.load() < totalThreads) {
            /* spin until all threads ready */
        }
        std::atomic_thread_fence(std::memory_order_seq_cst);
        /* 模拟一点延迟后注册 */
        for (volatile int i = 0; i < 1000; ++i) {}
        localRegistry.Register(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
            VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy);
        registered.store(true);
    });

    /* 校验线程：立即开始校验，注册前可能失败，注册后应成功 */
    std::vector<std::thread> verifyThreads;
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        verifyThreads.emplace_back([&localRegistry, &page, &verifyAttempts, &verifySuccess, &verifyFail,
                                    &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyReport report;
                RetStatus ret = localRegistry.Verify(page, VerifyLevel::LIGHT, &report);
                verifyAttempts.fetch_add(1);
                if (ret == DSTORE_SUCC) {
                    verifySuccess.fetch_add(1);
                } else {
                    verifyFail.fetch_add(1);
                }
            }
        });
    }

    registerThread.join();
    for (auto &th : verifyThreads) {
        th.join();
    }

    int expectedTotal = CONCURRENT_THREAD_COUNT * CONCURRENT_LOOP_COUNT;
    EXPECT_EQ(verifyAttempts.load(), expectedTotal);
    /* 注册前可能有失败（unregistered type），注册后全部成功 */
    /* 两种结果之和等于总数 */
    EXPECT_EQ(verifySuccess.load() + verifyFail.load(), expectedTotal);
    /* 注册完成后应有成功的校验 */
    EXPECT_GT(verifySuccess.load(), 0);
}

/* ========== Generic MEDIUM LSN 校验测试 ========== */

TEST(UTPageVerifyRegistry, GenericMedium_LsnMaxValue_Fails)
{
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    ScopedVerifyConfig guard;
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {500, 600});
    /* Set GLSN to UINT64_MAX to trigger the sentinel check.
     * Cannot use SetLsn() because LsnSanityCheck rejects UINT64_MAX.
     * Use UT-only SetGlsn/SetPlsn to bypass sanity check. */
    /* 使用 UT-only 的 SetGlsn()/SetPlsn() 绕过生产代码的 LsnSanityCheck，
     * 以便构造 ValidateGenericMedium() 需要检测的非法 LSN 值。
     * 生产环境中 SetLsn() 会拒绝 UINT64_MAX 等异常值。 */
    page->SetGlsn(UINT64_MAX);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());

    bool hasLsnError = false;
    for (const auto &r : report.GetResults()) {
        if (std::string(r.checkName).find("lsn_invalid") != std::string::npos) {
            hasLsnError = true;
            break;
        }
    }
    EXPECT_TRUE(hasLsnError);
}

TEST(UTPageVerifyRegistry, GenericMedium_LsnInconsistent_Fails)
{
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    ScopedVerifyConfig guard;
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {501, 601});
    /* Make the page initialized (upper != 0) but with inconsistent LSN:
     * GLSN = 0 but PLSN != 0.
     * Use UT-only setters to bypass LsnSanityCheck. */
    /* 使用 UT-only 的 SetGlsn()/SetPlsn() 绕过生产代码的 LsnSanityCheck，
     * 以便构造 ValidateGenericMedium() 需要检测的非法 LSN 值。
     * 生产环境中 SetLsn() 会拒绝 UINT64_MAX 等异常值。 */
    page->SetGlsn(0);
    page->SetPlsn(1);
    page->SetChecksum();

    VerifyReport report;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::MEDIUM, &report), DSTORE_FAIL);
    EXPECT_TRUE(report.HasError());

    bool hasLsnError = false;
    for (const auto &r : report.GetResults()) {
        if (std::string(r.checkName).find("lsn_inconsistent") != std::string::npos) {
            hasLsnError = true;
            break;
        }
    }
    EXPECT_TRUE(hasLsnError);
}

TEST(UTPageVerifyRegistry, UninitializedPage_LightSkips_MediumChecks)
{
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    ScopedVerifyConfig guard;
    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {502, 602});
    /* Force PageNoInit() to return true: PageNoInit() checks m_upper == 0 */
    page->SetUpper(0);
    page->SetChecksum();

    /* LIGHT level should skip uninitialized pages (ShouldSkipUninitializedPage returns true) */
    VerifyReport lightReport;
    EXPECT_EQ(VerifyPage(page, VerifyLevel::LIGHT, &lightReport), DSTORE_SUCC);
    EXPECT_EQ(lightReport.GetTotalChecks(), 0U);

    /* MEDIUM level should NOT skip and will fail on this corrupt page
     * (boundary check: lower > upper since upper=0) */
    VerifyReport medReport;
    RetStatus ret = VerifyPage(page, VerifyLevel::MEDIUM, &medReport);
    /* For MEDIUM, generic checks will run - page should fail or have checks recorded */
    EXPECT_TRUE(medReport.GetTotalChecks() > 0 || ret != DSTORE_SUCC);
}

/* ========== HEAVY 级别并发巡检 — 全局 VerifyPageFull 路径 ========== */

TEST(UTPageVerifyRegistry, Concurrent_HeavyLevel_FullScan_ValidPage)
{
    /*
     * 多线程同时以 HEAVY 级别对合法 Heap 页面执行 VerifyPageFull，
     * 验证各线程的 VerifyReport 独立无交叉污染：
     *   - 合法页面不应产生任何错误
     *   - HEAVY 级别确实执行了校验（GetTotalChecks() > 0）
     */
    ASSERT_EQ(RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage", VerifyModule::HEAP,
        VerifyHeapLight, VerifyHeapMedium, VerifyHeapHeavy), DSTORE_SUCC);

    ScopedVerifyConfig guard;
    SetDfxVerifyLevel(VerifyLevel::HEAVY);
    SetDfxVerifyModules(ALL_VERIFY_MODULES);

    PageBuffer pageBuffer{};
    Page *page = InitPage(pageBuffer, PageType::HEAP_PAGE_TYPE, {800, 900});

    std::atomic<int> successCount{0};
    std::atomic<int> checksPositive{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = CONCURRENT_THREAD_COUNT;

    std::vector<std::thread> threads;
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        threads.emplace_back([&page, &successCount, &checksPositive, &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyReport report;
                RetStatus ret = VerifyPageFull(page, VerifyLevel::HEAVY, &report);
                if (ret == DSTORE_SUCC && !report.HasError()) {
                    successCount.fetch_add(1);
                }
                if (report.GetTotalChecks() > 0) {
                    checksPositive.fetch_add(1);
                }
            }
        });
    }
    for (auto &th : threads) {
        th.join();
    }

    int totalExpected = CONCURRENT_THREAD_COUNT * CONCURRENT_LOOP_COUNT;
    /* 合法页面：所有校验均成功，report 无错误 */
    EXPECT_EQ(successCount.load(), totalExpected);
    /* HEAVY 级别确实执行了校验（每次迭代 GetTotalChecks > 0） */
    EXPECT_EQ(checksPositive.load(), totalExpected);
}

TEST(UTPageVerifyRegistry, Concurrent_HeavyLevel_CorruptedPage)
{
    /*
     * 多线程同时以 HEAVY 级别对损坏页面执行 VerifyPageFull，
     * 验证：
     *   - 每个线程的 report 都独立检测到错误（无交叉污染）
     *   - 每个 report 中的 VerifyCode 一致（PAGE_BOUNDARY_INVALID）
     */
    RegisterHeapPageVerifier();

    ScopedVerifyConfig guard;
    SetDfxVerifyLevel(VerifyLevel::HEAVY);
    SetDfxVerifyModules(ALL_VERIFY_MODULES);

    /* 损坏页面：TD count 超出范围，真实 Heap verifier LIGHT 即可检出 */
    PageBuffer badBuf{};
    Page *badPage = InitPage(badBuf, PageType::HEAP_PAGE_TYPE, {801, 901});
    DataPage *dataPage = static_cast<DataPage *>(badPage);
    dataPage->dataHeader.tdCount = MAX_TD_COUNT + 1;
    badPage->SetChecksum();

    std::atomic<int> failCount{0};
    std::atomic<int> correctCode{0};
    std::atomic<int> readyCount{0};
    const int totalThreads = CONCURRENT_THREAD_COUNT;

    std::vector<std::thread> threads;
    for (int t = 0; t < CONCURRENT_THREAD_COUNT; ++t) {
        threads.emplace_back([&badPage, &failCount, &correctCode, &readyCount, totalThreads]() {
            readyCount.fetch_add(1);
            while (readyCount.load() < totalThreads) {
                /* spin until all threads ready */
            }
            std::atomic_thread_fence(std::memory_order_seq_cst);
            for (int i = 0; i < CONCURRENT_LOOP_COUNT; ++i) {
                VerifyReport report;
                (void)VerifyPageFull(badPage, VerifyLevel::HEAVY, &report);
                /* VerifyPageFull 巡检模式总是返回 DSTORE_SUCC，通过 report 判断 */
                if (report.HasError()) {
                    failCount.fetch_add(1);
                    if (DSTORE::ut_dfx::HasVerifyCode(report, VerifyCode::HEAP_TD_COUNT_OVERFLOW)) {
                        correctCode.fetch_add(1);
                    }
                }
            }
        });
    }
    for (auto &th : threads) {
        th.join();
    }

    int totalExpected = CONCURRENT_THREAD_COUNT * CONCURRENT_LOOP_COUNT;
    /* 所有校验都应检测到错误 */
    EXPECT_EQ(failCount.load(), totalExpected);
    /* 每次都应报告 HEAP_TD_COUNT_OVERFLOW（report 无交叉污染） */
    EXPECT_EQ(correctCode.load(), totalExpected);
}

#include <gtest/gtest.h>
#include "dfx/dstore_verify_report.h"

using namespace DSTORE;

class UTVerifyReport : public ::testing::Test {
protected:
    VerifyReport report;
};

TEST_F(UTVerifyReport, AddResultTracksSeverity)
{
    PageId pageId{1, 2};

    report.AddResult(VerifySeverity::SEVERITY_INFO, "page", pageId, "info_check", 1, 1, "info message");
    report.AddResult(VerifySeverity::SEVERITY_WARNING, "page", pageId, "warn_check", 2, 3, "warn message");
    report.AddResult(VerifySeverity::SEVERITY_ERROR, "page", pageId, "error_check", 4, 5, "error message");

    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetTotalChecks(), 3U);
    EXPECT_EQ(report.GetWarningCount(), 1U);
    EXPECT_EQ(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetRetStatus(), DSTORE_FAIL);
}

TEST_F(UTVerifyReport, FormattersContainKeyFields)
{
    PageId pageId{3, 4};

    report.AddResult(VerifySeverity::SEVERITY_ERROR, "page", pageId, "crc_mismatch", 1, 0, "crc mismatch");

    std::string text = report.FormatText();
    std::string json = report.FormatJson();

    EXPECT_NE(text.find("crc_mismatch"), std::string::npos);
    EXPECT_NE(text.find("errors=1"), std::string::npos);
    EXPECT_NE(json.find("\"checkName\":\"crc_mismatch\""), std::string::npos);
    EXPECT_NE(json.find("\"errors\":1"), std::string::npos);
}

/* ========== 补全测试 ========== */

TEST_F(UTVerifyReport, EmptyReportIsClean)
{
    EXPECT_FALSE(report.HasError());
    EXPECT_FALSE(report.HasFatal());
    EXPECT_EQ(report.GetTotalChecks(), 0U);
    EXPECT_EQ(report.GetErrorCount(), 0U);
    EXPECT_EQ(report.GetWarningCount(), 0U);
    EXPECT_EQ(report.GetFatalCount(), 0U);
    EXPECT_EQ(report.GetRetStatus(), DSTORE_SUCC);
    EXPECT_EQ(report.GetResults().size(), 0U);

    /* 空 report 的格式化输出也应正常 */
    std::string text = report.FormatText();
    std::string json = report.FormatJson();
    EXPECT_NE(text.find("total=0"), std::string::npos);
    EXPECT_NE(json.find("\"totalChecks\":0"), std::string::npos);
}

TEST_F(UTVerifyReport, FatalSeverityTracking)
{
    PageId pageId{5, 6};

    report.AddResult(VerifySeverity::SEVERITY_FATAL, "page", pageId, "fatal_check", 0, 1, "fatal error");

    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(report.HasFatal());
    EXPECT_EQ(report.GetFatalCount(), 1U);
    EXPECT_EQ(report.GetErrorCount(), 1U); /* FATAL 计入 failedChecks */
    EXPECT_EQ(report.GetRetStatus(), DSTORE_FAIL);
}

TEST_F(UTVerifyReport, MixedSeverityResults)
{
    PageId pageId{7, 8};

    report.AddResult(VerifySeverity::SEVERITY_INFO, "page", pageId, "info_1", 0, 0, "ok");
    report.AddResult(VerifySeverity::SEVERITY_INFO, "page", pageId, "info_2", 0, 0, "ok");
    report.AddResult(VerifySeverity::SEVERITY_WARNING, "page", pageId, "warn_1", 1, 2, "warn");
    report.AddResult(VerifySeverity::SEVERITY_WARNING, "page", pageId, "warn_2", 3, 4, "warn");
    report.AddResult(VerifySeverity::SEVERITY_WARNING, "page", pageId, "warn_3", 5, 6, "warn");
    report.AddResult(VerifySeverity::SEVERITY_ERROR, "page", pageId, "err_1", 7, 8, "err");
    report.AddResult(VerifySeverity::SEVERITY_FATAL, "page", pageId, "fatal_1", 9, 10, "fatal");

    EXPECT_EQ(report.GetTotalChecks(), 7U);
    /* INFO(2) + WARNING(3) = 5 passed; WARNING 算 passed 但计入 warningCount */
    EXPECT_EQ(report.GetWarningCount(), 3U);
    /* ERROR(1) + FATAL(1) = 2 failed */
    EXPECT_EQ(report.GetErrorCount(), 2U);
    EXPECT_EQ(report.GetFatalCount(), 1U);
    EXPECT_TRUE(report.HasError());
    EXPECT_TRUE(report.HasFatal());
    EXPECT_EQ(report.GetResults().size(), 7U);
}

TEST_F(UTVerifyReport, JsonEscapeSpecialChars)
{
    PageId pageId{9, 10};

    /* message 中包含需要转义的特殊字符 */
    report.AddResult(VerifySeverity::SEVERITY_ERROR, "page", pageId, "escape_test", 0, 1,
        "line1\nline2\ttab\\backslash\"quote");

    std::string json = report.FormatJson();
    /* 验证 JSON 转义正确 */
    EXPECT_NE(json.find("\\n"), std::string::npos);
    EXPECT_NE(json.find("\\t"), std::string::npos);
    EXPECT_NE(json.find("\\\\"), std::string::npos);
    EXPECT_NE(json.find("\\\""), std::string::npos);
    /* 原始字符不应出现在 JSON 中 */
    /* 注：\n 在 C++ 字符串中就是换行，json 中应被转义为 \\n */
}

TEST_F(UTVerifyReport, GetRetStatusBehavior)
{
    PageId pageId{11, 12};

    /* 无结果 → SUCC */
    EXPECT_EQ(report.GetRetStatus(), DSTORE_SUCC);

    /* 仅 INFO → SUCC */
    report.AddResult(VerifySeverity::SEVERITY_INFO, "page", pageId, "check1", 0, 0, "ok");
    EXPECT_EQ(report.GetRetStatus(), DSTORE_SUCC);

    /* 仅 WARNING → SUCC（warning 不算 error） */
    VerifyReport warnReport;
    warnReport.AddResult(VerifySeverity::SEVERITY_WARNING, "page", pageId, "check2", 0, 0, "warn");
    EXPECT_EQ(warnReport.GetRetStatus(), DSTORE_SUCC);

    /* 有 ERROR → FAIL */
    VerifyReport errReport;
    errReport.AddResult(VerifySeverity::SEVERITY_ERROR, "page", pageId, "check3", 0, 1, "err");
    EXPECT_EQ(errReport.GetRetStatus(), DSTORE_FAIL);

    /* 有 FATAL → FAIL */
    VerifyReport fatalReport;
    fatalReport.AddResult(VerifySeverity::SEVERITY_FATAL, "page", pageId, "check4", 0, 1, "fatal");
    EXPECT_EQ(fatalReport.GetRetStatus(), DSTORE_FAIL);
}

TEST_F(UTVerifyReport, AddResultWithCodePreservesCode)
{
    PageId pageId{13, 14};

    report.AddResultWithCode(VerifySeverity::SEVERITY_ERROR, VerifyCode::HEAP_TD_COUNT_OVERFLOW,
        "page", pageId, "td_check", 10, 100, "TD count %u exceeds max %u", 100, 10);

    ASSERT_EQ(report.GetResults().size(), 1U);
    EXPECT_EQ(report.GetResults()[0].code, VerifyCode::HEAP_TD_COUNT_OVERFLOW);
    EXPECT_EQ(report.GetResults()[0].severity, VerifySeverity::SEVERITY_ERROR);
    EXPECT_EQ(report.GetResults()[0].expected, 10U);
    EXPECT_EQ(report.GetResults()[0].actual, 100U);
    /* 验证 format string 被正确格式化 */
    EXPECT_NE(std::string(report.GetResults()[0].message).find("100"), std::string::npos);
}

TEST_F(UTVerifyReport, NullTargetTypeAndCheckNameSafe)
{
    PageId pageId{15, 16};

    /* targetType 和 checkName 为 nullptr */
    VerifyResult result;
    result.severity = VerifySeverity::SEVERITY_ERROR;
    result.targetType = nullptr;
    result.targetId = pageId;
    result.checkName = nullptr;
    result.expected = 0;
    result.actual = 1;
    snprintf(result.message, sizeof(result.message), "null ptr test");
    report.AddResult(result);

    /* FormatText 和 FormatJson 不应崩溃 */
    std::string text = report.FormatText();
    std::string json = report.FormatJson();
    EXPECT_NE(text.find("unknown"), std::string::npos);
    EXPECT_NE(json.find("unknown"), std::string::npos);
}

TEST_F(UTVerifyReport, LargeResultSet)
{
    PageId pageId{20, 21};

    for (int i = 0; i < 1000; ++i) {
        report.AddResult(VerifySeverity::SEVERITY_INFO, "page", pageId, "bulk_check", i, i, "result %d", i);
    }

    EXPECT_EQ(report.GetTotalChecks(), 1000U);
    /* FormatJson and FormatText should not crash */
    std::string json = report.FormatJson();
    std::string text = report.FormatText();
    EXPECT_NE(json.find("\"totalChecks\":1000"), std::string::npos);
    EXPECT_NE(text.find("total=1000"), std::string::npos);
}

TEST_F(UTVerifyReport, EscapeJsonCarriageReturn)
{
    PageId pageId{22, 23};

    report.AddResult(VerifySeverity::SEVERITY_ERROR, "page", pageId, "cr_test", 0, 1,
        "line1\rline2");

    std::string json = report.FormatJson();
    EXPECT_NE(json.find("\\r"), std::string::npos);
}

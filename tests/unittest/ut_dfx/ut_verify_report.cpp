#include <gtest/gtest.h>
#include "dfx/dstore_verify_report.h"

using namespace DSTORE;

TEST(UTVerifyReport, AddResultTracksSeverity)
{
    VerifyReport report;
    PageId pageId{1, 2};

    report.AddResult(VerifySeverity::INFO_LEVEL, "page", pageId, "info_check", 1, 1, "info message");
    report.AddResult(VerifySeverity::WARNING_LEVEL, "page", pageId, "warn_check", 2, 3, "warn message");
    report.AddResult(VerifySeverity::ERROR_LEVEL, "page", pageId, "error_check", 4, 5, "error message");

    EXPECT_TRUE(report.HasError());
    EXPECT_EQ(report.GetTotalChecks(), 3U);
    EXPECT_EQ(report.GetWarningCount(), 1U);
    EXPECT_EQ(report.GetErrorCount(), 1U);
    EXPECT_EQ(report.GetRetStatus(), DSTORE_FAIL);
}

TEST(UTVerifyReport, FormattersContainKeyFields)
{
    VerifyReport report;
    PageId pageId{3, 4};

    report.AddResult(VerifySeverity::ERROR_LEVEL, "page", pageId, "crc_mismatch", 1, 0, "crc mismatch");

    std::string text = report.FormatText();
    std::string json = report.FormatJson();

    EXPECT_NE(text.find("crc_mismatch"), std::string::npos);
    EXPECT_NE(text.find("errors=1"), std::string::npos);
    EXPECT_NE(json.find("\"checkName\":\"crc_mismatch\""), std::string::npos);
    EXPECT_NE(json.find("\"errors\":1"), std::string::npos);
}

#ifndef DSTORE_VERIFY_REPORT_H
#define DSTORE_VERIFY_REPORT_H

#include <string>
#include <vector>
#include "common/dstore_datatype.h"
#include "common/dstore_common_utils.h"
#include "page/dstore_page_struct.h"

namespace DSTORE {

enum class VerifySeverity : uint8 {
    INFO_LEVEL = 0,
    WARNING_LEVEL,
    ERROR_LEVEL
};

enum class VerifyLevel : uint8 {
    OFF = 0,
    LIGHTWEIGHT,
    HEAVYWEIGHT
};

enum class VerifyModule : uint8 {
    HEAP = 0,
    INDEX,
    ALL
};

struct VerifyResult {
    VerifySeverity severity{VerifySeverity::INFO_LEVEL};
    const char *targetType{nullptr};
    PageId targetId{INVALID_PAGE_ID};
    const char *checkName{nullptr};
    uint64 expected{0};
    uint64 actual{0};
    char message[256]{0};
};

class VerifyReport {
public:
    VerifyReport();
    ~VerifyReport() = default;

    DISALLOW_COPY_AND_MOVE(VerifyReport);

    void AddResult(const VerifyResult &result);
    void AddResult(VerifySeverity severity, const char *targetType, const PageId &targetId, const char *checkName,
        uint64 expected, uint64 actual, const char *format, ...);

    bool HasError() const;
    uint64 GetErrorCount() const;
    uint64 GetWarningCount() const;
    uint64 GetTotalChecks() const;
    RetStatus GetRetStatus() const;

    std::string FormatText() const;
    std::string FormatJson() const;

    const std::vector<VerifyResult> &GetResults() const;

private:
    static const char *SeverityToStr(VerifySeverity severity);
    static std::string EscapeJson(const char *input);

    std::vector<VerifyResult> m_results;
    uint64 m_totalChecks{0};
    uint64 m_passedChecks{0};
    uint64 m_failedChecks{0};
    uint64 m_warningCount{0};
    TimestampTz m_startTime{0};
    TimestampTz m_endTime{0};
};

}  // namespace DSTORE

#endif

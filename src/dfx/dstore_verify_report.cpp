#include "dfx/dstore_verify_report.h"

#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <sstream>

namespace DSTORE {

VerifyReport::VerifyReport() : m_startTime(GetCurrentTimestamp()), m_endTime(m_startTime)
{}

void VerifyReport::AddResult(const VerifyResult &result)
{
    m_results.push_back(result);
    ++m_totalChecks;
    if (result.severity == VerifySeverity::SEVERITY_ERROR || result.severity == VerifySeverity::SEVERITY_FATAL) {
        ++m_failedChecks;
        if (result.severity == VerifySeverity::SEVERITY_FATAL) {
            ++m_fatalCount;
        }
    } else {
        ++m_passedChecks;
        if (result.severity == VerifySeverity::SEVERITY_WARNING) {
            ++m_warningCount;
        }
    }
    m_endTime = GetCurrentTimestamp();
}

void VerifyReport::AddResult(VerifySeverity severity, const char *targetType, const PageId &targetId, const char *checkName,
    uint64 expected, uint64 actual, const char *format, ...)
{
    VerifyResult result;
    result.severity = severity;
    result.targetType = targetType;
    result.targetId = targetId;
    result.checkName = checkName;
    result.expected = expected;
    result.actual = actual;

    va_list args;
    va_start(args, format);
    int written = vsnprintf(result.message, sizeof(result.message), format, args);
    va_end(args);
    if (written < 0 || written >= static_cast<int>(sizeof(result.message))) {
        /* Mark truncation — overwrite last 4 bytes with "..." */
        constexpr size_t suffixLen = 3;
        memcpy(result.message + sizeof(result.message) - suffixLen - 1, "...", suffixLen + 1);
    }

    AddResult(result);
}

void VerifyReport::AddResultWithCode(VerifySeverity severity, VerifyCode code, const char *targetType, const PageId &targetId,
    const char *checkName, uint64 expected, uint64 actual, const char *format, ...)
{
    VerifyResult result;
    result.severity = severity;
    result.code = code;
    result.targetType = targetType;
    result.targetId = targetId;
    result.checkName = checkName;
    result.expected = expected;
    result.actual = actual;

    va_list args;
    va_start(args, format);
    int written = vsnprintf(result.message, sizeof(result.message), format, args);
    va_end(args);
    if (written < 0 || written >= static_cast<int>(sizeof(result.message))) {
        /* Mark truncation — overwrite last 4 bytes with "..." */
        constexpr size_t suffixLen = 3;
        memcpy(result.message + sizeof(result.message) - suffixLen - 1, "...", suffixLen + 1);
    }

    AddResult(result);
}

bool VerifyReport::HasError() const
{
    return m_failedChecks > 0;
}

bool VerifyReport::HasFatal() const
{
    return m_fatalCount > 0;
}

uint64 VerifyReport::GetErrorCount() const
{
    return m_failedChecks;
}

uint64 VerifyReport::GetWarningCount() const
{
    return m_warningCount;
}

uint64 VerifyReport::GetFatalCount() const
{
    return m_fatalCount;
}

uint64 VerifyReport::GetTotalChecks() const
{
    return m_totalChecks;
}

RetStatus VerifyReport::GetRetStatus() const
{
    return HasError() ? DSTORE_FAIL : DSTORE_SUCC;
}

std::string VerifyReport::FormatText() const
{
    std::ostringstream oss;
    oss << "VerifyReport summary: total=" << m_totalChecks << ", passed=" << m_passedChecks << ", warnings="
        << m_warningCount << ", errors=" << m_failedChecks << ", fatal=" << m_fatalCount << '\n';
    for (const VerifyResult &result : m_results) {
        oss << '[' << SeverityToStr(result.severity) << "] "
            << "code=0x" << std::hex << static_cast<uint32>(result.code) << std::dec << " "
            << (result.targetType == nullptr ? "unknown" : result.targetType)
            << '(' << result.targetId.m_fileId << ',' << result.targetId.m_blockId << ") "
            << (result.checkName == nullptr ? "unknown_check" : result.checkName)
            << " expected=" << result.expected << " actual=" << result.actual
            << " message=\"" << result.message << '"' << '\n';
    }
    return oss.str();
}

std::string VerifyReport::FormatJson() const
{
    std::ostringstream oss;
    oss << "{\"totalChecks\":" << m_totalChecks << ",\"warnings\":" << m_warningCount
        << ",\"errors\":" << m_failedChecks << ",\"fatal\":" << m_fatalCount << ",\"results\":[";
    for (size_t i = 0; i < m_results.size(); ++i) {
        const VerifyResult &result = m_results[i];
        if (i != 0) {
            oss << ',';
        }
        oss << "{\"severity\":\"" << SeverityToStr(result.severity) << "\",\"code\":"
            << static_cast<uint32>(result.code) << ",\"targetType\":\""
            << EscapeJson(result.targetType == nullptr ? "unknown" : result.targetType)
            << "\",\"fileId\":" << result.targetId.m_fileId << ",\"blockId\":" << result.targetId.m_blockId
            << ",\"checkName\":\"" << EscapeJson(result.checkName == nullptr ? "unknown_check" : result.checkName)
            << "\",\"expected\":" << result.expected << ",\"actual\":" << result.actual
            << ",\"message\":\"" << EscapeJson(result.message) << "\"}";
    }
    oss << "]}";
    return oss.str();
}

const std::vector<VerifyResult> &VerifyReport::GetResults() const
{
    return m_results;
}

const char *VerifyReport::SeverityToStr(VerifySeverity severity)
{
    switch (severity) {
        case VerifySeverity::SEVERITY_INFO:
            return "INFO";
        case VerifySeverity::SEVERITY_WARNING:
            return "WARNING";
        case VerifySeverity::SEVERITY_ERROR:
            return "ERROR";
        case VerifySeverity::SEVERITY_FATAL:
            return "FATAL";
        default:
            return "UNKNOWN";
    }
}

std::string VerifyReport::EscapeJson(const char *input)
{
    std::ostringstream oss;
    for (const char *ptr = input; ptr != nullptr && *ptr != '\0'; ++ptr) {
        switch (*ptr) {
            case '\\':
                oss << "\\\\";
                break;
            case '"':
                oss << "\\\"";
                break;
            case '\n':
                oss << "\\n";
                break;
            case '\r':
                oss << "\\r";
                break;
            case '\t':
                oss << "\\t";
                break;
            default:
                oss << *ptr;
                break;
        }
    }
    return oss.str();
}

}  // namespace DSTORE
#include "dfx/dstore_verify_context.h"

namespace DSTORE {

VerifyContext::VerifyContext(
    VerifyReport *report, SnapshotData *snapshot, float sampleRatio, bool isOnline, uint32 maxErrors)
    : m_report(report), m_snapshot(snapshot), m_isOnline(isOnline), m_maxErrors(maxErrors), m_sampleRatio(sampleRatio)
{}

VerifyReport *VerifyContext::GetReport() const
{
    return m_report;
}

SnapshotData *VerifyContext::GetSnapshot() const
{
    return m_snapshot;
}

bool VerifyContext::IsOnline() const
{
    return m_isOnline;
}

uint32 VerifyContext::GetMaxErrors() const
{
    return m_maxErrors;
}

bool VerifyContext::VisitPage(const PageId &pageId)
{
    return m_visitedPages.insert(PageIdToUint64(pageId)).second;
}

void VerifyContext::SetSampleRatio(float ratio)
{
    if (ratio < 0.0f) {
        m_sampleRatio = 0.0f;
    } else if (ratio > 1.0f) {
        m_sampleRatio = 1.0f;
    } else {
        m_sampleRatio = ratio;
    }
}

float VerifyContext::GetSampleRatio() const
{
    return m_sampleRatio;
}

bool VerifyContext::ShouldSamplePage(const PageId &pageId) const
{
    if (m_sampleRatio >= 1.0f) {
        return true;
    }
    if (m_sampleRatio <= 0.0f) {
        return false;
    }
    const uint64 hashValue = (static_cast<uint64>(pageId.m_fileId) << 32) ^
        (static_cast<uint64>(pageId.m_blockId) * 2654435761ULL);
    return hashValue % 10000 < static_cast<uint64>(m_sampleRatio * 10000);
}

void VerifyContext::ResetVisitedPages()
{
    m_visitedPages.clear();
}

bool VerifyContext::HasReachedErrorLimit() const
{
    return m_report != nullptr && m_report->GetErrorCount() >= m_maxErrors;
}

uint64 VerifyContext::PageIdToUint64(const PageId &pageId)
{
    return (static_cast<uint64>(pageId.m_fileId) << 32) | pageId.m_blockId;
}

}  // namespace DSTORE

#include "dfx/dstore_verify_context.h"

namespace DSTORE {

VerifyContext::VerifyContext(
    VerifyReport *report, SnapshotData *snapshot, float sampleRatio, bool isOnline, uint32 maxErrors)
    : m_report(report), m_snapshot(snapshot), m_sampleRatio(sampleRatio), m_isOnline(isOnline), m_maxErrors(maxErrors)
{}

VerifyReport *VerifyContext::GetReport() const
{
    return m_report;
}

SnapshotData *VerifyContext::GetSnapshot() const
{
    return m_snapshot;
}

float VerifyContext::GetSampleRatio() const
{
    return m_sampleRatio;
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

bool VerifyContext::HasReachedErrorLimit() const
{
    return m_report != nullptr && m_report->GetErrorCount() >= m_maxErrors;
}

uint64 VerifyContext::PageIdToUint64(const PageId &pageId)
{
    return (static_cast<uint64>(pageId.m_fileId) << 32) | pageId.m_blockId;
}

}  // namespace DSTORE

#ifndef DSTORE_VERIFY_CONTEXT_H
#define DSTORE_VERIFY_CONTEXT_H

#include <unordered_set>
#include "dfx/dstore_verify_report.h"
#include "transaction/dstore_transaction_struct.h"

namespace DSTORE {

class VerifyContext {
public:
    VerifyContext(VerifyReport *report, SnapshotData *snapshot = nullptr, float sampleRatio = 1.0f, bool isOnline = true,
        uint32 maxErrors = 1000);

    VerifyReport *GetReport() const;
    SnapshotData *GetSnapshot() const;
    float GetSampleRatio() const;
    bool IsOnline() const;
    uint32 GetMaxErrors() const;

    bool VisitPage(const PageId &pageId);
    bool HasReachedErrorLimit() const;

private:
    static uint64 PageIdToUint64(const PageId &pageId);

    VerifyReport *m_report{nullptr};
    SnapshotData *m_snapshot{nullptr};
    float m_sampleRatio{1.0f};
    bool m_isOnline{true};
    std::unordered_set<uint64> m_visitedPages;
    uint32 m_maxErrors{1000};
};

}  // namespace DSTORE

#endif

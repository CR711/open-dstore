/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version).
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 */

#ifndef STRESS_VERIFY_STATS_H
#define STRESS_VERIFY_STATS_H

#include <cstdint>
#include <vector>
#include <atomic>
#include <mutex>
#include <map>
#include "stress_verify_common.h"

namespace DSTORE {
namespace STRESS_VERIFY {

/* Maximum latency samples kept per thread to bound memory usage */
constexpr uint32_t MAX_LATENCY_SAMPLES = 500000;

/* Verification error record */
struct VerifyErrorRecord {
    uint32_t        errorId;
    FaultType       faultType;
    FaultTargetModule module;
    uint32_t        targetPage;
    VerifyCode      code;
    VerifySeverity  severity;
    char            message[200];
    uint64_t        timestampUs;
};

/* Per-thread statistics */
struct ThreadStats {
    std::atomic<uint64_t>   txCommitted{0};
    std::atomic<uint64_t>   txAborted{0};
    std::atomic<uint64_t>   readOps{0};
    std::atomic<uint64_t>   writeOps{0};
    std::atomic<uint64_t>   otherOps{0};

    /* Verification statistics */
    std::atomic<uint64_t>   verifyWriteChecks{0};
    std::atomic<uint64_t>   verifyWritePass{0};
    std::atomic<uint64_t>   verifyWriteErrors{0};
    std::atomic<uint64_t>   verifyReadChecks{0};
    std::atomic<uint64_t>   verifyReadPass{0};
    std::atomic<uint64_t>   verifyReadErrors{0};

    std::vector<uint64_t>   latencySamples;
    mutable std::mutex      sampleMutex;
    uint64_t                sampleCount{0};

    ThreadStats() = default;
    ThreadStats(ThreadStats &&other) noexcept
        : txCommitted(other.txCommitted.load()),
          txAborted(other.txAborted.load()),
          readOps(other.readOps.load()),
          writeOps(other.writeOps.load()),
          otherOps(other.otherOps.load()),
          verifyWriteChecks(other.verifyWriteChecks.load()),
          verifyWritePass(other.verifyWritePass.load()),
          verifyWriteErrors(other.verifyWriteErrors.load()),
          verifyReadChecks(other.verifyReadChecks.load()),
          verifyReadPass(other.verifyReadPass.load()),
          verifyReadErrors(other.verifyReadErrors.load()),
          latencySamples(std::move(other.latencySamples)),
          sampleCount(other.sampleCount) {}
    ThreadStats(const ThreadStats &) = delete;
    ThreadStats &operator=(const ThreadStats &) = delete;
};

/* Module-level error summary */
struct ModuleErrorSummary {
    uint64_t                totalCount{0};
    std::map<FaultType, uint64_t> faultTypeCounts;
};

class VerifyStats {
public:
    explicit VerifyStats(uint32_t threadNum);
    ~VerifyStats() = default;

    /* Called by worker threads during Run phase */
    void RecordTransaction(uint32_t threadId, uint64_t latencyUs,
                           bool success, uint64_t reads, uint64_t writes, uint64_t others);

    /* Verification result recording */
    void RecordVerifyWrite(uint32_t threadId, bool passed, VerifyCode code = VERIFY_OK);
    void RecordVerifyRead(uint32_t threadId, bool passed, VerifyCode code = VERIFY_OK);
    void RecordVerifyError(const VerifyErrorRecord &error);

    /* Called periodically by the reporter thread */
    void PrintIntervalReport(uint32_t elapsedSec);

    /* Called once at the end to print the full summary */
    void PrintFinalReport(uint64_t totalDurationUs);

    /* Reset all counters (called between warmup and run phases) */
    void Reset();

    /* Get total verification errors */
    uint64_t GetTotalVerifyErrors() const;

private:
    double CalcPercentile(std::vector<uint64_t> &samples, double pct);
    void PrintErrorSummary();

    uint32_t                        m_threadNum;
    std::vector<ThreadStats>        m_threadStats;

    /* Error records */
    std::vector<VerifyErrorRecord>  m_errorRecords;
    std::mutex                      m_errorMutex;

    /* Module-level error summaries */
    std::map<FaultTargetModule, ModuleErrorSummary> m_moduleSummaries;

    /* Snapshot values for interval delta computation */
    uint64_t                        m_lastSnapshotTx{0};
    uint64_t                        m_lastSnapshotRead{0};
    uint64_t                        m_lastSnapshotWrite{0};
    uint64_t                        m_lastSnapshotOther{0};
    uint64_t                        m_lastSnapshotVerifyW{0};
    uint64_t                        m_lastSnapshotVerifyR{0};
    uint64_t                        m_lastSnapshotUs{0};
};

} /* namespace STRESS_VERIFY */
} /* namespace DSTORE */

#endif /* STRESS_VERIFY_STATS_H */
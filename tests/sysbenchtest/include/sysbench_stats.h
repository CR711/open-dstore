/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 */

#ifndef SYSBENCH_STATS_H
#define SYSBENCH_STATS_H

#include <cstdint>
#include <vector>
#include <atomic>
#include <mutex>

namespace SYSBENCH {

/* Maximum latency samples kept per thread to bound memory usage */
constexpr uint32_t MAX_LATENCY_SAMPLES = 500000;

struct ThreadStats {
    std::atomic<uint64_t>   txCommitted{0};
    std::atomic<uint64_t>   txAborted{0};
    std::atomic<uint64_t>   readOps{0};
    std::atomic<uint64_t>   writeOps{0};
    std::atomic<uint64_t>   otherOps{0};

    std::vector<uint64_t>   latencySamples; /* microseconds */
    std::mutex              sampleMutex;
    uint64_t                sampleCount{0}; /* total samples seen (for reservoir) */

    ThreadStats() = default;
    ThreadStats(const ThreadStats &) = delete;
    ThreadStats &operator=(const ThreadStats &) = delete;
};

class SysbenchStats {
public:
    explicit SysbenchStats(uint32_t threadNum);
    ~SysbenchStats() = default;

    /* Called by worker threads during Run phase */
    void RecordTransaction(uint32_t threadId, uint64_t latencyUs,
                           bool success, uint64_t reads, uint64_t writes, uint64_t others);

    /* Called periodically by the reporter thread */
    void PrintIntervalReport(uint32_t elapsedSec);

    /* Called once at the end to print the full summary */
    void PrintFinalReport(uint64_t totalDurationUs);

    /* Reset all counters (called between warmup and run phases) */
    void Reset();

private:
    double CalcPercentile(std::vector<uint64_t> &samples, double pct);

    uint32_t                        m_threadNum;
    std::vector<ThreadStats>        m_threadStats;

    /* Snapshot values for interval delta computation */
    uint64_t                        m_lastSnapshotTx{0};
    uint64_t                        m_lastSnapshotRead{0};
    uint64_t                        m_lastSnapshotWrite{0};
    uint64_t                        m_lastSnapshotOther{0};
    uint64_t                        m_lastSnapshotUs{0};
};

} /* namespace SYSBENCH */

#endif /* SYSBENCH_STATS_H */

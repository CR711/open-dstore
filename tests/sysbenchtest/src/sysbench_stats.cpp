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

#include "sysbench_stats.h"
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <iomanip>
#include <random>

namespace SYSBENCH {

SysbenchStats::SysbenchStats(uint32_t threadNum)
    : m_threadNum(threadNum), m_threadStats(threadNum)
{
}

void SysbenchStats::RecordTransaction(uint32_t threadId, uint64_t latencyUs,
                                      bool success,
                                      uint64_t reads, uint64_t writes, uint64_t others)
{
    ThreadStats &ts = m_threadStats[threadId];
    if (success) {
        ts.txCommitted.fetch_add(1, std::memory_order_relaxed);
    } else {
        ts.txAborted.fetch_add(1, std::memory_order_relaxed);
    }
    ts.readOps.fetch_add(reads,   std::memory_order_relaxed);
    ts.writeOps.fetch_add(writes, std::memory_order_relaxed);
    ts.otherOps.fetch_add(others, std::memory_order_relaxed);

    /* Reservoir sampling to bound memory */
    std::lock_guard<std::mutex> lock(ts.sampleMutex);
    ts.sampleCount++;
    if (ts.latencySamples.size() < MAX_LATENCY_SAMPLES) {
        ts.latencySamples.push_back(latencyUs);
    } else {
        static thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<uint64_t> dist(0, ts.sampleCount - 1);
        uint64_t pos = dist(rng);
        if (pos < MAX_LATENCY_SAMPLES) {
            ts.latencySamples[pos] = latencyUs;
        }
    }
}

void SysbenchStats::Reset()
{
    for (auto &ts : m_threadStats) {
        ts.txCommitted.store(0, std::memory_order_relaxed);
        ts.txAborted.store(0, std::memory_order_relaxed);
        ts.readOps.store(0, std::memory_order_relaxed);
        ts.writeOps.store(0, std::memory_order_relaxed);
        ts.otherOps.store(0, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(ts.sampleMutex);
        ts.latencySamples.clear();
        ts.sampleCount = 0;
    }
    m_lastSnapshotTx    = 0;
    m_lastSnapshotRead  = 0;
    m_lastSnapshotWrite = 0;
    m_lastSnapshotOther = 0;
    m_lastSnapshotUs    = 0;
}

static uint64_t GetMonotonicUs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000ULL +
           static_cast<uint64_t>(ts.tv_nsec) / 1000ULL;
}

void SysbenchStats::PrintIntervalReport(uint32_t elapsedSec)
{
    uint64_t totalTx    = 0;
    uint64_t totalRead  = 0;
    uint64_t totalWrite = 0;
    uint64_t totalOther = 0;
    for (auto &ts : m_threadStats) {
        totalTx    += ts.txCommitted.load(std::memory_order_relaxed);
        totalRead  += ts.readOps.load(std::memory_order_relaxed);
        totalWrite += ts.writeOps.load(std::memory_order_relaxed);
        totalOther += ts.otherOps.load(std::memory_order_relaxed);
    }

    uint64_t nowUs       = GetMonotonicUs();
    double   intervalSec = (m_lastSnapshotUs == 0) ? static_cast<double>(elapsedSec)
                                                   : (nowUs - m_lastSnapshotUs) / 1e6;
    if (intervalSec <= 0.0) intervalSec = 1.0;

    uint64_t deltaTx    = totalTx    - m_lastSnapshotTx;
    uint64_t deltaRead  = totalRead  - m_lastSnapshotRead;
    uint64_t deltaWrite = totalWrite - m_lastSnapshotWrite;
    uint64_t deltaOther = totalOther - m_lastSnapshotOther;

    double tps = deltaTx    / intervalSec;
    double qps = (deltaRead + deltaWrite + deltaOther) / intervalSec;

    /* Collect all latency samples for P95 of this interval - use full merged set */
    std::vector<uint64_t> merged;
    for (auto &ts : m_threadStats) {
        std::lock_guard<std::mutex> lock(ts.sampleMutex);
        merged.insert(merged.end(), ts.latencySamples.begin(), ts.latencySamples.end());
    }
    double p95ms = 0.0;
    if (!merged.empty()) {
        p95ms = CalcPercentile(merged, 95.0) / 1000.0;
    }

    std::cout << "[ " << std::setw(3) << elapsedSec << "s ] "
              << "thds: " << m_threadNum
              << "; tps: " << std::fixed << std::setprecision(2) << tps
              << "; qps: " << std::fixed << std::setprecision(2) << qps
              << " (r/w/o: "
              << std::fixed << std::setprecision(2) << (deltaRead  / intervalSec) << "/"
              << std::fixed << std::setprecision(2) << (deltaWrite / intervalSec) << "/"
              << std::fixed << std::setprecision(2) << (deltaOther / intervalSec) << ")"
              << "; lat (ms,95%): " << std::fixed << std::setprecision(2) << p95ms
              << "; err/s: " << std::fixed << std::setprecision(2)
              << (m_threadStats[0].txAborted.load() / intervalSec)
              << std::endl;

    m_lastSnapshotTx    = totalTx;
    m_lastSnapshotRead  = totalRead;
    m_lastSnapshotWrite = totalWrite;
    m_lastSnapshotOther = totalOther;
    m_lastSnapshotUs    = nowUs;
}

void SysbenchStats::PrintFinalReport(uint64_t totalDurationUs)
{
    uint64_t totalCommit = 0;
    uint64_t totalAbort  = 0;
    uint64_t totalRead   = 0;
    uint64_t totalWrite  = 0;
    uint64_t totalOther  = 0;

    std::vector<uint64_t> allSamples;
    for (auto &ts : m_threadStats) {
        totalCommit += ts.txCommitted.load(std::memory_order_relaxed);
        totalAbort  += ts.txAborted.load(std::memory_order_relaxed);
        totalRead   += ts.readOps.load(std::memory_order_relaxed);
        totalWrite  += ts.writeOps.load(std::memory_order_relaxed);
        totalOther  += ts.otherOps.load(std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(ts.sampleMutex);
        allSamples.insert(allSamples.end(), ts.latencySamples.begin(), ts.latencySamples.end());
    }

    double durationSec = totalDurationUs / 1e6;
    uint64_t totalQueries = totalRead + totalWrite + totalOther;

    double tps = totalCommit / durationSec;
    double qps = totalQueries / durationSec;

    double latMin   = 0.0, latAvg = 0.0, latMax = 0.0;
    double latP50   = 0.0, latP95  = 0.0, latP99 = 0.0, latP999 = 0.0;
    double latSum   = 0.0;

    if (!allSamples.empty()) {
        latMin = *std::min_element(allSamples.begin(), allSamples.end()) / 1000.0;
        latMax = *std::max_element(allSamples.begin(), allSamples.end()) / 1000.0;
        for (auto v : allSamples) { latSum += v; }
        latAvg  = latSum / allSamples.size() / 1000.0;
        latSum  = latSum / 1000.0;
        latP50  = CalcPercentile(allSamples, 50.0)  / 1000.0;
        latP95  = CalcPercentile(allSamples, 95.0)  / 1000.0;
        latP99  = CalcPercentile(allSamples, 99.0)  / 1000.0;
        latP999 = CalcPercentile(allSamples, 99.9)  / 1000.0;
    }

    std::cout << std::endl;
    std::cout << "SQL statistics:" << std::endl;
    std::cout << "    queries performed:" << std::endl;
    std::cout << "        read:                            " << totalRead   << std::endl;
    std::cout << "        write:                           " << totalWrite  << std::endl;
    std::cout << "        other:                           " << totalOther  << std::endl;
    std::cout << "        total:                           " << totalQueries << std::endl;
    std::cout << "    transactions:                        "
              << totalCommit << " (" << std::fixed << std::setprecision(2) << tps << " per sec.)" << std::endl;
    std::cout << "    queries:                             "
              << totalQueries << " (" << std::fixed << std::setprecision(2) << qps << " per sec.)" << std::endl;
    std::cout << "    ignored errors:                      "
              << totalAbort << " (" << std::fixed << std::setprecision(2) << (totalAbort / durationSec) << " per sec.)" << std::endl;
    std::cout << std::endl;
    std::cout << "General statistics:" << std::endl;
    std::cout << "    total time:                          "
              << std::fixed << std::setprecision(4) << durationSec << "s" << std::endl;
    std::cout << "    total number of events:              " << totalCommit << std::endl;
    std::cout << std::endl;
    std::cout << "Latency (ms):" << std::endl;
    std::cout << "         min:                            " << std::fixed << std::setprecision(2) << latMin  << std::endl;
    std::cout << "         avg:                            " << std::fixed << std::setprecision(2) << latAvg  << std::endl;
    std::cout << "         max:                            " << std::fixed << std::setprecision(2) << latMax  << std::endl;
    std::cout << "         50th percentile:                " << std::fixed << std::setprecision(2) << latP50  << std::endl;
    std::cout << "         95th percentile:                " << std::fixed << std::setprecision(2) << latP95  << std::endl;
    std::cout << "         99th percentile:                " << std::fixed << std::setprecision(2) << latP99  << std::endl;
    std::cout << "         99.9th percentile:              " << std::fixed << std::setprecision(2) << latP999 << std::endl;
    std::cout << "         sum:                            " << std::fixed << std::setprecision(2) << latSum  << std::endl;
    std::cout << std::endl;

    /* Thread fairness */
    if (m_threadNum > 0) {
        double eventsAvg = static_cast<double>(totalCommit) / m_threadNum;
        double timeAvg   = durationSec;

        double eventsVar = 0.0;
        for (auto &ts : m_threadStats) {
            double diff = static_cast<double>(ts.txCommitted.load()) - eventsAvg;
            eventsVar += diff * diff;
        }
        double eventsStddev = (m_threadNum > 1) ? std::sqrt(eventsVar / m_threadNum) : 0.0;

        std::cout << "Threads fairness:" << std::endl;
        std::cout << "    events (avg/stddev):           "
                  << std::fixed << std::setprecision(4) << eventsAvg << "/"
                  << std::fixed << std::setprecision(2) << eventsStddev << std::endl;
        std::cout << "    execution time (avg/stddev):   "
                  << std::fixed << std::setprecision(4) << timeAvg << "/"
                  << "0.00" << std::endl;
    }
}

double SysbenchStats::CalcPercentile(std::vector<uint64_t> &samples, double pct)
{
    if (samples.empty()) { return 0.0; }
    std::sort(samples.begin(), samples.end());
    size_t idx = static_cast<size_t>((pct / 100.0) * (samples.size() - 1) + 0.5);
    if (idx >= samples.size()) { idx = samples.size() - 1; }
    return static_cast<double>(samples[idx]);
}

} /* namespace SYSBENCH */

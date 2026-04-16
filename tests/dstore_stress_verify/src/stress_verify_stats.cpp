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

#include "stress_verify_stats.h"

#include <iostream>
#include <iomanip>
#include <algorithm>
#include <chrono>

namespace DSTORE {
namespace STRESS_VERIFY {

VerifyStats::VerifyStats(uint32_t threadNum)
    : m_threadNum(threadNum)
{
    m_threadStats.resize(threadNum);
}

void VerifyStats::RecordTransaction(uint32_t threadId, uint64_t latencyUs,
                                     bool success, uint64_t reads, uint64_t writes, uint64_t others)
{
    if (threadId >= m_threadNum) return;

    ThreadStats &ts = m_threadStats[threadId];
    if (success) {
        ts.txCommitted.fetch_add(1, std::memory_order_relaxed);
    } else {
        ts.txAborted.fetch_add(1, std::memory_order_relaxed);
    }
    ts.readOps.fetch_add(reads, std::memory_order_relaxed);
    ts.writeOps.fetch_add(writes, std::memory_order_relaxed);
    ts.otherOps.fetch_add(others, std::memory_order_relaxed);

    /* Reservoir sampling for latency samples */
    std::lock_guard<std::mutex> lock(ts.sampleMutex);
    ++ts.sampleCount;
    if (ts.latencySamples.size() < MAX_LATENCY_SAMPLES) {
        ts.latencySamples.push_back(latencyUs);
    } else {
        /* Replace random existing sample */
        uint64_t replaceIdx = ts.sampleCount % MAX_LATENCY_SAMPLES;
        if (replaceIdx < ts.latencySamples.size()) {
            ts.latencySamples[replaceIdx] = latencyUs;
        }
    }
}

void VerifyStats::RecordVerifyWrite(uint32_t threadId, bool passed, VerifyCode code)
{
    if (threadId >= m_threadNum) return;

    ThreadStats &ts = m_threadStats[threadId];
    ts.verifyWriteChecks.fetch_add(1, std::memory_order_relaxed);
    if (passed) {
        ts.verifyWritePass.fetch_add(1, std::memory_order_relaxed);
    } else {
        ts.verifyWriteErrors.fetch_add(1, std::memory_order_relaxed);
    }
}

void VerifyStats::RecordVerifyRead(uint32_t threadId, bool passed, VerifyCode code)
{
    if (threadId >= m_threadNum) return;

    ThreadStats &ts = m_threadStats[threadId];
    ts.verifyReadChecks.fetch_add(1, std::memory_order_relaxed);
    if (passed) {
        ts.verifyReadPass.fetch_add(1, std::memory_order_relaxed);
    } else {
        ts.verifyReadErrors.fetch_add(1, std::memory_order_relaxed);
    }
}

void VerifyStats::RecordVerifyError(const VerifyErrorRecord &error)
{
    std::lock_guard<std::mutex> lock(m_errorMutex);
    m_errorRecords.push_back(error);

    /* Update module summary */
    if (m_moduleSummaries.find(error.module) == m_moduleSummaries.end()) {
        m_moduleSummaries[error.module] = ModuleErrorSummary();
    }
    ModuleErrorSummary &summary = m_moduleSummaries[error.module];
    ++summary.totalCount;
    ++summary.faultTypeCounts[error.faultType];
}

uint64_t VerifyStats::GetTotalVerifyErrors() const
{
    uint64_t total = 0;
    for (const auto &ts : m_threadStats) {
        total += ts.verifyWriteErrors.load(std::memory_order_relaxed);
        total += ts.verifyReadErrors.load(std::memory_order_relaxed);
    }
    return total;
}

void VerifyStats::Reset()
{
    for (auto &ts : m_threadStats) {
        ts.txCommitted.store(0);
        ts.txAborted.store(0);
        ts.readOps.store(0);
        ts.writeOps.store(0);
        ts.otherOps.store(0);
        ts.verifyWriteChecks.store(0);
        ts.verifyWritePass.store(0);
        ts.verifyWriteErrors.store(0);
        ts.verifyReadChecks.store(0);
        ts.verifyReadPass.store(0);
        ts.verifyReadErrors.store(0);
        std::lock_guard<std::mutex> lock(ts.sampleMutex);
        ts.latencySamples.clear();
        ts.sampleCount = 0;
    }
    m_lastSnapshotTx = 0;
    m_lastSnapshotRead = 0;
    m_lastSnapshotWrite = 0;
    m_lastSnapshotOther = 0;
    m_lastSnapshotVerifyW = 0;
    m_lastSnapshotVerifyR = 0;
    auto now = std::chrono::system_clock::now();
    m_lastSnapshotUs = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();
}

double VerifyStats::CalcPercentile(std::vector<uint64_t> &samples, double pct)
{
    if (samples.empty()) return 0.0;
    std::sort(samples.begin(), samples.end());
    size_t idx = static_cast<size_t>(pct / 100.0 * (samples.size() - 1));
    return static_cast<double>(samples[idx]) / 1000.0;  /* Convert us to ms */
}

void VerifyStats::PrintIntervalReport(uint32_t elapsedSec)
{
    /* Aggregate current snapshot */
    uint64_t curTx = 0, curRead = 0, curWrite = 0, curOther = 0;
    uint64_t curVerifyW = 0, curVerifyR = 0, curVerifyWErr = 0, curVerifyRErr = 0;

    for (const auto &ts : m_threadStats) {
        curTx += ts.txCommitted.load(std::memory_order_relaxed) +
                 ts.txAborted.load(std::memory_order_relaxed);
        curRead += ts.readOps.load(std::memory_order_relaxed);
        curWrite += ts.writeOps.load(std::memory_order_relaxed);
        curOther += ts.otherOps.load(std::memory_order_relaxed);
        curVerifyW += ts.verifyWriteChecks.load(std::memory_order_relaxed);
        curVerifyR += ts.verifyReadChecks.load(std::memory_order_relaxed);
        curVerifyWErr += ts.verifyWriteErrors.load(std::memory_order_relaxed);
        curVerifyRErr += ts.verifyReadErrors.load(std::memory_order_relaxed);
    }

    /* Calculate deltas */
    uint64_t deltaTx = curTx - m_lastSnapshotTx;
    uint64_t deltaVerifyW = curVerifyW - m_lastSnapshotVerifyW;
    uint64_t deltaVerifyR = curVerifyR - m_lastSnapshotVerifyR;

    auto now = std::chrono::system_clock::now();
    uint64_t nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();

    double intervalSec = static_cast<double>(nowUs - m_lastSnapshotUs) / 1e6;
    if (intervalSec <= 0) intervalSec = 1.0;

    double tps = static_cast<double>(deltaTx) / intervalSec;

    /* Print interval report */
    std::cout << "[" << elapsedSec << "s] TPS: " << std::fixed << std::setprecision(1) << tps
              << " | Verify: write=" << deltaVerifyW;
    if (curVerifyWErr == 0) {
        std::cout << "(pass)";
    } else {
        std::cout << "(" << curVerifyWErr << " ERROR)";
    }
    std::cout << " read=" << deltaVerifyR;
    if (curVerifyRErr == 0) {
        std::cout << "(pass)";
    } else {
        std::cout << "(" << curVerifyRErr << " ERROR)";
    }
    std::cout << " errors=" << (curVerifyWErr + curVerifyRErr) << std::endl;

    /* Update snapshot */
    m_lastSnapshotTx = curTx;
    m_lastSnapshotRead = curRead;
    m_lastSnapshotWrite = curWrite;
    m_lastSnapshotOther = curOther;
    m_lastSnapshotVerifyW = curVerifyW;
    m_lastSnapshotVerifyR = curVerifyR;
    m_lastSnapshotUs = nowUs;
}

void VerifyStats::PrintFinalReport(uint64_t totalDurationUs)
{
    /* Aggregate all thread stats */
    uint64_t totalTx = 0, totalTxCommit = 0, totalTxAbort = 0;
    uint64_t totalRead = 0, totalWrite = 0, totalOther = 0;
    uint64_t totalVerifyW = 0, totalVerifyWPass = 0, totalVerifyWErr = 0;
    uint64_t totalVerifyR = 0, totalVerifyRPass = 0, totalVerifyRErr = 0;

    std::vector<uint64_t> allLatencySamples;
    for (const auto &ts : m_threadStats) {
        totalTxCommit += ts.txCommitted.load(std::memory_order_relaxed);
        totalTxAbort += ts.txAborted.load(std::memory_order_relaxed);
        totalRead += ts.readOps.load(std::memory_order_relaxed);
        totalWrite += ts.writeOps.load(std::memory_order_relaxed);
        totalOther += ts.otherOps.load(std::memory_order_relaxed);
        totalVerifyW += ts.verifyWriteChecks.load(std::memory_order_relaxed);
        totalVerifyWPass += ts.verifyWritePass.load(std::memory_order_relaxed);
        totalVerifyWErr += ts.verifyWriteErrors.load(std::memory_order_relaxed);
        totalVerifyR += ts.verifyReadChecks.load(std::memory_order_relaxed);
        totalVerifyRPass += ts.verifyReadPass.load(std::memory_order_relaxed);
        totalVerifyRErr += ts.verifyReadErrors.load(std::memory_order_relaxed);

        std::lock_guard<std::mutex> lock(ts.sampleMutex);
        allLatencySamples.insert(allLatencySamples.end(),
                                 ts.latencySamples.begin(), ts.latencySamples.end());
    }
    totalTx = totalTxCommit + totalTxAbort;

    double totalSec = static_cast<double>(totalDurationUs) / 1e6;
    double avgTps = static_cast<double>(totalTx) / totalSec;

    std::cout << "\n=== Final Report ===" << std::endl;
    std::cout << "Transactions:    " << totalTx << " (" << std::fixed << std::setprecision(1)
              << avgTps << "/sec)" << std::endl;
    std::cout << "  Committed:     " << totalTxCommit << std::endl;
    std::cout << "  Aborted:       " << totalTxAbort << std::endl;

    std::cout << "\nVerify Statistics:" << std::endl;
    std::cout << "  Write path:  " << totalVerifyW << " checks, " << totalVerifyWErr << " errors"
              << std::endl;
    std::cout << "  Read path:   " << totalVerifyR << " checks, " << totalVerifyRErr << " errors"
              << std::endl;

    if (!allLatencySamples.empty()) {
        std::cout << "\nLatency (ms):" << std::endl;
        std::cout << "  avg: " << std::fixed << std::setprecision(2)
                  << CalcPercentile(allLatencySamples, 50) << std::endl;
        std::cout << "  p95: " << std::fixed << std::setprecision(2)
                  << CalcPercentile(allLatencySamples, 95) << std::endl;
        std::cout << "  p99: " << std::fixed << std::setprecision(2)
                  << CalcPercentile(allLatencySamples, 99) << std::endl;
    }

    /* Print error summary if any */
    if (totalVerifyWErr + totalVerifyRErr > 0) {
        PrintErrorSummary();
    }
}

void VerifyStats::PrintErrorSummary()
{
    std::cout << "\n=== Verification Errors ===" << std::endl;
    uint32_t errorNum = 1;
    for (const auto &err : m_errorRecords) {
        std::cout << "[ERROR #" << errorNum << "] Module: " << ModuleToString(err.module)
                  << " | Page: " << err.targetPage
                  << " | Fault: " << FaultTypeToString(err.faultType) << std::endl;
        std::cout << "  Code: 0x" << std::hex << err.code << std::dec << std::endl;
        std::cout << "  Message: " << err.message << std::endl;
        ++errorNum;
    }

    std::cout << "\nError Summary by Module:" << std::endl;
    for (const auto &kv : m_moduleSummaries) {
        std::cout << "  " << ModuleToString(kv.first) << ": " << kv.second.totalCount
                  << " error(s)" << std::endl;
        for (const auto &ft : kv.second.faultTypeCounts) {
            std::cout << "    - " << FaultTypeToString(ft.first) << ": " << ft.second << std::endl;
        }
    }
}

} /* namespace STRESS_VERIFY */
} /* namespace DSTORE */
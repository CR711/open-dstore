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

#include "sysbench_client.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "cjson/cJSON.h"
#include "common/dstore_datatype.h"
#include "framework/dstore_session_interface.h"
#include "framework/dstore_thread.h"
#include "framework/dstore_thread_interface.h"
#include "securec.h"
#include "table_handler.h"
#include "transaction/dstore_transaction_interface.h"
#include "tuple/dstore_memheap_tuple.h"
#include "tuple/dstore_tuple_interface.h"

using namespace DSTORE;

namespace SYSBENCH {

static const std::string SYSBENCH_CONFIG_PATH("config.json");

/* Thread-local random seed */
thread_local uint32_t gSysbenchSeed;

/* Thread-local table handler cache (one set per worker thread) */
thread_local std::map<std::string, DstoreTableHandler *> tls_tableHandlers;

/* ----------------------------------------------------------------
 * Thread lifecycle helpers (same pattern as tpcctest)
 * ---------------------------------------------------------------- */
static void CreateThreadAndRegister()
{
    g_instance->CreateThreadAndRegister(g_defaultPdbId);
    (void)pthread_setname_np(pthread_self(), "sysbench");
    volatile uint32_t *holdoffCount = new uint32_t(0);
    ThreadCore *core = thrd->GetCore();
    core->interruptHoldoffCount = holdoffCount;
    ThreadContextInterface::GetCurrentThreadContext()->InitTransactionRuntime(
        g_defaultPdbId, nullptr, nullptr);
    std::random_device sd;
    gSysbenchSeed = sd();
}

static void UnregisterThread()
{
    ThreadCore *core = thrd->GetCore();
    if (core != nullptr && core->interruptHoldoffCount != nullptr) {
        delete core->interruptHoldoffCount;
        core->interruptHoldoffCount = nullptr;
    }
    g_instance->UnregisterThread();
}

static void ClearTlsTableHandlers()
{
    for (auto it = tls_tableHandlers.begin(); it != tls_tableHandlers.end();) {
        delete it->second;
        it->second = nullptr;
        it = tls_tableHandlers.erase(it);
    }
}

/* ----------------------------------------------------------------
 * Helper: monotonic time in microseconds
 * ---------------------------------------------------------------- */
static uint64_t GetMonotonicUs()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000ULL +
           static_cast<uint64_t>(ts.tv_nsec) / 1000ULL;
}

/* ================================================================
 * SysbenchStorage implementation
 * ================================================================ */

SysbenchStorage::~SysbenchStorage()
{
    ClearTableHandlers();
    delete m_stats;
}

void SysbenchStorage::Init(int32_t nodeId)
{
    m_nodeId = nodeId;
    LoadConfig(SYSBENCH_CONFIG_PATH);
    m_stats = new SysbenchStats(m_config.threadNum);
}

void SysbenchStorage::LoadConfig(const std::string &configPath)
{
    std::ifstream f(configPath);
    if (!f.is_open()) {
        std::cout << "Cannot open config: " << configPath << std::endl;
        exit(1);
    }
    std::string msg((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    f.close();

    cJSON *json = cJSON_Parse(msg.c_str());
    std::cout << "--------------------Sysbench Config in "
              << configPath << "--------------------" << std::endl;

    m_config.tableNum         = cJSON_GetObjectItem(json, "tables")->valueint;
    m_config.tableSize        = cJSON_GetObjectItem(json, "table_size")->valueint;
    m_config.threadNum        = cJSON_GetObjectItem(json, "threads")->valueint;
    m_config.durationSec      = cJSON_GetObjectItem(json, "time")->valueint;
    m_config.warmupSec        = cJSON_GetObjectItem(json, "warmup_time")->valueint;
    m_config.reportInterval   = cJSON_GetObjectItem(json, "report_interval")->valueint;
    m_config.rangeSize        = cJSON_GetObjectItem(json, "range_size")->valueint;
    m_config.pointSelects     = cJSON_GetObjectItem(json, "point_selects")->valueint;
    m_config.simpleRanges     = cJSON_GetObjectItem(json, "simple_ranges")->valueint;
    m_config.sumRanges        = cJSON_GetObjectItem(json, "sum_ranges")->valueint;
    m_config.orderRanges      = cJSON_GetObjectItem(json, "order_ranges")->valueint;
    m_config.distinctRanges   = cJSON_GetObjectItem(json, "distinct_ranges")->valueint;
    m_config.indexUpdates     = cJSON_GetObjectItem(json, "index_updates")->valueint;
    m_config.nonIndexUpdates  = cJSON_GetObjectItem(json, "non_index_updates")->valueint;
    m_config.deleteInserts    = cJSON_GetObjectItem(json, "delete_inserts")->valueint;
    m_config.skipTrx          = (cJSON_GetObjectItem(json, "skip_trx")->valueint != 0);
    m_config.secondaryIndex   = (cJSON_GetObjectItem(json, "secondary")->valueint != 0);
    m_config.autoInc          = (cJSON_GetObjectItem(json, "auto_inc")->valueint != 0);

    const char *cmd = cJSON_GetObjectItem(json, "command")->valuestring;
    if (strcmp(cmd, "prepare") == 0) {
        m_config.command = CMD_PREPARE;
    } else if (strcmp(cmd, "run") == 0) {
        m_config.command = CMD_RUN;
    } else if (strcmp(cmd, "cleanup") == 0) {
        m_config.command = CMD_CLEANUP;
    } else {
        m_config.command = CMD_ALL;
    }

    /* mode: read_write (default) | read_only | write_only */
    cJSON *modeItem = cJSON_GetObjectItem(json, "mode");
    const char *modeStr = (modeItem != nullptr && modeItem->valuestring != nullptr)
                          ? modeItem->valuestring : "read_write";
    if (strcmp(modeStr, "read_only") == 0) {
        m_config.mode = MODE_READ_ONLY;
        m_config.simpleRanges    = 0;
        m_config.sumRanges       = 0;
        m_config.orderRanges     = 0;
        m_config.distinctRanges  = 0;
        m_config.indexUpdates    = 0;
        m_config.nonIndexUpdates = 0;
        m_config.deleteInserts   = 0;
        m_config.skipTrx         = true;
    } else if (strcmp(modeStr, "write_only") == 0) {
        m_config.mode = MODE_WRITE_ONLY;
        m_config.pointSelects   = 0;
        m_config.simpleRanges   = 0;
        m_config.sumRanges      = 0;
        m_config.orderRanges    = 0;
        m_config.distinctRanges = 0;
    } else {
        m_config.mode = MODE_READ_WRITE;
    }

    cJSON_Delete(json);

    const char *modeLabel = (m_config.mode == MODE_READ_ONLY)  ? "read_only"  :
                            (m_config.mode == MODE_WRITE_ONLY) ? "write_only" : "read_write";
    std::cout << "tables: "         << m_config.tableNum
              << ", table_size: "   << m_config.tableSize
              << ", threads: "      << m_config.threadNum
              << ", time: "         << m_config.durationSec << "s"
              << ", warmup: "       << m_config.warmupSec   << "s"
              << ", mode: "         << modeLabel
              << std::endl << std::endl;
}

/* Build table name: sbtest1, sbtest2, ... */
static std::string TableName(uint32_t tableIdx)
{
    return std::string("sbtest") + std::to_string(tableIdx + 1);
}

/* Build index name for the primary key index on id */
static std::string PrimaryIndexName(uint32_t tableIdx)
{
    char *name = TableDataGenerator::GenerateIndexName(
        TableName(tableIdx).c_str(),
        SBTEST_PRIMARY_INDEX_DESC[0].indexCol,
        SBTEST_PRIMARY_INDEX_DESC[0].indexAttrNum);
    std::string result(name);
    DestroyObject((void **)&name);
    return result;
}

DstoreTableHandler *SysbenchStorage::GetOrCreateTableHandler(uint32_t tableIdx, bool withIndex)
{
    std::string tName = TableName(tableIdx);
    std::string iName = withIndex ? PrimaryIndexName(tableIdx) : "";
    return simulator->GetTableHandler(tName.c_str(),
                                      withIndex ? iName.c_str() : nullptr);
}

void SysbenchStorage::ClearTableHandlers()
{
    for (auto *h : m_tableHandlers)   { delete h; }
    for (auto *h : m_heapOnlyHandlers){ delete h; }
    m_tableHandlers.clear();
    m_heapOnlyHandlers.clear();
}

/* ----------------------------------------------------------------
 * CreateTables
 * ---------------------------------------------------------------- */
void SysbenchStorage::CreateTables(uint32_t *allocedMaxRelOid)
{
    std::cout << "--------------------Start Create Tables--------------------" << std::endl;
    for (uint32_t i = 0; i < m_config.tableNum; ++i) {
        std::string tName = TableName(i);
        DstoreTableHandler tableHandler(g_instance);
        TableDataGenerator generator(tName.c_str(), SBTEST_COL_DESC, SBTEST_COL_MAX);
        generator.GenerationTableInfo();
        TableInfo tableInfo = generator.GetTableInfo();
        int ret = tableHandler.CreateTable(tableInfo);
        if (ret == 1) {
            std::cout << "Create " << tName << " failed (already exists)" << std::endl;
        } else {
            *allocedMaxRelOid = simulator->GetCurOid();
            std::cout << "Create " << tName << " success" << std::endl;
        }
    }
    std::cout << "--------------------Finish Create Tables--------------------" << std::endl
              << std::endl;
}

/* ----------------------------------------------------------------
 * CreateIndexes
 * ---------------------------------------------------------------- */
void SysbenchStorage::CreateIndexes(uint32_t *allocedMaxRelOid)
{
    std::cout << "--------------------Start Create Indexes--------------------" << std::endl;
    for (uint32_t i = 0; i < m_config.tableNum; ++i) {
        std::string tName = TableName(i);

        /* Primary index on id */
        DstoreTableHandler *handler = simulator->GetTableHandler(tName.c_str(), nullptr);
        TableDataGenerator generator(tName.c_str(), SBTEST_COL_DESC, SBTEST_COL_MAX);
        TableInfo tableInfo = generator.GetTableInfo();
        tableInfo.indexDesc = SBTEST_PRIMARY_INDEX_DESC;

        TableDataGenerator indexGen;
        indexGen.GenerationIndexTableInfo(tableInfo);
        TableInfo indexInfo = indexGen.GetTableInfo();

        TransactionInterface::StartTrxCommand();
        TransactionInterface::SetSnapShot();
        int ret = handler->CreateIndex(indexInfo);
        delete handler;
        if (ret == 0) {
            TransactionInterface::CommitTrxCommand();
            *allocedMaxRelOid = simulator->GetCurOid();
            std::cout << "Create index on " << tName << " success" << std::endl;
        } else {
            TransactionInterface::AbortTrx();
            std::cout << "Create index on " << tName << " failed" << std::endl;
        }
    }
    std::cout << "--------------------Finish Create Indexes--------------------" << std::endl
              << std::endl;
}

/* ----------------------------------------------------------------
 * LoadData - distribute rows across loader threads per table
 * ---------------------------------------------------------------- */
void SysbenchStorage::LoadTableData(uint32_t tableIdx, uint32_t rowStart, uint32_t rowEnd)
{
    std::string tName = TableName(tableIdx);
    DstoreTableHandler *handler = simulator->GetTableHandler(tName.c_str(), nullptr);

    char cBuf[SBTEST_C_LEN + 1];
    char padBuf[SBTEST_PAD_LEN + 1];

    /* commit every BATCH_SIZE rows */
    constexpr uint32_t BATCH_SIZE = 1000;
    uint32_t batchCount = 0;

    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();

    for (uint32_t id = rowStart; id <= rowEnd; ++id) {
        Datum   values[SBTEST_COL_MAX];
        bool    isNulls[SBTEST_COL_MAX] = {false};

        int32_t k = static_cast<int32_t>(rand_r(&gSysbenchSeed) % m_config.tableSize);

        /* Build c: 11 groups of 3 digits joined by '-' */
        int pos = 0;
        for (int g = 0; g < 11; ++g) {
            if (g > 0) { cBuf[pos++] = '-'; }
            int v = rand_r(&gSysbenchSeed) % 1000;
            pos += sprintf_s(cBuf + pos, sizeof(cBuf) - pos, "%03d", v);
        }
        cBuf[pos] = '\0';

        /* Build pad: 5 groups of 2 digits joined by '-' */
        pos = 0;
        for (int g = 0; g < 5; ++g) {
            if (g > 0) { padBuf[pos++] = '-'; }
            int v = rand_r(&gSysbenchSeed) % 100;
            pos += sprintf_s(padBuf + pos, sizeof(padBuf) - pos, "%02d", v);
        }
        padBuf[pos] = '\0';

        /* Build varlena-format strings for c and pad columns */
        uint64_t cLen   = strlen(cBuf);
        text *cText = (text *)malloc(VARHDRSZ + cLen + 1);
        DstoreSetVarSize(cText, VARHDRSZ + cLen + 1);
        (void)memcpy_s(cText->vl_dat, cLen + 1, cBuf, cLen + 1);

        uint64_t padLen = strlen(padBuf);
        text *padText = (text *)malloc(VARHDRSZ + padLen + 1);
        DstoreSetVarSize(padText, VARHDRSZ + padLen + 1);
        (void)memcpy_s(padText->vl_dat, padLen + 1, padBuf, padLen + 1);

        values[SBTEST_COL_ID]  = Int32GetDatum(static_cast<int32_t>(id));
        values[SBTEST_COL_K]   = Int32GetDatum(k);
        values[SBTEST_COL_C]   = PointerGetDatum(cText);
        values[SBTEST_COL_PAD] = PointerGetDatum(padText);

        int ret = handler->Insert(values, isNulls);
        free(cText);
        free(padText);
        if (ret != 0) {
            std::cout << "Insert row " << id << " into " << tName << " failed" << std::endl;
        }

        ++batchCount;
        if (batchCount >= BATCH_SIZE) {
            TransactionInterface::CommitTrxCommand();
            ThreadContextInterface::GetCurrentThreadContext()->ResetQueryMemory();
            TransactionInterface::StartTrxCommand();
            TransactionInterface::SetSnapShot();
            batchCount = 0;
        }
    }

    TransactionInterface::CommitTrxCommand();
    ThreadContextInterface::GetCurrentThreadContext()->ResetQueryMemory();
    delete handler;
}

void SysbenchStorage::LoadData()
{
    std::cout << "--------------------Start Load Data--------------------" << std::endl;
    auto tStart = std::chrono::system_clock::now();

    uint32_t threadNum = std::min(m_config.threadNum, m_config.tableSize);
    if (threadNum == 0) threadNum = 1;
    uint32_t rowsPerThread = m_config.tableSize / threadNum;
    uint32_t remainder     = m_config.tableSize % threadNum;

    for (uint32_t tIdx = 0; tIdx < m_config.tableNum; ++tIdx) {
        std::vector<std::thread> loaderThreads;
        uint32_t nextRow = 1;

        for (uint32_t tid = 0; tid < threadNum; ++tid) {
            uint32_t rows = rowsPerThread + (tid < remainder ? 1 : 0);
            uint32_t rowStart = nextRow;
            uint32_t rowEnd   = nextRow + rows - 1;
            nextRow = rowEnd + 1;

            loaderThreads.emplace_back([this, tIdx, rowStart, rowEnd] {
                CreateThreadAndRegister();
                StorageSession *sc = CreateStorageSession(1ULL);
                ThreadContextInterface *ctx = ThreadContextInterface::GetCurrentThreadContext();
                ctx->AttachSessionToThread(sc);
                LoadTableData(tIdx, rowStart, rowEnd);
                ctx->DetachSessionFromThread();
                UnregisterThread();
                CleanUpSession(sc);
            });
        }
        for (auto &t : loaderThreads) { t.join(); }
        std::cout << "Loaded table " << TableName(tIdx)
                  << " (" << m_config.tableSize << " rows)" << std::endl;
    }

    auto tEnd = std::chrono::system_clock::now();
    double elapsed = std::chrono::duration_cast<std::chrono::microseconds>(tEnd - tStart).count() / 1e6;
    std::cout << "Load Data finished in " << elapsed << "s" << std::endl;
    std::cout << "--------------------Finish Load Data--------------------" << std::endl
              << std::endl;
}

/* ----------------------------------------------------------------
 * RecoverTables - open existing tables/indexes into simulator map
 * ---------------------------------------------------------------- */
void SysbenchStorage::RecoverTables()
{
    std::cout << "--------------------Recover Tables--------------------" << std::endl;
    for (uint32_t i = 0; i < m_config.tableNum; ++i) {
        std::string tName = TableName(i);
        DstoreTableHandler heapHandler(g_instance);
        int ret = heapHandler.RecoveryTable(tName.c_str());
        if (ret == 1) {
            std::cout << "Recovery heap " << tName << " failed" << std::endl;
        } else {
            std::cout << "Recovery heap " << tName << " success" << std::endl;
        }

        std::string iName = PrimaryIndexName(i);
        DstoreTableHandler idxHandler(g_instance);
        ret = idxHandler.RecoveryTable(iName.c_str());
        if (ret == 1) {
            std::cout << "Recovery index " << iName << " failed" << std::endl;
        } else {
            std::cout << "Recovery index " << iName << " success" << std::endl;
        }
    }
    std::cout << "--------------------Recover Tables Done--------------------" << std::endl;
}

/* ----------------------------------------------------------------
 * Execute - run warmup then the measurement phase
 * ---------------------------------------------------------------- */
void SysbenchStorage::Execute()
{
    RecoverTables();
    auto runPhase = [&](uint32_t durationSec, bool measuring) {
        std::atomic<bool> stopFlag{false};
        std::vector<std::thread> workers;

        for (uint32_t tid = 0; tid < m_config.threadNum; ++tid) {
            workers.emplace_back([this, tid, &stopFlag, measuring] {
                CreateThreadAndRegister();
                StorageSession *sc = CreateStorageSession(1ULL);
                ThreadContextInterface *ctx = ThreadContextInterface::GetCurrentThreadContext();
                ctx->AttachSessionToThread(sc);

                /* Create per-thread handlers (same pattern as tpcc localTableHandlers) */
                std::vector<DstoreTableHandler *> threadHandlers;
                for (uint32_t i = 0; i < m_config.tableNum; ++i) {
                    threadHandlers.push_back(GetOrCreateTableHandler(i, true));
                }

                SysbenchWorker worker(tid, m_config, threadHandlers, m_stats);
                worker.Run(stopFlag, measuring);

                for (auto *h : threadHandlers) { delete h; }
                ClearTlsTableHandlers();
                ctx->DetachSessionFromThread();
                UnregisterThread();
                CleanUpSession(sc);
            });
        }

        /* Reporter thread */
        std::thread reporter;
        if (measuring && m_config.reportInterval > 0) {
            reporter = std::thread([&, durationSec] {
                uint32_t elapsed = 0;
                while (!stopFlag.load() && elapsed < durationSec) {
                    std::this_thread::sleep_for(
                        std::chrono::seconds(m_config.reportInterval));
                    elapsed += m_config.reportInterval;
                    if (elapsed <= durationSec) {
                        m_stats->PrintIntervalReport(elapsed);
                    }
                }
            });
        }

        std::this_thread::sleep_for(std::chrono::seconds(durationSec));
        stopFlag.store(true);

        for (auto &w : workers) { w.join(); }
        if (reporter.joinable()) { reporter.join(); }
    };

    /* Warmup phase */
    if (m_config.warmupSec > 0) {
        std::cout << "Warmup " << m_config.warmupSec << "s ..." << std::endl;
        runPhase(m_config.warmupSec, false);
        m_stats->Reset();
        std::cout << "Warmup done." << std::endl << std::endl;
    }

    /* Measurement phase */
    std::cout << "Running " << m_config.durationSec << "s benchmark ..." << std::endl;
    uint64_t t0 = GetMonotonicUs();
    runPhase(m_config.durationSec, true);
    uint64_t elapsed = GetMonotonicUs() - t0;

    m_stats->PrintFinalReport(elapsed);
}

/* ----------------------------------------------------------------
 * DropTables
 * ---------------------------------------------------------------- */
void SysbenchStorage::DropTables()
{
    std::cout << "--------------------Cleanup--------------------" << std::endl;
    /* Dstore does not expose a DropTable API via TableHandler in this codebase;
     * clearing the simulator context achieves the equivalent effect for tests. */
    if (simulator != nullptr) {
        simulator->Destory();
        simulator = nullptr;
    }
    std::cout << "Tables cleared." << std::endl;
}

/* ================================================================
 * SysbenchWorker implementation
 * ================================================================ */

SysbenchWorker::SysbenchWorker(uint32_t threadId, const SysbenchConfig &cfg,
                               std::vector<DstoreTableHandler *> &handlers,
                               SysbenchStats *stats)
    : m_threadId(threadId),
      m_cfg(cfg),
      m_handlers(handlers),
      m_stats(stats),
      m_rng(std::random_device{}()),
      m_tableDist(0, static_cast<int32_t>(cfg.tableNum) - 1),
      m_rowDist(1, static_cast<int32_t>(cfg.tableSize))
{
}

void SysbenchWorker::Run(std::atomic<bool> &stopFlag, bool isMeasuring)
{
    m_measuring = isMeasuring;
    while (!stopFlag.load(std::memory_order_relaxed)) {
        RunOltpTransaction();
    }
}

bool SysbenchWorker::RunOltpTransaction()
{
    uint64_t tStart = GetMonotonicUs();

    uint32_t tableIdx = RandTableIdx();
    int32_t  baseId   = RandId();

    uint64_t reads  = 0;
    uint64_t writes = 0;
    uint64_t others = 0;

    bool ok = true;

    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();

    /* Point selects */
    for (uint32_t i = 0; i < m_cfg.pointSelects && ok; ++i) {
        ok = DoPointSelect(tableIdx, RandId());
        ++reads;
    }

    /* Simple ranges */
    for (uint32_t i = 0; i < m_cfg.simpleRanges && ok; ++i) {
        int32_t id = RandId();
        ok = DoSimpleRange(tableIdx, id, id + static_cast<int32_t>(m_cfg.rangeSize) - 1);
        ++reads;
    }

    /* Sum ranges */
    for (uint32_t i = 0; i < m_cfg.sumRanges && ok; ++i) {
        int32_t id = RandId();
        ok = DoSumRange(tableIdx, id, id + static_cast<int32_t>(m_cfg.rangeSize) - 1);
        ++reads;
    }

    /* Order ranges */
    for (uint32_t i = 0; i < m_cfg.orderRanges && ok; ++i) {
        int32_t id = RandId();
        ok = DoOrderRange(tableIdx, id, id + static_cast<int32_t>(m_cfg.rangeSize) - 1);
        ++reads;
    }

    /* Distinct ranges */
    for (uint32_t i = 0; i < m_cfg.distinctRanges && ok; ++i) {
        int32_t id = RandId();
        ok = DoDistinctRange(tableIdx, id, id + static_cast<int32_t>(m_cfg.rangeSize) - 1);
        ++reads;
    }

    /* Index updates */
    for (uint32_t i = 0; i < m_cfg.indexUpdates && ok; ++i) {
        ok = DoIndexUpdate(tableIdx, RandId());
        ++writes;
    }

    /* Non-index updates */
    for (uint32_t i = 0; i < m_cfg.nonIndexUpdates && ok; ++i) {
        ok = DoNonIndexUpdate(tableIdx, RandId());
        ++writes;
    }

    /* Delete+Insert */
    for (uint32_t i = 0; i < m_cfg.deleteInserts && ok; ++i) {
        ok = DoDeleteInsert(tableIdx, baseId);
        others += 2;  /* 1 delete + 1 insert */
    }

    if (!m_cfg.skipTrx) {
        others += 2;  /* BEGIN + COMMIT/ROLLBACK */
    }
    if (ok) {
        TransactionInterface::CommitTrxCommand();
    } else {
        TransactionInterface::AbortTrx();
    }

    ThreadContextInterface::GetCurrentThreadContext()->ResetQueryMemory();

    uint64_t latUs = GetMonotonicUs() - tStart;
    if (m_measuring) {
        m_stats->RecordTransaction(m_threadId, latUs, ok, reads, writes, others);
    }
    return ok;
}

/* ---- helpers ---- */

uint32_t SysbenchWorker::RandTableIdx()
{
    if (m_cfg.tableNum == 1) return 0;
    return static_cast<uint32_t>(m_tableDist(m_rng));
}

int32_t SysbenchWorker::RandId()
{
    return m_rowDist(m_rng);
}

std::string SysbenchWorker::GenRandomC()
{
    char buf[SBTEST_C_LEN + 1];
    int pos = 0;
    for (int g = 0; g < 11; ++g) {
        if (g > 0) buf[pos++] = '-';
        int v = m_rng() % 1000;
        pos += sprintf_s(buf + pos, sizeof(buf) - pos, "%03d", v);
    }
    buf[pos] = '\0';
    return std::string(buf);
}

std::string SysbenchWorker::GenRandomPad()
{
    char buf[SBTEST_PAD_LEN + 1];
    int pos = 0;
    for (int g = 0; g < 5; ++g) {
        if (g > 0) buf[pos++] = '-';
        int v = m_rng() % 100;
        pos += sprintf_s(buf + pos, sizeof(buf) - pos, "%02d", v);
    }
    buf[pos] = '\0';
    return std::string(buf);
}

/* ----------------------------------------------------------------
 * DoPointSelect: SELECT c FROM sbtest WHERE id = ?
 * ---------------------------------------------------------------- */
bool SysbenchWorker::DoPointSelect(uint32_t tableIdx, int32_t id)
{
    DstoreTableHandler *handler = m_handlers[tableIdx];
    uint32_t colSeq[1] = {SBTEST_COL_ID};
    Datum    indexValues[1] = {Int32GetDatum(id)};
    HeapTuple *tuple = nullptr;

    int ret = handler->Scan(colSeq, indexValues, &tuple, 1);
    if (ret == 0 && tuple != nullptr) {
        DestroyObject((void **)&tuple);
    }
    return ret == 0;
}

/* ----------------------------------------------------------------
 * DoSimpleRange: SELECT id, k FROM sbtest WHERE id BETWEEN ? AND ?
 * Uses GetCount as a range count approximation since the current
 * TableHandler API's Scan does a prefix scan.
 * ---------------------------------------------------------------- */
bool SysbenchWorker::DoSimpleRange(uint32_t tableIdx, int32_t idFrom, int32_t idTo)
{
    DstoreTableHandler *handler = m_handlers[tableIdx];
    uint32_t colSeq[1] = {SBTEST_COL_ID};
    Datum    indexValues[1] = {Int32GetDatum(idFrom)};
    uint32_t cnt = 0;

    /* GetCount with single prefix key gives rows matching id=idFrom;
     * for range semantics we iterate from idFrom to idTo */
    __attribute__((__unused__)) int ret = 0;
    for (int32_t cur = idFrom; cur <= idTo; ++cur) {
        indexValues[0] = Int32GetDatum(cur);
        HeapTuple *tuple = nullptr;
        ret = handler->Scan(colSeq, indexValues, &tuple, 1);
        if (ret == 0 && tuple != nullptr) {
            DestroyObject((void **)&tuple);
            ++cnt;
        }
    }
    (void)cnt;
    return true;
}

/* ----------------------------------------------------------------
 * DoSumRange: SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?
 * ---------------------------------------------------------------- */
bool SysbenchWorker::DoSumRange(uint32_t tableIdx, int32_t idFrom, int32_t idTo)
{
    DstoreTableHandler *handler = m_handlers[tableIdx];
    uint32_t colSeq[1]   = {SBTEST_COL_ID};
    Datum    indexValues[1];
    double   sumK = 0.0;

    for (int32_t cur = idFrom; cur <= idTo; ++cur) {
        indexValues[0] = Int32GetDatum(cur);
        double partialSum = 0.0;
        /* Sum on heap column SBTEST_COL_K via index prefix on id */
        handler->Sum(colSeq, indexValues, SBTEST_COL_K, partialSum, 1);
        sumK += partialSum;
    }
    (void)sumK;
    return true;
}

/* ----------------------------------------------------------------
 * DoOrderRange: SELECT ... ORDER BY k (simulate: scan range, sort in memory)
 * ---------------------------------------------------------------- */
bool SysbenchWorker::DoOrderRange(uint32_t tableIdx, int32_t idFrom, int32_t idTo)
{
    DstoreTableHandler *handler = m_handlers[tableIdx];
    uint32_t colSeq[1] = {SBTEST_COL_ID};
    Datum    indexValues[1];
    std::vector<int32_t> kValues;

    for (int32_t cur = idFrom; cur <= idTo; ++cur) {
        indexValues[0] = Int32GetDatum(cur);
        HeapTuple *tuple = nullptr;
        int ret = handler->Scan(colSeq, indexValues, &tuple, 1);
        if (ret == 0 && tuple != nullptr) {
            Datum   vals[SBTEST_COL_MAX];
            bool    nulls[SBTEST_COL_MAX];
            TupleInterface::DeformHeapTuple(tuple, handler->m_heapRel->attr, vals, nulls);
            kValues.push_back(DatumGetInt32(vals[SBTEST_COL_K]));
            DestroyObject((void **)&tuple);
        }
    }
    std::sort(kValues.begin(), kValues.end());
    return true;
}

/* ----------------------------------------------------------------
 * DoDistinctRange: SELECT DISTINCT k FROM sbtest WHERE id BETWEEN ? AND ?
 * ---------------------------------------------------------------- */
bool SysbenchWorker::DoDistinctRange(uint32_t tableIdx, int32_t idFrom, int32_t idTo)
{
    DstoreTableHandler *handler = m_handlers[tableIdx];
    uint32_t colSeq[1] = {SBTEST_COL_ID};
    Datum    indexValues[1];
    std::set<int32_t> kSet;

    for (int32_t cur = idFrom; cur <= idTo; ++cur) {
        indexValues[0] = Int32GetDatum(cur);
        HeapTuple *tuple = nullptr;
        int ret = handler->Scan(colSeq, indexValues, &tuple, 1);
        if (ret == 0 && tuple != nullptr) {
            Datum   vals[SBTEST_COL_MAX];
            bool    nulls[SBTEST_COL_MAX];
            TupleInterface::DeformHeapTuple(tuple, handler->m_heapRel->attr, vals, nulls);
            kSet.insert(DatumGetInt32(vals[SBTEST_COL_K]));
            DestroyObject((void **)&tuple);
        }
    }
    return true;
}

/* ----------------------------------------------------------------
 * DoIndexUpdate: UPDATE sbtest SET k = k + 1 WHERE id = ?
 * ---------------------------------------------------------------- */
bool SysbenchWorker::DoIndexUpdate(uint32_t tableIdx, int32_t id)
{
    DstoreTableHandler *handler = m_handlers[tableIdx];
    uint32_t colSeq[1]   = {SBTEST_COL_ID};
    Datum    indexValues[1] = {Int32GetDatum(id)};

    HeapTuple *tuple = nullptr;
    int ret = handler->LockTuple(colSeq, indexValues, &tuple, 1);
    if (ret != 0 || tuple == nullptr) { return false; }

    /* Deform full tuple, modify k, update with full values + primary key colIndex */
    Datum vals[SBTEST_COL_MAX];
    bool  nulls[SBTEST_COL_MAX];
    TupleInterface::DeformHeapTuple(tuple, handler->m_heapRel->attr, vals, nulls);

    int32_t newK = DatumGetInt32(vals[SBTEST_COL_K]) + 1;
    vals[SBTEST_COL_K] = Int32GetDatum(newK);
    nulls[SBTEST_COL_K] = false;

    ItemPointerData newCtid = *tuple->GetCtid();
    /* colIndex = primary key column indices; values = full tuple */
    uint32_t pkCols[1] = {SBTEST_COL_ID};
    ret = handler->Update(&newCtid, pkCols, vals, nulls);
    DestroyObject((void **)&tuple);
    return ret == 0;
}

/* ----------------------------------------------------------------
 * DoNonIndexUpdate: UPDATE sbtest SET c = ? WHERE id = ?
 * ---------------------------------------------------------------- */
bool SysbenchWorker::DoNonIndexUpdate(uint32_t tableIdx, int32_t id)
{
    DstoreTableHandler *handler = m_handlers[tableIdx];
    uint32_t colSeq[1]   = {SBTEST_COL_ID};
    Datum    indexValues[1] = {Int32GetDatum(id)};

    HeapTuple *tuple = nullptr;
    int ret = handler->LockTuple(colSeq, indexValues, &tuple, 1);
    if (ret != 0 || tuple == nullptr) { return false; }

    /* Deform full tuple, modify c, update with full values + primary key colIndex */
    Datum vals[SBTEST_COL_MAX];
    bool  nulls[SBTEST_COL_MAX];
    TupleInterface::DeformHeapTuple(tuple, handler->m_heapRel->attr, vals, nulls);

    std::string newC = GenRandomC();
    uint64_t cLen = newC.size();
    text *cText = (text *)malloc(VARHDRSZ + cLen + 1);
    DstoreSetVarSize(cText, VARHDRSZ + cLen + 1);
    (void)memcpy_s(cText->vl_dat, cLen + 1, newC.c_str(), cLen + 1);

    vals[SBTEST_COL_C] = PointerGetDatum(cText);
    nulls[SBTEST_COL_C] = false;

    ItemPointerData newCtid = *tuple->GetCtid();
    uint32_t pkCols[1] = {SBTEST_COL_ID};
    ret = handler->Update(&newCtid, pkCols, vals, nulls);
    free(cText);
    DestroyObject((void **)&tuple);
    return ret == 0;
}

/* ----------------------------------------------------------------
 * DoDeleteInsert: DELETE FROM sbtest WHERE id = ?
 *                 INSERT INTO sbtest VALUES (id, rand_k, rand_c, rand_pad)
 * ---------------------------------------------------------------- */
bool SysbenchWorker::DoDeleteInsert(uint32_t tableIdx, int32_t id)
{
    DstoreTableHandler *handler = m_handlers[tableIdx];
    uint32_t colSeq[1] = {SBTEST_COL_ID};
    Datum    indexValues[1] = {Int32GetDatum(id)};

    int ret = handler->Delete(colSeq, indexValues, 1);
    if (ret != 0) { return false; }

    std::string newC   = GenRandomC();
    std::string newPad = GenRandomPad();
    int32_t newK = static_cast<int32_t>(m_rng() % m_cfg.tableSize);

    Datum vals[SBTEST_COL_MAX];
    bool  nulls[SBTEST_COL_MAX] = {false};
    uint64_t cLen2 = newC.size();
    text *cText2 = (text *)malloc(VARHDRSZ + cLen2 + 1);
    DstoreSetVarSize(cText2, VARHDRSZ + cLen2 + 1);
    (void)memcpy_s(cText2->vl_dat, cLen2 + 1, newC.c_str(), cLen2 + 1);

    uint64_t padLen2 = newPad.size();
    text *padText2 = (text *)malloc(VARHDRSZ + padLen2 + 1);
    DstoreSetVarSize(padText2, VARHDRSZ + padLen2 + 1);
    (void)memcpy_s(padText2->vl_dat, padLen2 + 1, newPad.c_str(), padLen2 + 1);

    vals[SBTEST_COL_ID]  = Int32GetDatum(id);
    vals[SBTEST_COL_K]   = Int32GetDatum(newK);
    vals[SBTEST_COL_C]   = PointerGetDatum(cText2);
    vals[SBTEST_COL_PAD] = PointerGetDatum(padText2);

    /* colIndex = primary key columns so Insert also updates the index */
    uint32_t pkCols[1] = {SBTEST_COL_ID};
    ret = handler->Insert(vals, nulls, pkCols);
    free(cText2);
    free(padText2);
    return ret == 0;
}

} /* namespace SYSBENCH */

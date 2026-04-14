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

#ifndef STRESS_VERIFY_CLIENT_H
#define STRESS_VERIFY_CLIENT_H

#include <cstdint>
#include <vector>
#include <string>
#include <atomic>
#include <random>
#include "stress_verify_common.h"
#include "stress_verify_stats.h"
#include "fault_injector.h"

class DstoreTableHandler;

namespace DSTORE {
namespace STRESS_VERIFY {

/*
 * StressVerifyStorage
 *
 * Top-level orchestrator that owns the table handlers and drives
 * Prepare / Run / Cleanup phases with page verification integration.
 */
class StressVerifyStorage {
public:
    StressVerifyStorage() = default;
    ~StressVerifyStorage();

    StressVerifyStorage(const StressVerifyStorage &) = delete;
    StressVerifyStorage &operator=(const StressVerifyStorage &) = delete;

    void Init(int32_t nodeId);

    /* Prepare phase */
    void CreateTables(uint32_t *allocedMaxRelOid);
    void CreateIndexes(uint32_t *allocedMaxRelOid);
    void LoadData();

    /* Run phase */
    void RecoverTables();
    void Execute();

    /* Cleanup phase */
    void DropTables();

    StressVerifyConfig GetConfig() const { return m_config; }
    VerifyStats* GetStats() const { return m_stats; }

private:
    void LoadConfig(const std::string &configPath);
    DstoreTableHandler *GetOrCreateTableHandler(uint32_t tableIdx, bool withIndex);
    void ClearTableHandlers();

    void LoadTableData(uint32_t tableIdx, uint32_t rowStart, uint32_t rowEnd);

    StressVerifyConfig              m_config;
    VerifyStats                    *m_stats      = nullptr;
    FaultInjector                  *m_injector   = nullptr;
    int32_t                         m_nodeId     = 1;
    std::vector<DstoreTableHandler *> m_tableHandlers;
    std::vector<DstoreTableHandler *> m_heapOnlyHandlers;
};

/*
 * StressVerifyWorker
 *
 * Runs in its own thread; executes OLTP transactions with page verification.
 * Calls VerifyPageOnWrite/OnRead APIs and records verification results.
 */
class StressVerifyWorker {
public:
    StressVerifyWorker(uint32_t threadId, const StressVerifyConfig &cfg,
                       std::vector<DstoreTableHandler *> &handlers,
                       VerifyStats *stats, FaultInjector *injector);

    void Run(std::atomic<bool> &stopFlag, bool isMeasuring);

private:
    bool RunOltpTransaction();

    /* Sub-operations; return false on error */
    bool DoPointSelect(uint32_t tableIdx, int32_t id);
    bool DoSimpleRange(uint32_t tableIdx, int32_t idFrom, int32_t idTo);
    bool DoSumRange(uint32_t tableIdx, int32_t idFrom, int32_t idTo);
    bool DoOrderRange(uint32_t tableIdx, int32_t idFrom, int32_t idTo);
    bool DoDistinctRange(uint32_t tableIdx, int32_t idFrom, int32_t idTo);
    bool DoIndexUpdate(uint32_t tableIdx, int32_t id);
    bool DoNonIndexUpdate(uint32_t tableIdx, int32_t id);
    bool DoDeleteInsert(uint32_t tableIdx, int32_t id);

    /* Verification helpers */
    void VerifyAfterRead(const char *opName, bool success);
    void VerifyAfterWrite(const char *opName, bool success);

    uint32_t RandTableIdx();
    int32_t  RandId();
    std::string GenRandomC();
    std::string GenRandomPad();

    uint32_t                            m_threadId;
    StressVerifyConfig                  m_cfg;
    std::vector<DstoreTableHandler *>  &m_handlers;
    VerifyStats                        *m_stats;
    FaultInjector                      *m_injector;
    bool                                m_measuring = false;
    std::mt19937                        m_rng;
    std::uniform_int_distribution<int32_t> m_tableDist;
    std::uniform_int_distribution<int32_t> m_rowDist;
};

} /* namespace STRESS_VERIFY */
} /* namespace DSTORE */

#endif /* STRESS_VERIFY_CLIENT_H */
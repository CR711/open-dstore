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

#ifndef SYSBENCH_CLIENT_H
#define SYSBENCH_CLIENT_H

#include <cstdint>
#include <vector>
#include <string>
#include <atomic>
#include <random>
#include "sysbench_common.h"
#include "sysbench_stats.h"

class DstoreTableHandler;

namespace SYSBENCH {

/*
 * SysbenchStorage
 *
 * Top-level orchestrator that owns the table handlers and drives
 * Prepare / Run / Cleanup phases.
 */
class SysbenchStorage {
public:
    SysbenchStorage() = default;
    ~SysbenchStorage();

    SysbenchStorage(const SysbenchStorage &) = delete;
    SysbenchStorage &operator=(const SysbenchStorage &) = delete;

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

    SysbenchConfig GetConfig() const { return m_config; }

private:
    void LoadConfig(const std::string &configPath);
    DstoreTableHandler *GetOrCreateTableHandler(uint32_t tableIdx, bool withIndex);
    void ClearTableHandlers();

    void LoadTableData(uint32_t tableIdx, uint32_t rowStart, uint32_t rowEnd);

    SysbenchConfig              m_config;
    SysbenchStats              *m_stats   = nullptr;
    int32_t                     m_nodeId  = 1;
    std::vector<DstoreTableHandler *> m_tableHandlers;  /* one per table, heap+index */
    std::vector<DstoreTableHandler *> m_heapOnlyHandlers; /* heap only, for load phase */
};

/*
 * SysbenchWorker
 *
 * Runs in its own thread; executes OLTP transactions until stopFlag is set.
 */
class SysbenchWorker {
public:
    SysbenchWorker(uint32_t threadId, const SysbenchConfig &cfg,
                   std::vector<DstoreTableHandler *> &handlers,
                   SysbenchStats *stats);

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

    uint32_t RandTableIdx();
    int32_t  RandId();
    std::string GenRandomC();
    std::string GenRandomPad();

    uint32_t                            m_threadId;
    SysbenchConfig                      m_cfg;
    std::vector<DstoreTableHandler *>  &m_handlers;
    SysbenchStats                      *m_stats;
    bool                                m_measuring = false;
    std::mt19937                        m_rng;
    std::uniform_int_distribution<int32_t> m_tableDist;
    std::uniform_int_distribution<int32_t> m_rowDist;
};

} /* namespace SYSBENCH */

#endif /* SYSBENCH_CLIENT_H */

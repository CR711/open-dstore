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

#ifndef SYSBENCH_COMMON_H
#define SYSBENCH_COMMON_H

#include <cstdint>
#include "catalog/dstore_fake_type.h"
#include "table_data_generator.h"

namespace SYSBENCH {

/* sbtest table column indices */
enum SbtestColType : uint8_t {
    SBTEST_COL_ID = 0,
    SBTEST_COL_K,
    SBTEST_COL_C,
    SBTEST_COL_PAD,
    SBTEST_COL_MAX
};

static __attribute__((__unused__)) ColDef SBTEST_COL_DESC[SBTEST_COL_MAX] = {
    {  INT4OID, false,   0, 0, 0},   /* id  - primary key */
    {  INT4OID,  true,   0, 0, 0},   /* k   - indexed column */
    {VARCHAROID,  true, 120, 0, 0},  /* c   - random string */
    {VARCHAROID,  true,  60, 0, 0},  /* pad - padding string */
};

/* Primary index on id (unique) */
static __attribute__((__unused__)) IndexDesc SBTEST_PRIMARY_INDEX_DESC[] = {
    {1, {SBTEST_COL_ID, 0, 0, 0}, true},
};

/* Secondary index on k (non-unique) */
static __attribute__((__unused__)) IndexDesc SBTEST_SECONDARY_INDEX_DESC[] = {
    {1, {SBTEST_COL_K, 0, 0, 0}, false},
};

constexpr int SBTEST_C_LEN   = 120;
constexpr int SBTEST_PAD_LEN = 60;

enum CommandType : uint8_t {
    CMD_PREPARE = 0,
    CMD_RUN,
    CMD_CLEANUP,
    CMD_ALL
};

enum ModeType : uint8_t {
    MODE_READ_WRITE = 0,  /* oltp_read_write: reads + writes (default) */
    MODE_READ_ONLY,       /* oltp_read_only:  all writes disabled */
    MODE_WRITE_ONLY       /* oltp_write_only: all reads disabled */
};

struct SysbenchConfig {
    uint32_t    tableNum          = 1;
    uint32_t    tableSize         = 10000;
    uint32_t    threadNum         = 8;
    uint32_t    durationSec       = 60;
    uint32_t    warmupSec         = 10;
    uint32_t    reportInterval    = 10;
    uint32_t    rangeSize         = 100;

    /* per-transaction operation counts */
    uint32_t    pointSelects      = 10;
    uint32_t    simpleRanges      = 1;
    uint32_t    sumRanges         = 1;
    uint32_t    orderRanges       = 1;
    uint32_t    distinctRanges    = 1;
    uint32_t    indexUpdates      = 1;
    uint32_t    nonIndexUpdates   = 1;
    uint32_t    deleteInserts     = 1;

    bool        skipTrx           = false;
    bool        secondaryIndex    = false;
    bool        autoInc           = true;

    ModeType    mode              = MODE_READ_WRITE;
    CommandType command           = CMD_RUN;
};

} /* namespace SYSBENCH */

#endif /* SYSBENCH_COMMON_H */

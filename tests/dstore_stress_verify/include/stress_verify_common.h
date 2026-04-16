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

#ifndef STRESS_VERIFY_COMMON_H
#define STRESS_VERIFY_COMMON_H

#include <cstdint>
#include <cstring>
#include <string>
#include "cjson/cJSON.h"

namespace DSTORE {
namespace STRESS_VERIFY {

/* Safe cJSON accessors to avoid null-pointer dereferences (C3/C4) */
static inline int GetJsonInt(cJSON *json, const char *key, int defaultVal)
{
    cJSON *item = cJSON_GetObjectItem(json, key);
    return (item != nullptr) ? item->valueint : defaultVal;
}

static inline const char *GetJsonStr(cJSON *json, const char *key, const char *defaultVal)
{
    cJSON *item = cJSON_GetObjectItem(json, key);
    return (item != nullptr && item->valuestring != nullptr) ? item->valuestring : defaultVal;
}

/* 命令类型 */
enum CommandType : uint8_t {
    CMD_PREPARE = 0,
    CMD_RUN,
    CMD_CLEANUP,
    CMD_ALL
};

/* 运行模式 */
enum RunMode : uint8_t {
    MODE_READ_WRITE = 0,
    MODE_READ_ONLY,
    MODE_WRITE_ONLY
};

/* 故障类型 */
enum FaultType : uint16_t {
    FAULT_NONE = 0,

    /* 通用页头类 */
    FAULT_CRC_ERROR = 0x0100,
    FAULT_PAGE_TYPE_INVALID = 0x0101,
    FAULT_PAGE_ID_MISMATCH = 0x0102,
    FAULT_BOUNDARY_ERROR = 0x0103,

    /* Heap 结构类 */
    FAULT_HEAP_TD_COUNT_ERROR = 0x0200,
    FAULT_HEAP_ITEMID_ALIGN_ERROR = 0x0201,
    FAULT_HEAP_TUPLE_OVERLAP = 0x0202,
    FAULT_HEAP_TUPLE_SIZE_ERROR = 0x0203,

    /* Index 结构类 */
    FAULT_INDEX_PAGE_TYPE_ERROR = 0x0300,
    FAULT_INDEX_META_PAGE_ERROR = 0x0301,
    FAULT_INDEX_QUEUE_ERROR = 0x0302,

    FAULT_MAX
};

/* 目标模块 */
enum FaultTargetModule : uint8_t {
    TARGET_HEAP = 0,
    TARGET_INDEX,
    TARGET_UNDO,
    TARGET_SEGMENT,
    TARGET_FSM
};

/* 校验级别（与 DFX 定义一致） */
enum VerifyLevel : uint8_t {
    VERIFY_NONE = 0,
    VERIFY_LIGHT = 1,
    VERIFY_MEDIUM = 2,
    VERIFY_HEAVY = 3
};

/* 严重级别 */
enum VerifySeverity : uint8_t {
    SEVERITY_INFO = 0,
    SEVERITY_WARNING = 1,
    SEVERITY_ERROR = 2,
    SEVERITY_FATAL = 3
};

/* 错误码（与 DFX 定义一致） */
enum VerifyCode : uint32_t {
    VERIFY_OK = 0,

    /* 通用 (0x0001-0x000F) */
    PAGE_TYPE_INVALID           = 0x0001,
    PAGE_ID_INVALID             = 0x0002,
    PAGE_ID_MISMATCH            = 0x0003,
    PAGE_CRC_MISMATCH           = 0x0004,
    PAGE_BOUNDARY_INVALID       = 0x0005,
    PAGE_MAGIC_MISMATCH         = 0x0006,
    PAGE_NULL                   = 0x000A,

    /* Heap (0x0100-0x012F) */
    HEAP_SPECIAL_OFFSET_MISMATCH    = 0x0110,
    HEAP_HEADER_OFFSET_INVALID      = 0x0111,
    HEAP_TD_COUNT_OVERFLOW          = 0x0112,
    HEAP_ITEMID_ALIGNMENT_INVALID   = 0x0113,
    HEAP_FSM_SLOT_INVALID           = 0x0114,
    HEAP_TUPLE_OVERLAP              = 0x0115,
    HEAP_TD_SANITY_FAIL             = 0x0116,
    HEAP_TUPLE_HEADER_SIZE_INVALID  = 0x0117,
    HEAP_TUPLE_NUM_COLUMN_INVALID   = 0x0118,
    HEAP_TUPLE_SIZE_MISMATCH        = 0x011A,
    HEAP_TUPLE_FLAG_INCONSISTENT    = 0x011D,

    /* Index (0x0200-0x022F) */
    BTR_PAGE_TYPE_INVALID       = 0x0200,
    BTR_SPLIT_STAT_INVALID      = 0x0201,
    BTR_SPECIAL_OFFSET_INVALID  = 0x0203,
    BTR_META_PAGE_ID_INVALID    = 0x0204,
    INDEX_TUPLE_SIZE_MISMATCH   = 0x0210,
    BTR_QUEUE_INCONSISTENT      = 0x0223,

    /* Undo (0x0300-0x031F) */
    UNDO_SLOT_STATE_INVALID     = 0x0300,
    UNDO_SLOT_XID_INVALID       = 0x0301,
    UNDO_REC_TYPE_INVALID       = 0x0310,
    UNDO_SEG_FIRST_PAGE_INVALID = 0x0320,

    /* Segment (0x0400-0x041F) */
    SEG_MAGIC_MISMATCH              = 0x0400,
    SEG_EXT_SIZE_INVALID            = 0x0401,
    SEG_SEGMENT_TYPE_INVALID        = 0x0402,
    BITMAP_META_EXTENT_SIZE_INVALID = 0x0410,
    BITMAP_ALLOCATED_CNT_MISMATCH   = 0x0415,
    FILE_BLOCK_ID_INVALID           = 0x0420,
    SPACE_PAGE_VERSION_INVALID      = 0x0430
};

/* 配置结构体 */
struct StressVerifyConfig {
    /* 基础配置 */
    uint32_t tables;
    uint32_t tableSize;
    uint32_t threads;
    uint32_t time;
    uint32_t warmupTime;
    uint32_t reportInterval;
    uint32_t rangeSize;

    /* 操作次数 */
    uint32_t pointSelects;
    uint32_t simpleRanges;
    uint32_t sumRanges;
    uint32_t orderRanges;
    uint32_t distinctRanges;
    uint32_t indexUpdates;
    uint32_t nonIndexUpdates;
    uint32_t deleteInserts;

    uint32_t skipTrx;
    uint32_t secondary;
    uint32_t autoInc;
    RunMode  mode;
    CommandType command;

    /* 页面校验配置 */
    VerifyLevel verifyLevel;
    uint64_t    verifyModules;   /* bitset: bit0=heap, bit1=index, ... */
    bool        verifyOnWrite;
    bool        verifyOnRead;

    /* 故障注入配置 */
    bool              faultInjectEnabled;
    uint32_t          faultInjectTime;
    FaultType         faultType;
    FaultTargetModule faultTargetModule;
    uint32_t          faultTargetPage;  /* 0 = 随机 */

    StressVerifyConfig()
        : tables(1), tableSize(10000), threads(8), time(60),
          warmupTime(10), reportInterval(10), rangeSize(100),
          pointSelects(10), simpleRanges(1), sumRanges(1),
          orderRanges(1), distinctRanges(1), indexUpdates(1),
          nonIndexUpdates(1), deleteInserts(1),
          skipTrx(0), secondary(0), autoInc(1),
          mode(MODE_READ_WRITE), command(CMD_ALL),
          verifyLevel(VERIFY_LIGHT), verifyModules(0x07),  /* heap+index+undo */
          verifyOnWrite(true), verifyOnRead(true),
          faultInjectEnabled(false), faultInjectTime(30),
          faultType(FAULT_NONE), faultTargetModule(TARGET_HEAP),
          faultTargetPage(0) {}
};

/* 故障类型名称转换 */
inline const char* FaultTypeToString(FaultType type) {
    switch (type) {
        case FAULT_CRC_ERROR:           return "crc_error";
        case FAULT_PAGE_TYPE_INVALID:   return "page_type_invalid";
        case FAULT_PAGE_ID_MISMATCH:    return "page_id_mismatch";
        case FAULT_BOUNDARY_ERROR:      return "boundary_error";
        case FAULT_HEAP_TD_COUNT_ERROR:     return "heap_td_count_error";
        case FAULT_HEAP_ITEMID_ALIGN_ERROR: return "heap_itemid_align_error";
        case FAULT_HEAP_TUPLE_OVERLAP:      return "heap_tuple_overlap";
        case FAULT_HEAP_TUPLE_SIZE_ERROR:   return "heap_tuple_size_error";
        case FAULT_INDEX_PAGE_TYPE_ERROR:   return "index_page_type_error";
        case FAULT_INDEX_META_PAGE_ERROR:   return "index_meta_page_error";
        case FAULT_INDEX_QUEUE_ERROR:       return "index_queue_error";
        default: return "unknown";
    }
}

/* 模块名称转换 */
inline const char* ModuleToString(FaultTargetModule mod) {
    switch (mod) {
        case TARGET_HEAP:    return "HEAP";
        case TARGET_INDEX:   return "INDEX";
        case TARGET_UNDO:    return "UNDO";
        case TARGET_SEGMENT: return "SEGMENT";
        case TARGET_FSM:     return "FSM";
        default: return "UNKNOWN";
    }
}

} /* namespace STRESS_VERIFY */
} /* namespace DSTORE */

#endif /* STRESS_VERIFY_COMMON_H */
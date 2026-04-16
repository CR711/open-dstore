#ifndef DSTORE_VERIFY_REPORT_H
#define DSTORE_VERIFY_REPORT_H

#include <string>
#include <vector>
#include "common/dstore_datatype.h"
#include "common/dstore_common_utils.h"
#include "page/dstore_page_struct.h"

namespace DSTORE {

/* 校验严重级别 */
enum class VerifySeverity : uint8 {
    SEVERITY_INFO = 0,
    SEVERITY_WARNING,
    SEVERITY_ERROR,
    SEVERITY_FATAL
};

/* 校验级别 */
enum class VerifyLevel : uint8 {
    NONE = 0,
    LIGHT,
    MEDIUM,
    HEAVY
};

/* 校验模块（bitset 位掩码） */
enum class VerifyModule : uint8 {
    HEAP = 0,
    INDEX,
    UNDO,
    SEGMENT,
    FSM,
    ALL    /* 兼容：启用所有模块 */
};

/* 错误码定义（按模块分段） */
enum class VerifyCode : uint32 {
    OK = 0,

    /* 通用错误码 (0x0001-0x000F) */
    PAGE_TYPE_INVALID           = 0x0001,
    PAGE_ID_INVALID             = 0x0002,
    PAGE_ID_MISMATCH            = 0x0003,
    PAGE_CRC_MISMATCH           = 0x0004,
    PAGE_BOUNDARY_INVALID       = 0x0005,
    PAGE_MAGIC_MISMATCH         = 0x0006,
    PAGE_NULL                   = 0x000A,

    /* Heap 模块 (0x0100-0x012F) */
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

    /* Index 模块 (0x0200-0x022F) */
    BTR_PAGE_TYPE_INVALID       = 0x0200,
    BTR_SPLIT_STAT_INVALID      = 0x0201,
    BTR_SPECIAL_OFFSET_INVALID  = 0x0203,
    BTR_META_PAGE_ID_INVALID    = 0x0204,
    INDEX_TUPLE_SIZE_MISMATCH   = 0x0210,
    INDEX_ITEMID_OFFSET_MISALIGNED = 0x0211,
    BTR_QUEUE_INCONSISTENT      = 0x0223,

    /* Btree 跨页校验 (0x0230-0x024F) */
    BTR_SIBLING_LINK_BROKEN         = 0x0230,
    BTR_LEVEL_INCONSISTENT          = 0x0231,
    BTR_KEY_ORDER_VIOLATION         = 0x0232,
    BTR_HIGHKEY_VIOLATION           = 0x0233,
    BTR_PARENT_CHILD_MISMATCH       = 0x0234,
    BTR_SIBLING_LINK_CYCLE          = 0x0235,
    BTR_META_ROOT_INVALID           = 0x0236,
    BTR_PAGE_READ_FAILED            = 0x0237,

    /* Index-Heap 一致性 (0x0250-0x025F) */
    INDEX_HEAP_POINTER_INVALID      = 0x0250,
    INDEX_HEAP_TUPLE_MISSING        = 0x0251,
    INDEX_HEAP_DATA_MISMATCH        = 0x0252,

    /* Undo 模块 (0x0300-0x031F) */
    UNDO_SLOT_STATE_INVALID     = 0x0300,
    UNDO_SLOT_XID_INVALID       = 0x0301,
    UNDO_REC_TYPE_INVALID       = 0x0310,
    UNDO_SEG_FIRST_PAGE_INVALID = 0x0320,

    /* Heap 跨页校验 (0x0130-0x014F) */
    HEAP_PAGE_READ_FAILED           = 0x0130,
    HEAP_TUPLE_FORMAT_INVALID       = 0x0131,
    HEAP_BIG_TUPLE_CHAIN_BROKEN     = 0x0132,
    HEAP_BIG_TUPLE_CHAIN_CYCLE      = 0x0133,
    HEAP_FSM_INCONSISTENT           = 0x0134,

    /* Segment 模块 (0x0400-0x041F) */
    SEG_MAGIC_MISMATCH              = 0x0400,
    SEG_EXT_SIZE_INVALID            = 0x0401,
    SEG_SEGMENT_TYPE_INVALID        = 0x0402,
    SEG_BLOCK_COUNT_MISMATCH        = 0x0403,
    SEG_EXTENT_COUNT_MISMATCH       = 0x0404,
    SEG_DATA_RANGE_MISMATCH         = 0x0405,
    SEG_EXTENT_CHAIN_CYCLE          = 0x0406,
    SEG_EXTENT_CHAIN_BROKEN         = 0x0407,
    SEG_EXTENT_OVERLAP              = 0x0408,
    SEG_PAGE_READ_FAILED            = 0x0409,
    SEG_LEAF_COUNT_MISMATCH         = 0x040A,
    BITMAP_META_EXTENT_SIZE_INVALID = 0x0410,
    BITMAP_ALLOCATED_CNT_MISMATCH   = 0x0415,
    BITMAP_NOT_SET                  = 0x0416,
    FILE_BLOCK_ID_INVALID           = 0x0420,
    SPACE_PAGE_VERSION_INVALID      = 0x0430,

    /* Metadata 一致性 (0x0500-0x050F) */
    METADATA_SEGMENT_MISSING        = 0x0500,
    METADATA_SEGMENT_TYPE_MISMATCH  = 0x0501,
    METADATA_TABLESPACE_MISMATCH    = 0x0502,
    METADATA_INDEX_META_MISSING     = 0x0503,
    METADATA_INDEX_ATTR_MISMATCH    = 0x0504,
    METADATA_SEGMENT_TYPE_INVALID   = 0x0505
};

struct VerifyResult {
    VerifySeverity severity{VerifySeverity::SEVERITY_INFO};
    VerifyCode code{VerifyCode::OK};
    const char *targetType{nullptr};
    PageId targetId{INVALID_PAGE_ID};
    const char *checkName{nullptr};
    uint64 expected{0};
    uint64 actual{0};
    char message[256]{0};
};

/* Thread Safety: NOT thread-safe. Each session must use its own instance. */
class VerifyReport {
public:
    VerifyReport();
    ~VerifyReport() = default;

    DISALLOW_COPY_AND_MOVE(VerifyReport);

    void AddResult(const VerifyResult &result);
    void AddResult(VerifySeverity severity, const char *targetType, const PageId &targetId, const char *checkName,
        uint64 expected, uint64 actual, const char *format, ...);
    void AddResultWithCode(VerifySeverity severity, VerifyCode code, const char *targetType, const PageId &targetId,
        const char *checkName, uint64 expected, uint64 actual, const char *format, ...);

    bool HasError() const;
    bool HasFatal() const;
    uint64 GetErrorCount() const;
    uint64 GetWarningCount() const;
    uint64 GetFatalCount() const;
    uint64 GetTotalChecks() const;
    RetStatus GetRetStatus() const;

    std::string FormatText() const;
    std::string FormatJson() const;

    const std::vector<VerifyResult> &GetResults() const;

private:
    static const char *SeverityToStr(VerifySeverity severity);
    static std::string EscapeJson(const char *input);

    std::vector<VerifyResult> m_results;
    uint64 m_totalChecks{0};
    uint64 m_passedChecks{0};
    uint64 m_failedChecks{0};
    uint64 m_warningCount{0};
    uint64 m_fatalCount{0};
    TimestampTz m_startTime{0};
    TimestampTz m_endTime{0};
};

}  // namespace DSTORE

#endif

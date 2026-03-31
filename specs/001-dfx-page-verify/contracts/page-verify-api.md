# Contract: Page Verification API

## 1. Single Page Verification Interface

### 1.1 Registry API

```cpp
namespace DSTORE {

// 校验函数签名
using PageVerifyFunc = RetStatus (*)(const Page* page, VerifyLevel level, VerifyReport* report);

// 注册单个 PageType 的校验函数
RetStatus RegisterPageVerifier(PageType type,
                               const char* typeName,
                               PageVerifyFunc lightweightFunc,
                               PageVerifyFunc heavyweightFunc);

// 初始化所有内置页面类型的校验函数（各模块调用 RegisterPageVerifier）
void InitPageVerifiers();

}  // namespace DSTORE
```

### 1.2 Inline Verification (Write Path)

```cpp
namespace DSTORE {

// 在写入路径调用，根据 GUC dfx_verify_level 自动分发
// 返回 DSTORE_SUCC 表示校验通过或未开启校验
// 返回 DSTORE_FAIL 表示校验失败，调用方决定是否中断
RetStatus VerifyPageInline(const Page* page);

// 带 report 的版本（用于需要收集诊断信息的场景）
RetStatus VerifyPageInlineWithReport(const Page* page, VerifyReport* report);

}  // namespace DSTORE
```

### 1.3 On-Demand Verification

```cpp
namespace DSTORE {

// 指定 level 的单页面校验
RetStatus VerifyPage(const Page* page, VerifyLevel level, VerifyReport* report);

// 校验指定 PageId 的页面（自动读取 buffer）
RetStatus VerifyPageById(const PageId& pageId, VerifyLevel level, VerifyReport* report);

}  // namespace DSTORE
```

## 2. Cross-Page Verification Interface

### 2.1 B-tree Index Verification

```cpp
namespace DSTORE {

struct BtreeVerifyOptions {
    bool checkStructure = true;        // 结构完整性（sibling links, levels, key ordering）
    bool checkHeapConsistency = true;  // index-heap 1:1 对应
    bool checkDataConsistency = false; // index-heap 数据值一致性
    float sampleRatio = 1.0f;         // 采样比例 (0.0~1.0)
    bool isOnline = true;             // 是否在线模式（考虑可见性）
    uint32 maxErrors = 1000;           // 最大错误数
};

// 校验单个 btree index
RetStatus VerifyBtreeIndex(StorageRelationData* indexRel,
                           StorageRelationData* heapRel,
                           const BtreeVerifyOptions& options,
                           VerifyReport* report);

}  // namespace DSTORE
```

### 2.2 Heap Segment Verification

```cpp
namespace DSTORE {

struct HeapVerifyOptions {
    bool checkTupleFormat = true;      // tuple 格式校验
    bool checkBigTupleChains = true;   // linked tuple chunk 完整性
    bool checkFsmConsistency = true;   // FSM-heap 一致性
    bool isOnline = true;
    uint32 maxErrors = 1000;
};

// 校验 heap segment
RetStatus VerifyHeapSegment(StorageRelationData* heapRel,
                            const HeapVerifyOptions& options,
                            VerifyReport* report);

}  // namespace DSTORE
```

### 2.3 Segment Metadata Verification

```cpp
namespace DSTORE {

struct SegmentVerifyOptions {
    bool checkExtentChain = true;      // extent chain 完整性
    bool checkExtentBitmap = true;     // extent-bitmap 一致性
    bool checkPageCounts = true;       // page count 一致性
    uint32 maxErrors = 1000;
};

// 校验 segment 元数据和 extent 分配
RetStatus VerifySegment(const SegmentId& segmentId,
                        const SegmentVerifyOptions& options,
                        VerifyReport* report);

}  // namespace DSTORE
```

### 2.4 InnoDB Metadata Consistency Verification

```cpp
namespace DSTORE {

struct MetadataInputStruct {
    Oid tableOid;
    SegmentId heapSegmentId;
    SegmentId lobSegmentId;           // INVALID_SEGMENT_ID if none
    uint16 indexCount;
    IndexMetaEntry* indexes;          // array of indexCount entries
    uint32 tablespaceId;
    uint32 ownerTablespaceId;
    uint16 heapRowFormat;
    uint16 indexRowFormat;
};

struct IndexMetaEntry {
    Oid indexOid;
    SegmentId indexSegmentId;
    uint16 nKeyAtts;
    Oid attTypeIds[INDEX_MAX_KEY_NUM];
};

// 校验上层元数据与 dstore 数据一致性
RetStatus VerifyMetadataConsistency(const MetadataInputStruct* metadata,
                                    VerifyReport* report);

}  // namespace DSTORE
```

### 2.5 Table-Level Aggregated Verification

```cpp
namespace DSTORE {

struct TableVerifyOptions {
    VerifyLevel pageLevel = VerifyLevel::HEAVYWEIGHT;
    BtreeVerifyOptions btreeOptions;
    HeapVerifyOptions heapOptions;
    SegmentVerifyOptions segmentOptions;
    bool checkMetadata = true;
    const MetadataInputStruct* metadata = nullptr;  // optional
};

// 对整表执行全量校验（聚合所有校验维度）
RetStatus VerifyTable(StorageRelationData* heapRel,
                      const TableVerifyOptions& options,
                      VerifyReport* report);

}  // namespace DSTORE
```

## 3. Result Reporting Interface

```cpp
namespace DSTORE {

class VerifyReport {
public:
    VerifyReport();
    ~VerifyReport();

    DISALLOW_COPY_AND_MOVE(VerifyReport);

    void AddResult(VerifySeverity severity,
                   const char* targetType,
                   const PageId& targetId,
                   const char* checkName,
                   uint64 expected,
                   uint64 actual,
                   const char* format, ...);

    bool HasError() const;
    uint64 GetErrorCount() const;
    uint64 GetWarningCount() const;
    uint64 GetTotalChecks() const;
    RetStatus GetRetStatus() const;

    // 输出格式化
    std::string FormatText() const;   // 可读文本（日志/CLI）
    std::string FormatJson() const;   // JSON（CLI 工具）

    // 迭代结果
    const std::vector<VerifyResult>& GetResults() const;

private:
    std::vector<VerifyResult> m_results;
    uint64 m_totalChecks;
    uint64 m_passedChecks;
    TimestampTz m_startTime;
    TimestampTz m_endTime;
};

}  // namespace DSTORE
```

## 4. CLI Tool Interface

```
Usage: dstore_verify [OPTIONS] <datadir>

Options:
  --table <oid>           校验指定表（by OID）
  --segment <segid>       校验指定 segment
  --page <fileid:blockid> 校验指定页面
  --level <lw|hw>         校验级别（lightweight/heavyweight，默认 heavyweight）
  --check-btree           执行 btree 结构校验
  --check-heap            执行 heap 数据校验
  --check-segment         执行 segment 元数据校验
  --check-extent          执行 extent 分配一致性校验
  --sample-ratio <0-100>  index-heap 数据校验采样比例（默认 100）
  --max-errors <N>        最大错误数（默认 1000）
  --format <text|json>    输出格式（默认 text）
  --all                   执行所有校验

Exit codes:
  0   所有校验通过
  1   发现 ERROR 级别问题
  2   仅有 WARNING 级别问题
  3   工具执行错误（参数错误、文件不可读等）
```

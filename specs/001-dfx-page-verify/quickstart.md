# Quickstart: DFX Page Verification

## 1. 为你的 PageType 添加校验函数

在你的模块目录下创建校验实现文件（如 `src/heap/dstore_heap_page_verify.cpp`）：

```cpp
#include "dfx/dstore_page_verify.h"
#include "page/dstore_heap_page.h"

namespace DSTORE {

static RetStatus VerifyHeapPageLightweight(const Page* page, VerifyLevel level, VerifyReport* report)
{
    // CRC、upper/lower bounds、LSN 已由框架检查
    // 这里添加 heap page 特有的轻量级检查
    const auto* heapPage = static_cast<const HeapPage*>(page);
    // ... heap-specific lightweight checks
    return DSTORE_SUCC;
}

static RetStatus VerifyHeapPageHeavyweight(const Page* page, VerifyLevel level, VerifyReport* report)
{
    const auto* heapPage = static_cast<const HeapPage*>(page);

    // ItemId datalen vs tuple length
    for (uint16 i = FIRST_ITEM_OFFSET_NUMBER; i <= heapPage->GetMaxOffsetNumber(); ++i) {
        const auto* itemId = heapPage->GetItemId(i);
        if (itemId->IsNormal()) {
            const auto* tuple = heapPage->GetTupleByOffset(i);
            if (itemId->GetLength() != tuple->GetSize()) {
                report->AddResult(VerifySeverity::ERROR, "page",
                    page->GetPageId(), "itemid_len_mismatch",
                    itemId->GetLength(), tuple->GetSize(),
                    "ItemId[%u] len %u != tuple size %u", i, itemId->GetLength(), tuple->GetSize());
            }
        }
    }
    // ... more heavyweight checks (TD state, TD-tuple consistency, etc.)
    return report->HasError() ? DSTORE_FAIL : DSTORE_SUCC;
}

// 在模块初始化时调用
void RegisterHeapPageVerifier()
{
    RegisterPageVerifier(PageType::HEAP_PAGE_TYPE, "HeapPage",
                         VerifyHeapPageLightweight,
                         VerifyHeapPageHeavyweight);
}

}  // namespace DSTORE
```

## 2. 在写入路径嵌入校验

在页面写入/刷脏处添加 inline 校验调用：

```cpp
// 例如在 FlushDirtyPage 中
RetStatus FlushDirtyPage(BufferDesc* buf)
{
    Page* page = GetPageFromBuffer(buf);

    // 刷脏前校验（根据 GUC 自动决定是否执行和级别）
    RetStatus verifyRet = VerifyPageInline(page);
    if (verifyRet != DSTORE_SUCC) {
        ErrLog(DSTORE_ERROR, MODULE_DFX,
               ErrMsg("Page verify failed before flush, pageId=(%u,%u)"),
               page->GetFileId(), page->GetBlockId());
        return verifyRet;  // 调用方决定是否继续
    }

    // 正常刷脏逻辑
    // ...
}
```

## 3. 执行跨页面校验

```cpp
// 校验单个表的 btree index
VerifyReport report;
BtreeVerifyOptions opts;
opts.checkHeapConsistency = true;
opts.checkDataConsistency = true;
opts.sampleRatio = 0.1f;  // 10% 采样

RetStatus ret = VerifyBtreeIndex(indexRel, heapRel, opts, &report);

if (report.HasError()) {
    ErrLog(DSTORE_ERROR, MODULE_DFX,
           ErrMsg("Btree verify found %lu errors"), report.GetErrorCount());
}
```

## 4. 使用 CLI 离线工具

```bash
# 校验整个数据目录的所有 segment
dstore_verify --all /data/dstore

# 校验指定表，重量级
dstore_verify --table 16384 --level hw /data/dstore

# 校验 btree 结构，10% 采样
dstore_verify --check-btree --sample-ratio 10 /data/dstore

# JSON 格式输出
dstore_verify --all --format json /data/dstore
```

## 5. 动态调整校验等级

```sql
-- 开启轻量级校验（写入路径生效）
SET dfx_verify_level = 'LIGHTWEIGHT';
SET dfx_verify_module = 'ALL';

-- 排查问题时提升到重量级
SET dfx_verify_level = 'HEAVYWEIGHT';

-- 关闭校验（生产环境默认）
SET dfx_verify_level = 'OFF';
```

## 6. US 闭环回归门禁

每个 user story 闭环前，必须在标准构建环境中重新执行以下 8 个官方 UT 目标：

```bash
cd tmp_build
make run_dstore_buffer_unittest
make run_dstore_datamanager_unittest
make run_dstore_framework_unittest
make run_dstore_ha_unittest
make run_dstore_index_unittest
make run_dstore_lock_unittest
make run_dstore_xact_unittest
make run_dstore_undo_unittest
```

执行约束：

- 未通过时，不进入下一个 user story。
- 本地 macOS + Docker 环境下，如果 `run_dstore_xact_unittest` 的唯一失败是 `UTTransactionTest.TransactionNullptr_level0`，可暂按已知基线环境问题忽略；其他失败仍然阻塞推进。
- 完成一轮门禁验证后，先做 checkpoint commit，再继续下一批实现。

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

每个 user story 闭环前，必须在容器内使用干净、串行的 debug UT 构建环境重新执行以下 8 个官方 UT 目标。

推荐流程：

```bash
rm -rf tmp_build
source buildenv
cd utils && bash build.sh -m debug && cd ..
bash build.sh -m debug -tm ut
cd tmp_build
```

随后执行以下 8 个官方 UT 目标：

```bash
make run_dstore_buffer_unittest
make run_dstore_xact_unittest
make run_dstore_index_unittest
make run_dstore_lock_unittest
make run_dstore_ha_unittest
make run_dstore_framework_unittest
make run_dstore_datamanager_unittest
make run_dstore_index_and_undo_unittest
```

执行约束：

- 未通过时，不进入下一个 user story。
- 不要在运行中的容器构建或 UT 任务之间切换分支、复用旧的 `tmp_build`，也不要并发执行会重建 `tmp_build` 的命令。
- 如果需要单独复核失败用例，也要在同一轮干净 debug 构建完成后，在 `tmp_build` 目录下直接运行 `./bin/unittest --gtest_filter=<case>`，不要额外切换到其他重建流程。
- 完成一轮门禁验证后，先做 checkpoint commit，再继续下一批实现。

## 7. US 闭环后的主线同步

每个 user story 闭环后，在继续下一个 story 之前，先询问是否执行一次主线同步，以避免当前功能分支与 `main` 偏离过大。

推荐顺序：

```bash
git stash push -u
git checkout main
git pull --ff-only origin main
git checkout <feature-branch>
git rebase main
git stash pop
```

执行约束：

- 只有在当前 UT 门禁收尾后再执行，避免与运行中的容器构建/测试共享同一工作区。
- `stash` 使用 `-u`，确保未跟踪的 spec/实现文件也一起保留。
- `rebase` 完成并恢复工作区后，再做一次 checkpoint commit。
- 每次执行前，先和需求方确认是否现在就做这一步。

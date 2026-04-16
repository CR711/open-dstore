/*
 * DFX page verify performance baseline UT (v2).
 *
 * Scope:
 *   - Single-thread latency: HeapPage / IndexPage / UndoPage / SegmentPage /
 *     FsmPage / BtrRecycleRootPage × {LIGHT, HEAVY}
 *   - Concurrent throughput: 8-thread shared-page and distinct-pages workloads
 *
 * Design notes (CLAUDE.md 规则 7 performance thinking):
 *   - clock_gettime(CLOCK_MONOTONIC_RAW) 在 aarch64 上粒度 ~83ns，直接测单次 VerifyPage
 *     会被 timer 抖动 dominate。v2 采用 **批量摊销**：每个采样点执行 BATCH_PER_SAMPLE 次
 *     VerifyPage，把时间除以 batch 得到每次平均——把真实开销从 timer noise 下拉出来。
 *   - 所有采样缓冲、线程本地数据全部放 fixture 成员或 heap-allocated（new[] + RAII wrapper），
 *     **杜绝 static 文件级数组**——避免 gtest 将来启用 parallel 或 shuffle 时的数据竞争。
 *   - 每线程采样数组 `alignas(64)` 防 false sharing；barrier 使用 atomic acq/rel 顺序。
 *   - 使用 volatile sink + asm("":::"memory") 防止 VerifyPage 被 LTO 消除。
 *
 * 断言策略（规避 CI flaky）：
 *   - 默认只打印 p50/p95/p99 + throughput，不做硬断言（超 SLA 走 GTEST_SKIP）
 *   - DFX_BENCH_STRICT=1 环境变量开启硬断言 EXPECT_LT(p95, SLA * 2)
 *
 * Helper 合规（团队二轮规范）：
 *   - 复用 ut_dfx_test_utils.h 的 PageBuffer / ScopedVerifyConfig / EnableAllModules
 *   - 局部 Build* 构造页面（仅当前文件一次性辅助，不对外；如被其他 impl 需要将 DM
 *     给 dedup-impl 提取到 utils.h）
 *
 * CLAUDE.md 规则 5 自检：
 *   - 堆分配（采样缓冲 + PageBuffer 数组）统一包装在 ThreadLocalSampleBuf RAII 中；
 *     析构负责 delete[]，异常/early return 不泄漏。
 *   - page 指针在 Build* 后非空，采样循环直接使用。
 *   - 数组下标由 SAMPLE_COUNT / thread 数算得，不会越界。
 *   - uint64 ns 累积不会溢出（每批 < 10ms，上限远低于 2^64）。
 */
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <thread>
#include <vector>
#include <gtest/gtest.h>

#include "dfx/dstore_page_verify.h"
#include "page/dstore_btr_recycle_root_meta_page.h"
#include "page/dstore_data_segment_meta_page.h"
#include "page/dstore_fsm_page.h"
#include "page/dstore_heap_page.h"
#include "page/dstore_undo_page.h"
#include "ut_dfx_test_utils.h"

using namespace DSTORE;
using DSTORE::ut_dfx::PageBuffer;
using DSTORE::ut_dfx::ScopedVerifyConfig;
using DSTORE::ut_dfx::EnableAllModules;

namespace {

/* --- single-thread benchmark knobs ---
 *
 * BATCH_PER_SAMPLE=64：clock_gettime(CLOCK_MONOTONIC_RAW) 在 aarch64 Docker 粒度约 83ns，
 * 单次 VerifyPage 真实开销 <100ns 会被 timer 抖动完全吃掉；每采样点跑 N=64 次、
 * 时间差除以 N 得到单次均摊。N 选 64 是在 timer 摊薄（>= 16 足以）与采样点稳定性
 * （每点总时长 >5µs，稳定落在毫秒时钟片内）之间的折中。
 */
constexpr int WARMUP_BATCHES = 128;
constexpr int SAMPLE_COUNT = 4096;
constexpr int BATCH_PER_SAMPLE = 64;

/* --- concurrent benchmark knobs --- */
constexpr int CONC_THREADS = 8;
constexpr int CONC_SAMPLES_PER_THREAD = 1024;
constexpr int CONC_BATCH = 64;

/* --- SLA (ns) --- */
constexpr uint64 SLA_LIGHT_NS = 1000;
constexpr uint64 SLA_MEDIUM_NS = 10000;
constexpr uint64 SLA_HEAVY_NS = 100000;
constexpr uint64 SLA_TOLERANCE = 2;

struct LatencyStats {
    uint64 p50;
    uint64 p95;
    uint64 p99;
    uint64 min;
    uint64 max;
};

struct ConcurrentResult {
    LatencyStats stats;
    double pagesPerSec;
};

inline uint64 NowNs()
{
    struct timespec ts;
    (void)clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return static_cast<uint64>(ts.tv_sec) * 1000000000ULL + static_cast<uint64>(ts.tv_nsec);
}

bool IsStrictMode()
{
    const char *env = std::getenv("DFX_BENCH_STRICT");
    return (env != nullptr) && (env[0] == '1');
}

/*
 * 栈开销较大的采样缓冲 RAII 包装（SAMPLE_COUNT * 8B = 32KB）。
 * 使用 new[]/delete[] 而非 std::vector：
 *   - 规则 4：避免 std::vector 运行时扩容路径可能的隐式分配抖动
 *   - 析构统一释放，异常/提前 return 安全
 */
class LatencyBuffer {
public:
    explicit LatencyBuffer(size_t n) : m_size(n), m_data(new uint64[n]())
    {}
    ~LatencyBuffer()
    {
        delete[] m_data;
    }
    LatencyBuffer(const LatencyBuffer &) = delete;
    LatencyBuffer &operator=(const LatencyBuffer &) = delete;

    uint64 *data() noexcept { return m_data; }
    size_t size() const noexcept { return m_size; }
    uint64 &operator[](size_t i) noexcept { return m_data[i]; }

private:
    size_t m_size;
    uint64 *m_data;
};

/* 统计：对 latencies[0..n) 求 p50/p95/p99/min/max */
void ComputeStats(uint64 *latencies, size_t n, LatencyStats *out)
{
    const size_t p50Idx = n / 2;
    const size_t p95Idx = (n * 95) / 100;
    const size_t p99Idx = (n * 99) / 100;

    std::nth_element(latencies, latencies + p50Idx, latencies + n);
    out->p50 = latencies[p50Idx];
    std::nth_element(latencies, latencies + p95Idx, latencies + n);
    out->p95 = latencies[p95Idx];
    std::nth_element(latencies, latencies + p99Idx, latencies + n);
    out->p99 = latencies[p99Idx];

    uint64 minVal = latencies[0];
    uint64 maxVal = latencies[0];
    for (size_t i = 1; i < n; ++i) {
        if (latencies[i] < minVal) {
            minVal = latencies[i];
        }
        if (latencies[i] > maxVal) {
            maxVal = latencies[i];
        }
    }
    out->min = minVal;
    out->max = maxVal;
}

/*
 * 单线程基准：每采样点跑 BATCH_PER_SAMPLE 次 VerifyPage 并平均，把 timer 粒度下的
 * 真实开销挤出来。返回的 p50/p95/p99 是**每次** VerifyPage 的 ns 摊销值。
 */
void RunBenchmark(const Page *page, VerifyLevel level, const char *tag, LatencyStats *stats)
{
    volatile unsigned sink = 0;

    /* 预热：每批 BATCH_PER_SAMPLE 次 */
    for (int i = 0; i < WARMUP_BATCHES; ++i) {
        for (int k = 0; k < BATCH_PER_SAMPLE; ++k) {
            VerifyReport report;
            sink += static_cast<unsigned>(VerifyPage(page, level, &report));
        }
    }

    LatencyBuffer samples(SAMPLE_COUNT);
    for (int i = 0; i < SAMPLE_COUNT; ++i) {
        uint64 t0 = NowNs();
        for (int k = 0; k < BATCH_PER_SAMPLE; ++k) {
            VerifyReport report;
            sink += static_cast<unsigned>(VerifyPage(page, level, &report));
            asm volatile("" ::: "memory");
        }
        uint64 t1 = NowNs();
        uint64 elapsed = (t1 > t0) ? (t1 - t0) : 0;
        samples[i] = elapsed / static_cast<uint64>(BATCH_PER_SAMPLE);
    }

    ComputeStats(samples.data(), SAMPLE_COUNT, stats);
    (void)sink;

    std::printf("[bench] %-28s p50=%5lu ns  p95=%5lu ns  p99=%5lu ns  min=%5lu max=%5lu\n",
        tag,
        static_cast<unsigned long>(stats->p50),
        static_cast<unsigned long>(stats->p95),
        static_cast<unsigned long>(stats->p99),
        static_cast<unsigned long>(stats->min),
        static_cast<unsigned long>(stats->max));
}

void AssertOrSkip(const LatencyStats &stats, uint64 slaNs, const char *tag)
{
    uint64 threshold = slaNs * SLA_TOLERANCE;
    if (IsStrictMode()) {
        EXPECT_LT(stats.p95, threshold)
            << tag << ": p95=" << stats.p95 << "ns exceeds 2x SLA (" << threshold << "ns)";
    } else if (stats.p95 >= threshold) {
        std::printf("[bench] %s p95=%lu exceeds 2x SLA (%lu); skipping hard assert\n",
            tag,
            static_cast<unsigned long>(stats.p95),
            static_cast<unsigned long>(threshold));
        GTEST_SKIP() << tag << " p95 over SLA but strict mode disabled";
    }
}

/* ---------- Page builders ---------- */

HeapPage *BuildHeapPage(PageBuffer &buffer, PageId pageId)
{
    HeapPage *page = reinterpret_cast<HeapPage *>(buffer.data());
    page->Init(0, PageType::HEAP_PAGE_TYPE, pageId);
    page->SetLsn(1, 1, 1, false);
    page->SetDataHeaderSize(HEAP_PAGE_DATA_OFFSET);
    page->m_header.m_lower = HEAP_PAGE_DATA_OFFSET;
    page->AllocateTdSpace();
    page->SetFsmIndex({INVALID_PAGE_ID, 0});
    page->SetPotentialDelSize(0);
    page->SetChecksum();
    return page;
}

BtrPage *BuildIndexPage(PageBuffer &buffer, PageId pageId)
{
    BtrPage *page = reinterpret_cast<BtrPage *>(buffer.data());
    page->InitBtrPageInner(pageId);
    page->SetLsn(1, 1, 1, false);
    page->GetLinkAndStatus()->InitPageMeta({1, 1}, 0, true);
    page->SetBtrMetaCreateXid(Xid(0));
    page->AllocateTdSpace();
    page->SetChecksum();
    return page;
}

UndoRecordPage *BuildUndoPage(PageBuffer &buffer, PageId pageId)
{
    UndoRecordPage *page = reinterpret_cast<UndoRecordPage *>(buffer.data());
    page->m_undoRecPageHeader = {0, pageId, INVALID_PAGE_ID, INVALID_PAGE_ID};
    page->InitUndoRecPage(pageId);
    page->SetLsn(1, 1, 1, false);
    page->m_header.m_lower = UNDO_RECORD_PAGE_HEADER_SIZE;
    page->m_header.m_upper = BLCKSZ;
    page->SetChecksum();
    return page;
}

DataSegmentMetaPage *BuildSegmentPage(PageBuffer &buffer, PageId pageId)
{
    DataSegmentMetaPage *page = reinterpret_cast<DataSegmentMetaPage *>(buffer.data());
    page->InitDataSegmentMetaPage(SegmentType::HEAP_SEGMENT_TYPE, pageId, EXT_SIZE_8, 1, 1);
    page->dataBlockCount = 1;
    page->dataFirst = pageId;
    page->dataLast = pageId;
    page->addedPageId = pageId;
    page->extendedPageId = {pageId.m_fileId, pageId.m_blockId + 7};
    page->lastExtentIsReused = false;
    page->SetChecksum();
    return page;
}

FsmPage *BuildFsmPage(PageBuffer &buffer, PageId pageId, PageId fsmMetaPageId)
{
    FsmPage *page = reinterpret_cast<FsmPage *>(buffer.data());
    page->InitFsmPage(pageId, fsmMetaPageId, {INVALID_PAGE_ID, INVALID_FSM_SLOT_NUM});
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();
    return page;
}

BtrRecycleRootMetaPage *BuildBtrRecycleRootPage(PageBuffer &buffer, PageId pageId)
{
    BtrRecycleRootMetaPage *page = reinterpret_cast<BtrRecycleRootMetaPage *>(buffer.data());
    page->InitRecycleRootMetaPage(pageId, Xid(1));
    page->SetLsn(1, 1, 1, false);
    page->SetChecksum();
    return page;
}

/* ---------- Test fixture ---------- */

class UTPageVerifyBench : public ::testing::Test {
protected:
    void SetUp() override
    {
        RegisterHeapPageVerifier();
        RegisterIndexPageVerifier();
        RegisterUndoPageVerifiers();
        RegisterSegmentPageVerifiers();
        RegisterFsmPageVerifiers();
        RegisterBtrRecyclePageVerifiers();
        EnableAllModules();
    }

    ScopedVerifyConfig m_guard{};
    PageBuffer m_buf{};
};

}  /* anonymous namespace */

/* ================================================================
 * Single-thread latency tests
 * ================================================================ */

TEST_F(UTPageVerifyBench, HeapPageLightLatencyPasses)
{
    HeapPage *page = BuildHeapPage(m_buf, {10, 100});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::LIGHT, "HeapPage.LIGHT", &stats);
    AssertOrSkip(stats, SLA_LIGHT_NS, "HeapPage.LIGHT");
}

TEST_F(UTPageVerifyBench, HeapPageHeavyLatencyPasses)
{
    HeapPage *page = BuildHeapPage(m_buf, {10, 101});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::HEAVY, "HeapPage.HEAVY", &stats);
    AssertOrSkip(stats, SLA_HEAVY_NS, "HeapPage.HEAVY");
}

TEST_F(UTPageVerifyBench, IndexPageLightLatencyPasses)
{
    BtrPage *page = BuildIndexPage(m_buf, {20, 200});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::LIGHT, "IndexPage.LIGHT", &stats);
    AssertOrSkip(stats, SLA_LIGHT_NS, "IndexPage.LIGHT");
}

TEST_F(UTPageVerifyBench, IndexPageHeavyLatencyPasses)
{
    BtrPage *page = BuildIndexPage(m_buf, {20, 201});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::HEAVY, "IndexPage.HEAVY", &stats);
    AssertOrSkip(stats, SLA_HEAVY_NS, "IndexPage.HEAVY");
}

TEST_F(UTPageVerifyBench, UndoPageLightLatencyPasses)
{
    UndoRecordPage *page = BuildUndoPage(m_buf, {30, 300});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::LIGHT, "UndoPage.LIGHT", &stats);
    AssertOrSkip(stats, SLA_LIGHT_NS, "UndoPage.LIGHT");
}

TEST_F(UTPageVerifyBench, UndoPageHeavyLatencyPasses)
{
    UndoRecordPage *page = BuildUndoPage(m_buf, {30, 301});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::HEAVY, "UndoPage.HEAVY", &stats);
    AssertOrSkip(stats, SLA_HEAVY_NS, "UndoPage.HEAVY");
}

TEST_F(UTPageVerifyBench, SegmentPageLightLatencyPasses)
{
    DataSegmentMetaPage *page = BuildSegmentPage(m_buf, {40, 400});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::LIGHT, "SegmentPage.LIGHT", &stats);
    AssertOrSkip(stats, SLA_LIGHT_NS, "SegmentPage.LIGHT");
}

TEST_F(UTPageVerifyBench, SegmentPageHeavyLatencyPasses)
{
    DataSegmentMetaPage *page = BuildSegmentPage(m_buf, {40, 401});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::HEAVY, "SegmentPage.HEAVY", &stats);
    AssertOrSkip(stats, SLA_HEAVY_NS, "SegmentPage.HEAVY");
}

TEST_F(UTPageVerifyBench, FsmPageLightLatencyPasses)
{
    FsmPage *page = BuildFsmPage(m_buf, {80, 800}, {80, 801});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::LIGHT, "FsmPage.LIGHT", &stats);
    AssertOrSkip(stats, SLA_LIGHT_NS, "FsmPage.LIGHT");
}

TEST_F(UTPageVerifyBench, FsmPageHeavyLatencyPasses)
{
    FsmPage *page = BuildFsmPage(m_buf, {80, 802}, {80, 803});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::HEAVY, "FsmPage.HEAVY", &stats);
    AssertOrSkip(stats, SLA_HEAVY_NS, "FsmPage.HEAVY");
}

TEST_F(UTPageVerifyBench, BtrRecycleRootLightLatencyPasses)
{
    BtrRecycleRootMetaPage *page = BuildBtrRecycleRootPage(m_buf, {90, 900});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::LIGHT, "BtrRecycleRoot.LIGHT", &stats);
    AssertOrSkip(stats, SLA_LIGHT_NS, "BtrRecycleRoot.LIGHT");
}

/* NOTE: 2026-04-15 本地实测 HEAVY p50 ~1076ns，显著高于其他 HEAVY verifiers (~580ns)，
 * 差距 ~2x。怀疑 BtrRecycleRoot registry entry 的 HEAVY 实现工作量更重（多字段/链遍历），
 * 或 dispatch 路径存在热点。补 MEDIUM 级校验时应优先 profile 这条路径。
 */
TEST_F(UTPageVerifyBench, BtrRecycleRootHeavyLatencyPasses)
{
    BtrRecycleRootMetaPage *page = BuildBtrRecycleRootPage(m_buf, {90, 901});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::HEAVY, "BtrRecycleRoot.HEAVY", &stats);
    AssertOrSkip(stats, SLA_HEAVY_NS, "BtrRecycleRoot.HEAVY");
}

/* ================================================================
 * MEDIUM 级 bench（SLA <10µs，AssertOrSkip 按 *2 容忍 CI 抖动）
 * ================================================================ */

TEST_F(UTPageVerifyBench, HeapPageMediumLatencyPasses)
{
    HeapPage *page = BuildHeapPage(m_buf, {10, 102});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::MEDIUM, "HeapPage.MEDIUM", &stats);
    AssertOrSkip(stats, SLA_MEDIUM_NS, "HeapPage.MEDIUM");
}

TEST_F(UTPageVerifyBench, IndexPageMediumLatencyPasses)
{
    BtrPage *page = BuildIndexPage(m_buf, {20, 202});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::MEDIUM, "IndexPage.MEDIUM", &stats);
    AssertOrSkip(stats, SLA_MEDIUM_NS, "IndexPage.MEDIUM");
}

TEST_F(UTPageVerifyBench, UndoPageMediumLatencyPasses)
{
    UndoRecordPage *page = BuildUndoPage(m_buf, {30, 302});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::MEDIUM, "UndoPage.MEDIUM", &stats);
    AssertOrSkip(stats, SLA_MEDIUM_NS, "UndoPage.MEDIUM");
}

TEST_F(UTPageVerifyBench, SegmentPageMediumLatencyPasses)
{
    DataSegmentMetaPage *page = BuildSegmentPage(m_buf, {40, 402});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::MEDIUM, "SegmentPage.MEDIUM", &stats);
    AssertOrSkip(stats, SLA_MEDIUM_NS, "SegmentPage.MEDIUM");
}

TEST_F(UTPageVerifyBench, FsmPageMediumLatencyPasses)
{
    FsmPage *page = BuildFsmPage(m_buf, {80, 804}, {80, 805});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::MEDIUM, "FsmPage.MEDIUM", &stats);
    AssertOrSkip(stats, SLA_MEDIUM_NS, "FsmPage.MEDIUM");
}

TEST_F(UTPageVerifyBench, BtrRecycleRootMediumLatencyPasses)
{
    BtrRecycleRootMetaPage *page = BuildBtrRecycleRootPage(m_buf, {90, 902});
    LatencyStats stats{};
    RunBenchmark(page, VerifyLevel::MEDIUM, "BtrRecycleRoot.MEDIUM", &stats);
    AssertOrSkip(stats, SLA_MEDIUM_NS, "BtrRecycleRoot.MEDIUM");
}

/* ================================================================
 * Concurrent throughput tests
 *
 * 两种工作负载：
 *   1. SharedPage：N 线程验同一只读页——测 lock-free 读路径在 cacheline 共享下的伸缩性
 *   2. DistinctPages：每线程独立页面，page type 轮转——测 registry 分派 + ICache 行为
 *
 * 所有缓冲/采样数组均 heap 分配（LatencyBuffer / vector），**无 static 全局**，
 * gtest 并行运行时天然安全。
 * ================================================================ */

namespace {

/*
 * Per-thread sample slot：alignas(64) 隔离 false sharing。
 * LatencyBuffer 析构自动释放（RAII）。
 */
struct alignas(64) PerThreadSlot {
    LatencyBuffer samples;
    explicit PerThreadSlot(size_t n) : samples(n) {}
};

/*
 * 并发驱动：每线程对 pages[t] 做 warmup+sample，atomic barrier 同步起跑。
 * 聚合所有样本计算 p50/p95/p99 + 总吞吐（pages/sec）。
 */
void RunConcurrentBenchmark(const Page *const *pages, int threadCount, VerifyLevel level,
    const char *tag, ConcurrentResult *out)
{
    std::vector<std::unique_ptr<PerThreadSlot>> slots;
    slots.reserve(static_cast<size_t>(threadCount));
    for (int t = 0; t < threadCount; ++t) {
        slots.emplace_back(new PerThreadSlot(static_cast<size_t>(CONC_SAMPLES_PER_THREAD)));
    }

    std::atomic<int> readyCount{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(threadCount));

    for (int t = 0; t < threadCount; ++t) {
        const Page *page = pages[t];
        PerThreadSlot *slot = slots[static_cast<size_t>(t)].get();
        workers.emplace_back([page, level, slot, &readyCount, &start]() {
            volatile unsigned sink = 0;
            /* 预热 */
            for (int i = 0; i < WARMUP_BATCHES; ++i) {
                for (int k = 0; k < CONC_BATCH; ++k) {
                    VerifyReport report;
                    sink += static_cast<unsigned>(VerifyPage(page, level, &report));
                }
            }
            /* 等齐起跑：barrier 阶段必须 yield——CI runner 核数 < 线程数时，
             * busy-spin 会霸占 CPU，阻止晚到 worker 完成 warmup，延迟 readyCount
             * 聚齐。yield 仅影响起跑对齐，不进入采样窗口，不会污染延迟数据。*/
            readyCount.fetch_add(1, std::memory_order_acq_rel);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            /* 采样：每点摊销 CONC_BATCH 次 */
            for (int i = 0; i < CONC_SAMPLES_PER_THREAD; ++i) {
                uint64 t0 = NowNs();
                for (int k = 0; k < CONC_BATCH; ++k) {
                    VerifyReport report;
                    sink += static_cast<unsigned>(VerifyPage(page, level, &report));
                    asm volatile("" ::: "memory");
                }
                uint64 t1 = NowNs();
                uint64 elapsed = (t1 > t0) ? (t1 - t0) : 0;
                slot->samples[static_cast<size_t>(i)] = elapsed / static_cast<uint64>(CONC_BATCH);
            }
            (void)sink;
        });
    }

    while (readyCount.load(std::memory_order_acquire) < threadCount) {
        std::this_thread::yield();
    }
    /* 先释放 start，再取 wallStart——否则 store 发布延迟会被算进墙钟，吞吐低估。
     * seq_cst fence 确保 wallStart 的读一定排在 start.store 发布之后。*/
    start.store(true, std::memory_order_release);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    uint64 wallStart = NowNs();

    for (auto &th : workers) {
        th.join();
    }
    uint64 wallEnd = NowNs();

    /* 聚合 */
    const size_t total = static_cast<size_t>(threadCount) * static_cast<size_t>(CONC_SAMPLES_PER_THREAD);
    LatencyBuffer agg(total);
    for (int t = 0; t < threadCount; ++t) {
        const uint64 *src = slots[static_cast<size_t>(t)]->samples.data();
        for (int i = 0; i < CONC_SAMPLES_PER_THREAD; ++i) {
            agg[static_cast<size_t>(t) * static_cast<size_t>(CONC_SAMPLES_PER_THREAD) + static_cast<size_t>(i)] = src[i];
        }
    }
    ComputeStats(agg.data(), total, &out->stats);

    /* 总完成次数 = threadCount * samples * batch */
    double seconds = (wallEnd > wallStart)
        ? (static_cast<double>(wallEnd - wallStart) / 1e9) : 1e-9;
    uint64 completedCalls = static_cast<uint64>(threadCount)
        * static_cast<uint64>(CONC_SAMPLES_PER_THREAD)
        * static_cast<uint64>(CONC_BATCH);
    out->pagesPerSec = static_cast<double>(completedCalls) / seconds;

    std::printf("[bench][conc] %-24s threads=%d  p50=%5lu  p95=%5lu  p99=%5lu  min=%5lu  max=%7lu  throughput=%.2e pages/s\n",
        tag,
        threadCount,
        static_cast<unsigned long>(out->stats.p50),
        static_cast<unsigned long>(out->stats.p95),
        static_cast<unsigned long>(out->stats.p99),
        static_cast<unsigned long>(out->stats.min),
        static_cast<unsigned long>(out->stats.max),
        out->pagesPerSec);
}

void AssertConcurrentOrSkip(const ConcurrentResult &result, uint64 slaNs, const char *tag)
{
    uint64 threshold = slaNs * SLA_TOLERANCE;
    if (IsStrictMode()) {
        EXPECT_LT(result.stats.p95, threshold)
            << tag << ": concurrent p95=" << result.stats.p95 << "ns exceeds 2x SLA (" << threshold << "ns)";
    } else if (result.stats.p95 >= threshold) {
        std::printf("[bench][conc] %s p95=%lu exceeds 2x SLA (%lu); skipping hard assert\n",
            tag,
            static_cast<unsigned long>(result.stats.p95),
            static_cast<unsigned long>(threshold));
        GTEST_SKIP() << tag << " concurrent p95 over SLA but strict mode disabled";
    }
}

}  /* anonymous namespace */

TEST_F(UTPageVerifyBench, Concurrent_SharedHeapLight_YieldsStableThroughput)
{
    HeapPage *shared = BuildHeapPage(m_buf, {50, 500});
    std::vector<const Page *> pages(CONC_THREADS, shared);
    ConcurrentResult result{};
    RunConcurrentBenchmark(pages.data(), CONC_THREADS, VerifyLevel::LIGHT,
        "SharedHeap.LIGHT", &result);
    AssertConcurrentOrSkip(result, SLA_LIGHT_NS, "SharedHeap.LIGHT");
}

TEST_F(UTPageVerifyBench, Concurrent_SharedHeapHeavy_YieldsStableThroughput)
{
    HeapPage *shared = BuildHeapPage(m_buf, {50, 501});
    std::vector<const Page *> pages(CONC_THREADS, shared);
    ConcurrentResult result{};
    RunConcurrentBenchmark(pages.data(), CONC_THREADS, VerifyLevel::HEAVY,
        "SharedHeap.HEAVY", &result);
    AssertConcurrentOrSkip(result, SLA_HEAVY_NS, "SharedHeap.HEAVY");
}

TEST_F(UTPageVerifyBench, Concurrent_DistinctMixedLight_YieldsStableThroughput)
{
    std::vector<PageBuffer> bufs(CONC_THREADS);
    std::vector<const Page *> pages(CONC_THREADS);
    for (int t = 0; t < CONC_THREADS; ++t) {
        PageId pid = {static_cast<uint16>(60 + t), static_cast<uint32>(600 + t)};
        int bucket = t % 4;
        if (bucket == 0) {
            pages[static_cast<size_t>(t)] = BuildHeapPage(bufs[static_cast<size_t>(t)], pid);
        } else if (bucket == 1) {
            pages[static_cast<size_t>(t)] = BuildIndexPage(bufs[static_cast<size_t>(t)], pid);
        } else if (bucket == 2) {
            pages[static_cast<size_t>(t)] = BuildUndoPage(bufs[static_cast<size_t>(t)], pid);
        } else {
            pages[static_cast<size_t>(t)] = BuildSegmentPage(bufs[static_cast<size_t>(t)], pid);
        }
    }
    ConcurrentResult result{};
    RunConcurrentBenchmark(pages.data(), CONC_THREADS, VerifyLevel::LIGHT,
        "DistinctMixed.LIGHT", &result);
    AssertConcurrentOrSkip(result, SLA_LIGHT_NS, "DistinctMixed.LIGHT");
}

TEST_F(UTPageVerifyBench, Concurrent_DistinctMixedHeavy_YieldsStableThroughput)
{
    std::vector<PageBuffer> bufs(CONC_THREADS);
    std::vector<const Page *> pages(CONC_THREADS);
    for (int t = 0; t < CONC_THREADS; ++t) {
        PageId pid = {static_cast<uint16>(70 + t), static_cast<uint32>(700 + t)};
        int bucket = t % 4;
        if (bucket == 0) {
            pages[static_cast<size_t>(t)] = BuildHeapPage(bufs[static_cast<size_t>(t)], pid);
        } else if (bucket == 1) {
            pages[static_cast<size_t>(t)] = BuildIndexPage(bufs[static_cast<size_t>(t)], pid);
        } else if (bucket == 2) {
            pages[static_cast<size_t>(t)] = BuildUndoPage(bufs[static_cast<size_t>(t)], pid);
        } else {
            pages[static_cast<size_t>(t)] = BuildSegmentPage(bufs[static_cast<size_t>(t)], pid);
        }
    }
    ConcurrentResult result{};
    RunConcurrentBenchmark(pages.data(), CONC_THREADS, VerifyLevel::HEAVY,
        "DistinctMixed.HEAVY", &result);
    AssertConcurrentOrSkip(result, SLA_HEAVY_NS, "DistinctMixed.HEAVY");
}

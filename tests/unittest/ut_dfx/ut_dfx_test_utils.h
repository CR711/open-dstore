/*
 * Shared test utilities for DFX unit tests.
 *
 * Provides:
 * - PageBuffer: aligned std::array wrapper for page buffers
 * - ScopedVerifyConfig: RAII guard for GUC verify level and modules
 * - HasVerifyCode: helper to check whether a VerifyCode appears in a report
 * - EnableAllModules / RestoreDefaultModules: convenience wrappers
 */
#ifndef DSTORE_TESTS_UT_DFX_TEST_UTILS_H
#define DSTORE_TESTS_UT_DFX_TEST_UTILS_H

#include <array>
#include <cstddef>
#include <cstring>
#include <string>
#include "dfx/dstore_page_verify.h"
#include "page/dstore_heap_page.h"
#include "page/dstore_index_page.h"
#include "tuple/dstore_index_tuple.h"

namespace DSTORE {
namespace ut_dfx {

/* ---------- Aligned PageBuffer ---------- */

/*
 * Inherits all std::array methods (.data(), .fill(), operator[], etc.)
 * while guaranteeing 8-byte alignment for page structures that
 * contain uint64 or require natural alignment.
 */
struct alignas(8) PageBuffer : public std::array<unsigned char, BLCKSZ> {};

/* ---------- RAII guard for GUC global state ---------- */

/*
 * Saves the current verify level and module bitmask on construction,
 * restores them on destruction.  Safe against early assertion failures
 * and exceptions — the destructor always runs.
 */
class ScopedVerifyConfig {
public:
    ScopedVerifyConfig() : m_level(GetDfxVerifyLevel()), m_modules(GetDfxVerifyModules())
    {}

    ~ScopedVerifyConfig()
    {
        SetDfxVerifyLevel(m_level);
        SetDfxVerifyModules(m_modules);
    }

    /* Non-copyable, non-movable */
    ScopedVerifyConfig(const ScopedVerifyConfig &) = delete;
    ScopedVerifyConfig &operator=(const ScopedVerifyConfig &) = delete;
    ScopedVerifyConfig(ScopedVerifyConfig &&) = delete;
    ScopedVerifyConfig &operator=(ScopedVerifyConfig &&) = delete;

private:
    VerifyLevel m_level;
    uint64 m_modules;
};

/* ---------- Report helper ---------- */

/* Check whether a specific VerifyCode appears in a VerifyReport. */
inline bool HasVerifyCode(const VerifyReport &report, VerifyCode code)
{
    for (const auto &r : report.GetResults()) {
        if (r.code == code) {
            return true;
        }
    }
    return false;
}

/*
 * Check whether any VerifyResult.checkName exactly matches the given name.
 * Required for verifiers (SegmentVerifier / MetadataVerifier) that use
 * AddResult(severity, targetType, ...) without an explicit VerifyCode —
 * result.code stays VerifyCode::OK there, so HasVerifyCode cannot match.
 *
 * Exact-match semantics prevent false positives like "extent_chain_broken"
 * matching "extent_chain_broken_XYZ" (reviewer Round 1 #7).
 */
inline bool HasCheckName(const VerifyReport &report, const char *checkName)
{
    if (checkName == nullptr) {
        return false;
    }
    for (const auto &r : report.GetResults()) {
        if (r.checkName != nullptr && std::strcmp(r.checkName, checkName) == 0) {
            return true;
        }
    }
    return false;
}

/* Check whether any VerifyResult has the given severity. */
inline bool HasSeverity(const VerifyReport &report, VerifySeverity severity)
{
    for (const auto &r : report.GetResults()) {
        if (r.severity == severity) {
            return true;
        }
    }
    return false;
}

/*
 * Count results whose checkName exactly matches.  Lets callers pin the
 * exact number of triggers (e.g. `EXPECT_EQ(CountCheckName(r, "x"), 1U)`)
 * rather than rely on presence-only assertions.
 */
inline uint64 CountCheckName(const VerifyReport &report, const char *checkName)
{
    if (checkName == nullptr) {
        return 0;
    }
    uint64 count = 0;
    for (const auto &r : report.GetResults()) {
        if (r.checkName != nullptr && std::strcmp(r.checkName, checkName) == 0) {
            ++count;
        }
    }
    return count;
}

/*
 * Count results of a specific severity.  Enables tight count assertions
 * (e.g. EXPECT_EQ(CountSeverity(report, SEVERITY_ERROR), 1U)) instead of
 * loose HasError()-only checks.
 */
inline uint64 CountSeverity(const VerifyReport &report, VerifySeverity severity)
{
    uint64 count = 0;
    for (const auto &r : report.GetResults()) {
        if (r.severity == severity) {
            ++count;
        }
    }
    return count;
}

/*
 * Count results matching a specific VerifyCode.  Used when a single
 * page triggers the same code multiple times and we need to pin the
 * exact number (not just presence).
 */
inline uint64 CountVerifyCode(const VerifyReport &report, VerifyCode code)
{
    uint64 count = 0;
    for (const auto &r : report.GetResults()) {
        if (r.code == code) {
            ++count;
        }
    }
    return count;
}

/* ---------- Module convenience wrappers ---------- */

/* 默认启用模块：HEAP(bit0) + INDEX(bit1) + UNDO(bit2) */
constexpr uint64 DEFAULT_VERIFY_MODULES = 0x07;
/* 全部模块：HEAP + INDEX + UNDO + SEGMENT(bit3) + FSM(bit4) */
constexpr uint64 ALL_VERIFY_MODULES = 0x1F;

/* Enable all verify modules (HEAP + INDEX + UNDO + SEGMENT + FSM). */
inline void EnableAllModules()
{
    SetDfxVerifyModules(ALL_VERIFY_MODULES);
}

/* Restore the default module bitmask (HEAP + INDEX + UNDO only). */
inline void RestoreDefaultModules()
{
    SetDfxVerifyModules(DEFAULT_VERIFY_MODULES);
}

/* ---------- Parameterized-test description structs ----------
 *
 * These structs are consumed by INSTANTIATE_TEST_SUITE_P to replace
 * large families of copy-paste TEST_F cases.  They intentionally use
 * C-style function pointers (not std::function / lambdas) to stay
 * consistent with the existing codebase style (CLAUDE.md rule 4).
 *
 * A parameterized case semantically says:
 *   "given a VALID page of kind `pageKind`, apply `injector` to
 *    corrupt one field, then run verifier at `level`; the report
 *    must contain `expectedCode` (or `altCode` if two codes are
 *    both acceptable) at severity `expectedSeverity`."
 */

enum class FaultPageKind {
    HEAP_KIND = 0,
    INDEX_KIND = 1,
};

/*
 * Corruption-injection signature (team-lead mandated, 2026-04-15).
 * Takes a raw page buffer and its size in bytes; must be idempotent
 * (re-applying should be a no-op or deterministic) and hold no global
 * state.  `pageSize` is passed explicitly so injectors can validate
 * offsets before writing — protects against future BLCKSZ changes.
 */
typedef void (*FaultInjectFn)(char *page, size_t pageSize);

struct FaultInjectCase {
    FaultPageKind pageKind;        /* PageType family (selects page builder) */
    FaultInjectFn injector;        /* CorruptionKind (mutates a valid page) */
    VerifyLevel triggerLevel;      /* Verify level at which fault must fire */
    VerifyCode expectedCode;       /* Primary VerifyCode expected in report */
    VerifyCode altCode;            /* VerifyCode::OK if no alternate is acceptable */
    VerifySeverity expectedSeverity;
    const char *label;             /* gtest name suffix */
};

/* ---------- Shared page builders (team-lead helper収斂, 2026-04-15) ----------
 *
 * Every test file that needs a "known-good" page of a given family must
 * call one of these.  Keep builders side-effect free and return the typed
 * page pointer aliased onto the caller's PageBuffer.  Callers then apply
 * InjectXxx() to corrupt exactly one field.
 */

inline HeapPage *MakeValidHeapPage(PageBuffer &buffer, PageId pageId = {10, 20})
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

inline void AddHeapTuple(HeapPage *page, OffsetNumber offset, uint16 tupleSize, uint8 tdId)
{
    page->SetUpper(static_cast<uint16>(page->GetUpper() - tupleSize));
    ItemId *itemId = page->GetItemIdPtr(offset);
    itemId->SetNormal(page->GetUpper(), tupleSize);
    page->SetLower(static_cast<uint16>(page->GetLower() + sizeof(ItemId)));

    HeapDiskTuple *tuple = page->GetDiskTuple(offset);
    tuple->SetTupleSize(tupleSize);
    tuple->SetTdId(tdId);
    tuple->SetLockerTdId(INVALID_TD_SLOT);
    tuple->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    tuple->SetLiveMode(HeapDiskTupLiveMode::TUPLE_BY_NORMAL_INSERT);
    tuple->SetNumColumn(1);
}

inline BtrPage *MakeValidIndexPage(PageBuffer &buffer, PageId pageId = {30, 40})
{
    BtrPage *page = reinterpret_cast<BtrPage *>(buffer.data());
    page->InitBtrPageInner(pageId);
    page->SetLsn(1, 1, 1, false);
    page->GetLinkAndStatus()->InitPageMeta({1, 1}, 0, false);
    page->SetBtrMetaCreateXid(Xid(0));
    page->AllocateTdSpace();
    page->SetChecksum();
    return page;
}

inline void AddIndexTuple(BtrPage *page, OffsetNumber offset, uint16 tupleSize, uint8 tdId)
{
    page->SetUpper(static_cast<uint16>(page->GetUpper() - tupleSize));
    ItemId *itemId = page->GetItemIdPtr(offset);
    itemId->SetNormal(page->GetUpper(), tupleSize);
    page->SetLower(static_cast<uint16>(page->GetLower() + sizeof(ItemId)));

    IndexTuple *tuple = page->GetIndexTuple(offset);
    tuple->SetSize(tupleSize);
    tuple->SetTdId(tdId);
    tuple->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    ItemPointerData heapCtid = INVALID_ITEM_POINTER;
    tuple->SetHeapCtid(&heapCtid);
}

}  /* namespace ut_dfx */
}  /* namespace DSTORE */

#endif /* DSTORE_TESTS_UT_DFX_TEST_UTILS_H */

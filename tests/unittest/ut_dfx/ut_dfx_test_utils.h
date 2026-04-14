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
#include "dfx/dstore_page_verify.h"

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

}  /* namespace ut_dfx */
}  /* namespace DSTORE */

#endif /* DSTORE_TESTS_UT_DFX_TEST_UTILS_H */

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

#ifndef FAULT_INJECTOR_H
#define FAULT_INJECTOR_H

#include <cstdint>
#include <atomic>
#include <mutex>
#include <vector>
#include "stress_verify_common.h"
#include "page/dstore_page_struct.h"

namespace DSTORE {

/* Forward declaration for Page (defined in page/dstore_page.h) */
struct Page;
namespace STRESS_VERIFY {

/*
 * FaultInjector
 *
 * Three-layer fault injection:
 * 1. Disk layer: Corrupt page content before/after disk write
 * 2. Memory layer: Corrupt in-memory buffer page
 * 3. Path layer: Inject fault at specific operation point
 *
 * Uses existing FAULT_INJECTION_* framework integration.
 */
class FaultInjector {
public:
    explicit FaultInjector(const StressVerifyConfig &config);
    ~FaultInjector() = default;

    /* Check if fault injection is enabled */
    bool IsEnabled() const { return m_config.faultInjectEnabled; }

    /* Check if injection time has been reached */
    bool ShouldInject(uint32_t elapsedSec);

    /* Mark injection as done */
    void MarkInjected();

    /* Get target page for injection (0 means random) */
    uint32_t GetTargetPage();

    /* Inject fault into a page */
    bool InjectFault(Page *page, const PageId &pageId);

    /* Get injection status */
    bool WasInjected() const { return m_injected.load(); }
    FaultType GetFaultType() const { return m_config.faultType; }
    FaultTargetModule GetTargetModule() const { return m_config.faultTargetModule; }

private:
    /* Layer-specific injection methods */
    bool InjectDiskFault(Page *page);
    bool InjectMemoryFault(Page *page);
    bool InjectPathFault(Page *page);

    /* Fault-specific corruption methods */
    void CorruptPageCrc(Page *page);
    void CorruptPageType(Page *page);
    void CorruptPageId(Page *page);
    void CorruptPageBoundary(Page *page);
    void CorruptHeapTdCount(Page *page);
    void CorruptHeapItemidAlign(Page *page);
    void CorruptHeapTupleOverlap(Page *page);
    void CorruptHeapTupleSize(Page *page);
    void CorruptIndexPageType(Page *page);
    void CorruptIndexMetaPage(Page *page);
    void CorruptIndexQueue(Page *page);

    StressVerifyConfig              m_config;
    std::atomic<bool>               m_injected{false};
    std::atomic<uint32_t>           m_injectionTimeReached{0};
    std::mutex                      m_injectMutex;

    /* Recorded injection details */
    uint32_t                        m_actualTargetPage{0};
    uint64_t                        m_injectionTimestampUs{0};
};

} /* namespace STRESS_VERIFY */
} /* namespace DSTORE */

#endif /* FAULT_INJECTOR_H */
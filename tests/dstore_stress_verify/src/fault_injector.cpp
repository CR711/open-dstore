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

#include "fault_injector.h"
#include "page/dstore_page.h"
#include <iostream>
#include <chrono>
#include <cstring>

namespace DSTORE {
namespace STRESS_VERIFY {

FaultInjector::FaultInjector(const StressVerifyConfig &config)
    : m_config(config)
{
}

bool FaultInjector::ShouldInject(uint32_t elapsedSec)
{
    if (!m_config.faultInjectEnabled) return false;
    if (m_injected.load()) return false;
    return elapsedSec >= m_config.faultInjectTime;
}

void FaultInjector::MarkInjected()
{
    m_injected.store(true);
}

uint32_t FaultInjector::GetTargetPage()
{
    if (m_config.faultTargetPage == 0) {
        /* Random page selection - would need context from actual pages */
        return 1;  /* Placeholder: first data page */
    }
    return m_config.faultTargetPage;
}

bool FaultInjector::InjectFault(Page *page, const PageId &pageId)
{
    if (page == nullptr) return false;

    std::lock_guard<std::mutex> lock(m_injectMutex);
    if (m_injected.load()) return false;

    /* Record injection details */
    m_actualTargetPage = pageId.m_blockId;
    auto now = std::chrono::system_clock::now();
    m_injectionTimestampUs = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();

    /* Apply fault based on type */
    switch (m_config.faultType) {
        case FAULT_CRC_ERROR:
            CorruptPageCrc(page);
            break;
        case FAULT_PAGE_TYPE_INVALID:
            CorruptPageType(page);
            break;
        case FAULT_PAGE_ID_MISMATCH:
            CorruptPageId(page);
            break;
        case FAULT_BOUNDARY_ERROR:
            CorruptPageBoundary(page);
            break;
        case FAULT_HEAP_TD_COUNT_ERROR:
            CorruptHeapTdCount(page);
            break;
        case FAULT_HEAP_ITEMID_ALIGN_ERROR:
            CorruptHeapItemidAlign(page);
            break;
        case FAULT_HEAP_TUPLE_OVERLAP:
            CorruptHeapTupleOverlap(page);
            break;
        case FAULT_HEAP_TUPLE_SIZE_ERROR:
            CorruptHeapTupleSize(page);
            break;
        case FAULT_INDEX_PAGE_TYPE_ERROR:
            CorruptIndexPageType(page);
            break;
        case FAULT_INDEX_META_PAGE_ERROR:
            CorruptIndexMetaPage(page);
            break;
        case FAULT_INDEX_QUEUE_ERROR:
            CorruptIndexQueue(page);
            break;
        default:
            std::cout << "Unknown fault type: " << m_config.faultType << std::endl;
            return false;
    }

    std::cout << "[INJECT] Fault: " << FaultTypeToString(m_config.faultType)
              << " | Module: " << ModuleToString(m_config.faultTargetModule)
              << " | Page: " << m_actualTargetPage << std::endl;

    MarkInjected();
    return true;
}

/* ----------------------------------------------------------------
 * Fault-specific corruption implementations
 *
 * These methods directly corrupt the Page struct fields to simulate
 * various types of page corruption for verification testing.
 * ---------------------------------------------------------------- */

void FaultInjector::CorruptPageCrc(Page *page)
{
    /* Corrupt the page checksum field in the header */
    page->m_header.m_checksum = 0xBEEF;  /* Invalid CRC value */
}

void FaultInjector::CorruptPageType(Page *page)
{
    /* Set an invalid page type in the header */
    page->m_header.m_type = 0xFF;  /* Invalid type */
}

void FaultInjector::CorruptPageId(Page *page)
{
    /* Set page ID to invalid value */
    page->m_header.m_myself.m_blockId = 0xFFFFFFFF;
}

void FaultInjector::CorruptPageBoundary(Page *page)
{
    /* Make lower/upper overlap */
    page->m_header.m_lower = page->m_header.m_upper + 100;  /* Invalid: lower > upper */
}

void FaultInjector::CorruptHeapTdCount(Page *page)
{
    /*
     * Corrupt the area right after the base page header where the TD count
     * would reside in a heap page. We write raw bytes to avoid depending
     * on HeapPage internals at compile time.
     */
    uint16 badTdCount = 1000;
    char *raw = reinterpret_cast<char *>(page);
    /* TD count is stored in the heap-specific header area after the base Page header */
    size_t tdCountOffset = sizeof(Page::PageHeader);
    memcpy(raw + tdCountOffset, &badTdCount, sizeof(badTdCount));
}

void FaultInjector::CorruptHeapItemidAlign(Page *page)
{
    /* Set lower to a misaligned value */
    page->m_header.m_lower = sizeof(Page::PageHeader) + 3;  /* Not aligned to 4 bytes */
}

void FaultInjector::CorruptHeapTupleOverlap(Page *page)
{
    /*
     * Corrupt the first item pointer area to create an overlap.
     * Item pointers reside in the area between header and lower.
     */
    if (page->m_header.m_lower > sizeof(Page::PageHeader) + 4) {
        char *raw = reinterpret_cast<char *>(page);
        /* Zero out the first item pointer to create inconsistency */
        memset(raw + sizeof(Page::PageHeader), 0xFF, 4);
    }
}

void FaultInjector::CorruptHeapTupleSize(Page *page)
{
    /*
     * Corrupt the item pointer area to set an unrealistic size.
     */
    if (page->m_header.m_lower > sizeof(Page::PageHeader) + 4) {
        char *raw = reinterpret_cast<char *>(page);
        uint32 badSize = 0xFFFFFFFF;
        memcpy(raw + sizeof(Page::PageHeader), &badSize, sizeof(badSize));
    }
}

void FaultInjector::CorruptIndexPageType(Page *page)
{
    /* Set an invalid page type to trigger index page type validation failure */
    page->m_header.m_type = 0xFE;
}

void FaultInjector::CorruptIndexMetaPage(Page *page)
{
    /* Corrupt the page ID to trigger meta page validation failure */
    page->m_header.m_myself.m_fileId = 0xFFFF;
    page->m_header.m_myself.m_blockId = 0xFFFFFFFF;
}

void FaultInjector::CorruptIndexQueue(Page *page)
{
    /*
     * Corrupt the index-specific area after the base header to create
     * queue inconsistency in B-tree recycle queue pages.
     */
    char *raw = reinterpret_cast<char *>(page);
    size_t offset = sizeof(Page::PageHeader);
    /* Write garbage to the first 8 bytes of the index-specific area */
    if (page->m_header.m_lower > offset + 8) {
        uint64_t garbage = 0xDEADDEADDEADDEADULL;
        memcpy(raw + offset, &garbage, sizeof(garbage));
    }
}

} /* namespace STRESS_VERIFY */
} /* namespace DSTORE */

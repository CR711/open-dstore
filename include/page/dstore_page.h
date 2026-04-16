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
 *
 * ---------------------------------------------------------------------------------------
 *
 * dstore_heap_handler.h
 *
 * IDENTIFICATION
 *        storage/include/page/dstore_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_PAGE_H
#define DSTORE_PAGE_H
#include "common/dstore_datatype.h"
#include "common/algorithm/dstore_string_info.h"
#include "page/dstore_page_struct.h"

#define PAGE_HEADER_FMT \
    "checksum:%hu, needcrc:%hu off:%hu glsn:%lu plsn:%lu walId:%lu lower:%hu upper:%hu type:%hu Page(%hu, %u)"
#define PAGE_HEADER_VAL(page) (page)->m_header.m_checksum, (page)->m_header.m_special.m_needCrc, \
    (page)->m_header.m_special.m_offset, (page)->m_header.m_glsn, (page)->m_header.m_plsn, (page)->m_header.m_walId, \
    (page)->m_header.m_lower, (page)->m_header.m_upper, (page)->m_header.m_type, (page)->m_header.m_myself.m_fileId, \
    (page)->m_header.m_myself.m_blockId

constexpr uint32 PAGE_SIZE = BLCKSZ;
constexpr uint32 EXTEND_PAGE_SIZE = BLCKSZ * 2;

namespace DSTORE {
/**
 * Base class for all page
 * contain common header (LSN, checksum, version,etc)
 */
struct Page {
    struct PACKED PageHeader {
        uint16 m_checksum;
        struct {
            uint16 m_needCrc : 1;  /* special region in the page need crc check or not */
            uint16 m_offset : 14;  /* the offset of special region in the page */
            uint16 m_reserved : 1;  /* reserved, will be used later */
        } m_special;
        uint64 m_glsn;
        uint64 m_plsn;
        WalId m_walId;
        uint16 m_flags;
        uint16 m_lower;
        uint16 m_upper;
        uint16 m_type;
        PageId m_myself;
    };

    PageHeader m_header;
    static const char *PageTypeToStr(PageType type);
    /*
     * Note: header lower is set to default page header size;
     * If inherit page has larger header size, must modify the m_lower outside this function.
     */
    void Init(uint16 specialSize, PageType type, const PageId &selfPageId);
    /* help function */

    inline bool PageNoInit() const
    {
        return (m_header.m_upper == 0);
    }

    inline uint64 GetGlsn() const
    {
        return m_header.m_glsn;
    }

    inline uint64 GetPlsn() const
    {
        return m_header.m_plsn;
    }

    inline WalId GetWalId() const
    {
        return m_header.m_walId;
    }

#ifdef UT
    inline void SetGlsn(uint64 glsn)
    {
        m_header.m_glsn = glsn;
    }

    inline void SetPlsn(uint64 plsn)
    {
        m_header.m_plsn = plsn;
    }

    inline void SetWalStreamId(WalId walId)
    {
        m_header.m_walId = walId;
    }
#endif

    inline void SetLsn(const WalId walId, const uint64 plsn, const uint64 glsn, const bool newPage = false)
    {
        /* Todo: we can get rid of the newPage flag if at time of init we assign invalid values to the lsn. */
        LsnSanityCheck(walId, plsn, glsn, newPage);

        m_header.m_walId = walId;
        m_header.m_plsn = plsn;
        m_header.m_glsn = glsn;
    }

    /* Todo: Is this function too complex for inline? Will performance be affected either way? */
    inline void LsnSanityCheck(const WalId walId, const uint64 plsn, const uint64 glsn, const bool newPage)
    {
        /* Todo: the page class should not need to know about WAL, we should move the sanity check to a common place. */
        const uint64 invalidGlsn = UINT64_MAX;
        ErrLevel errorLevel = DSTORE_PANIC;
#ifdef UT
        errorLevel = DSTORE_ERROR;
#endif

        /* Todo: unify this compare with the PageInfoVersion one. */
        /*
         * Incorrect LSN updates:
         *     1. This is a new page yet we are not setting the lsn to be (0,0)
         *     2. This is NOT a new page yet we are setting the plsn to be 0
         *     3. We are setting the glsn to be invalid
         *     4. This is NOT a new page yet we are setting the lsn to be smaller than or equal to the current lsn
         *        Note that we do not do this check for new pages as we are often setting the lsn from (0,0) to (0,0)
         */
        /* Todo: the WAL team is not 100% confident in this constraint, they wish to log it at error for now. */
        errorLevel = ((glsn == 0) && (plsn == 0)) ? DSTORE_ERROR : errorLevel;

        if ((newPage && ((glsn != 0) || (plsn != 0))) ||
            (!newPage && (plsn == 0)) ||
            (glsn == invalidGlsn) ||
            (!newPage && ((GetGlsn() > glsn) || ((GetGlsn() == glsn) && (GetPlsn() >= plsn))))) {
            ErrLog(errorLevel, MODULE_PAGE,
                ErrMsg("LsnSanityCheck. Overwriting current LSN:(%lu, %lu), WALID:%lu,"
                " with a new smaller/invalid/incorrect LSN:(%lu, %lu), WALID:%lu, newPage:%d."
                " Page Header Info. m_checksum:%u, m_special:(%u, %u, %u), m_flags:%u, m_lower:%u, m_upper:%u,"
                " m_type:%u, m_myself:(%u, %u).",
                GetGlsn(), GetPlsn(), GetWalId(), glsn, plsn, walId, newPage,
                m_header.m_checksum, m_header.m_special.m_needCrc, m_header.m_special.m_offset,
                m_header.m_special.m_reserved, m_header.m_flags, m_header.m_lower, m_header.m_upper, m_header.m_type,
                m_header.m_myself.m_fileId, m_header.m_myself.m_blockId));
        }
    }

    inline uint16 GetChecksum() const
    {
        return m_header.m_checksum;
    }

    inline char *PageHeaderPtr()
    {
        StorageAssert(static_cast<void *>(&m_header) == static_cast<void *>(this));
        return static_cast<char *>(static_cast<void *>(&m_header));
    }

    inline const char *PageHeaderPtr() const
    {
        return reinterpret_cast<const char *>(this);
    }

    inline uint16 GetSpecialOffset() const
    {
        return m_header.m_special.m_offset;
    }

    inline uint16 SetSpecialOffset(uint16 offset)
    {
        return m_header.m_special.m_offset = offset;
    }

    inline uint16 GetDataBeginOffset() const
    {
        return m_header.m_lower;
    }

    inline PageId GetSelfPageId() const
    {
        return m_header.m_myself;
    }

    inline BlockNumber GetBlockNum() const
    {
        return m_header.m_myself.m_blockId;
    }

    inline FileId GetFileId() const
    {
        return m_header.m_myself.m_fileId;
    }

    inline uint16 GetUpper() const
    {
        return m_header.m_upper;
    }
    inline void SetUpper(uint16 upper)
    {
        m_header.m_upper = upper;
    }

    inline uint16 GetLower() const
    {
        return m_header.m_lower;
    }
    inline void SetLower(uint16 lower)
    {
        m_header.m_lower = lower;
    }

    inline PageType GetType() const
    {
        return static_cast<PageType>(m_header.m_type);
    }

    inline bool TestType(PageType type) const
    {
        return m_header.m_type == static_cast<uint16>(type);
    }

    void DumpHeader(StringInfo str) const;

    void SetChecksum(bool isCrExtend = false);

    bool CheckPageCrcMatch() const;
};

static_assert(std::is_standard_layout<Page>::value == true,
              "Page must be C-like memory struct, forbid to add virtual function.");


/**
 * The extended space in file called by ftruncate interface is 0(INVALID_PAGE_TYPE).
 * so we can check whether the page can be allocated by m_type in page header.
 * */
inline bool PageHasAlloc(const Page *page)
{
    return page->GetType() != PageType::INVALID_PAGE_TYPE;
}
constexpr uint64 INVALID_PLSN = 0;
constexpr uint64 INVALID_END_PLSN = UINT64_MAX;
constexpr uint64 INVALID_WAL_GLSN = UINT64_MAX;
constexpr WalId INVALID_WAL_ID = UINT64_MAX;
} /* namespace DSTORE */
#endif

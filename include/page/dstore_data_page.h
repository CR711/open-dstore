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
 * dstore_data_page.h
 *
 * IDENTIFICATION
 *        include/page/dstore_data_page.h
 *
 *  Base struct for heap/index page
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_PAGE_DSTORE_DATA_PAGE_H
#define SRC_GAUSSKERNEL_INCLUDE_PAGE_DSTORE_DATA_PAGE_H

#include "common/algorithm/dstore_binaryheap.h"
#include "index/dstore_btree_undo_data_struct.h"
#include "page/dstore_page.h"
#include "page/dstore_fsm_page.h"
#include "page/dstore_itemid.h"
#include "page/dstore_itemptr.h"
#include "transaction/dstore_transaction_types.h"
#include "undo/dstore_undo_record.h"
#include "page/dstore_td.h"
#include "transaction/dstore_transaction_types.h"

namespace DSTORE {

using TdId = uint8_t;

struct CRContext;
class UndoMgr;
/* ----------------
 *      support macros
 * ----------------
 */

/*
 * OffsetNumberIsValid
 *      True iff the offset number is valid.
 */
inline bool DstoreOffsetNumberIsValid(const OffsetNumber &offset)
{
    return offset != INVALID_ITEM_OFFSET_NUMBER;
}

/*
 * OffsetNumberNext
 * OffsetNumberPrev
 *      Increments/decrements the argument.  These macros look pointless
 *      but they help us disambiguate the different manipulations on
 *      OffsetNumbers (e.g., sometimes we subtract one from an
 *      OffsetNumber to move back, and sometimes we do so to form a
 *      real C array index).
 */
inline OffsetNumber OffsetNumberNext(const OffsetNumber &offset)
{
    return static_cast<OffsetNumber>(1 + offset);
}

inline OffsetNumber OffsetNumberPrev(const OffsetNumber &offset)
{
    return static_cast<OffsetNumber>(-1 + offset);
}

const uint16 PAGE_HAS_FREE_LINES = (1);  /* are there any unused line pointers? */
const uint16 PAGE_TUPLE_PRUNABLE = (1 << 1);  /* has deleted tuple that could be pruned? */
const uint16 PAGE_ITEM_PRUNABLE = (1 << 2);   /* has redirected item that could be pruned? */
const uint16 PAGE_IS_NEW_PAGE = (1 << 3);     /* is this page a newly allocated page? */
const uint16 PAGE_IS_EXTEND_CR_PAGE = (1 << 4); /* is extend for cr page */
class Transaction;

/* IndexTuple defrag support for compact_tuples and compact_items */
struct ItemIdCompactData {
    uint16 tupOffset;      /* page offset of tuple */
    uint16 itemOffnum;     /* page offset number of item_id */
    uint16 itemLen;        /* itemId->len */

    static inline int Compare(const void *a, const void *b)
    {
        const ItemIdCompactData itemA = *(static_cast<const ItemIdCompactData *>(a));
        const ItemIdCompactData itemB = *(static_cast<const ItemIdCompactData *>(b));
        return (itemA.tupOffset > itemB.tupOffset) ? (-1) : 1;
    }
};
using ItemIdCompact = ItemIdCompactData *;

enum class FreeSpaceCondition {
    RAW,
    EXCLUDE_ITEMID,
};

constexpr uint16 HEAP_PAGE_HEADER_SIZE = 88;

// heap page header total size is 88 (including reserved)
#define RESERVED_HEAP_PAGE_HEADER_SIZE 5

struct PACKED HeapPageHeader {
    uint16 potentialDelSize;
    FsmIndex fsmIndex;                /* level 0 fsm index */
    uint64 recentDeadTupleMinCsn;     /* record min csn for redirected tuple, for prune only. */
    uint8 reserved[RESERVED_HEAP_PAGE_HEADER_SIZE];
};

#define RESERVED_DATA_PAGE_HEADER_SIZE 8

struct PACKED DataPageHeader {
    uint8 tdCount;
    uint32 versionNum;
    uint16 headerOffset;
    Xid segmentCreateXid;
    uint8 reserved[RESERVED_DATA_PAGE_HEADER_SIZE];
};

constexpr uint32 DATA_PAGE_HEADER_SIZE = (sizeof(Page) + sizeof(DataPageHeader));
constexpr uint32 HEAP_PAGE_DATA_OFFSET = (DATA_PAGE_HEADER_SIZE + sizeof(HeapPageHeader));

/* We must set a fixed btree BtrPage's header size to keep the max indexTuple size stable. */
constexpr uint32 RESERVED_BTR_PAGE_HEADER_SIZE = 72;
static_assert(RESERVED_BTR_PAGE_HEADER_SIZE >= DATA_PAGE_HEADER_SIZE,
              "BtrPage's header size must be no smaller than DataPage's header size");
struct PACKED BtrPageHeader {
    char reserved[RESERVED_BTR_PAGE_HEADER_SIZE - DATA_PAGE_HEADER_SIZE];
};
constexpr uint32 BTR_PAGE_DATA_OFFSET = RESERVED_BTR_PAGE_HEADER_SIZE;

/*
 * Base Class for index/heap page.
 */
struct DataPage : public Page {
    DataPageHeader dataHeader;

    template<FreeSpaceCondition cond> uint32 GetFreeSpace();
    uint32 GetFreeSpaceForInsert();

    inline char *GetDataOffset()
    {
        return reinterpret_cast<char *>(this) + dataHeader.headerOffset;
    }

    inline const char *GetDataOffset() const
    {
        return reinterpret_cast<const char *>(this) + dataHeader.headerOffset;
    }

    /* TODO: rmv the redo paramater, use the table status (replay、normal) judge whether check CsnStatus */
    inline void SetTd(uint8 tdId, Xid xid, UndoRecPtr undoPtr, CommandId commandId)
    {
        StorageAssert(xid != INVALID_XID);
        TD *td = GetTd(tdId);

        if (td->GetXid() != INVALID_XID && td->GetXid() != xid) {
            if (td->TestCsnStatus(IS_CUR_XID_CSN)) {
                StorageAssert(td->GetCsn() != INVALID_CSN);
                td->SetCsnStatus(IS_PREV_XID_CSN);
            } else if (td->TestCsnStatus(IS_PREV_XID_CSN)) {
                StorageAssert(td->GetCsn() != INVALID_CSN);
            } else {
                StorageAssert(td->GetCsn() == INVALID_CSN);
                StorageAssert(td->GetCsnStatus() == IS_INVALID);
            }
        }

        td->SetXid(xid);
        td->SetUndoRecPtr(undoPtr);
        td->SetStatus(TDStatus::OCCUPY_TRX_IN_PROGRESS);
        td->SetCommandId(commandId);
    }

    inline TD *GetTd(uint8 tdId)
    {
        StorageAssert(tdId != INVALID_TD_SLOT);
        StorageAssert((tdId + 1) * sizeof(TD) <= (static_cast<size_t>(BLCKSZ) - DataHeaderSize()));
        return static_cast<TD *>(static_cast<void *>(GetDataOffset() + tdId * sizeof(TD)));
    }

    inline uint8 GetTdCount() const
    {
        return dataHeader.tdCount;
    }

    inline bool IsTdValidAndOccupied(TdId tdId)
    {
        /* Check if the TD slot belongs to the TD space on the current page */
        /* Check if the TD slot is oppcupied by any tuple */
        return (tdId < GetTdCount() && !GetTd(tdId)->TestStatus(TDStatus::UNOCCUPY_AND_PRUNEABLE));
    }

    inline void AllocateTdSpace(uint8 tdCount = DEFAULT_TD_COUNT)
    {
        /* pivot page do not need td array. */
        dataHeader.tdCount = tdCount;
        m_header.m_lower += tdCount * sizeof(TD);
        for (uint8 tdId = 0; tdId < tdCount; ++tdId) {
            TD *td = GetTd(tdId);
            td->Reset();
        }
    }

    inline ItemId *GetItemIdPtr(OffsetNumber offset)
    {
        uint16 itemOffsetPos = static_cast<uint16>(TdDataSize() + (offset - 1) * static_cast<uint16>(sizeof(ItemId)));
        StorageAssert(itemOffsetPos < (BLCKSZ - DataHeaderSize()));
        ItemId* id = static_cast<ItemId *>(static_cast<void *>(GetDataOffset() + itemOffsetPos));
        return id;
    }

    inline const ItemId *GetItemIdPtr(OffsetNumber offset) const
    {
        uint16 itemOffsetPos = static_cast<uint16>(TdDataSize() + (offset - 1) * static_cast<uint16>(sizeof(ItemId)));
        StorageAssert(itemOffsetPos < (BLCKSZ - DataHeaderSize()));
        return reinterpret_cast<const ItemId *>(GetDataOffset() + itemOffsetPos);
    }

    inline char *GetItemIdArrayStartPtr()
    {
        return static_cast<char *>(
            static_cast<void *>(GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER)));
    }

    inline char *GetItemIdArrayEndPtr()
    {
        return static_cast<char *>(static_cast<void *>(this)) + m_header.m_lower;
    }

    inline void *GetRowData(ItemId *itemId)
    {
        if (STORAGE_VAR_NULL(itemId)) {
            ErrLog(DSTORE_PANIC, MODULE_PAGE, ErrMsg("ItemId is nullptr."));
        }
        StorageAssert(!itemId->IsNoStorage());
        StorageAssert(CheckItemIdSanity(itemId));
        return static_cast<void *>(PageHeaderPtr() + static_cast<int>(itemId->GetOffset()));
    }

    inline void *GetRowData(OffsetNumber offset)
    {
        ItemId* id = GetItemIdPtr(offset);
        StorageAssert(!id->IsNoStorage());
        return static_cast<void *>(PageHeaderPtr() + static_cast<int>(id->GetOffset()));
    }

    inline const void *GetRowData(OffsetNumber offset) const
    {
        const ItemId *id = GetItemIdPtr(offset);
        StorageAssert(!id->IsNoStorage());
        return static_cast<const void *>(PageHeaderPtr() + static_cast<int>(id->GetOffset()));
    }

    inline void RemoveLastItem()
    {
        m_header.m_lower -= sizeof(ItemId);
    }

    inline OffsetNumber GetMaxOffset() const
    {
        if (m_header.m_lower <= DataHeaderSize() + TdDataSize()) {
            return 0;
        }
        return static_cast<OffsetNumber>
            (m_header.m_lower - (DataHeaderSize() + TdDataSize())) / static_cast<uint16>(sizeof(ItemId));
    }

    inline uint16 TdDataSize() const
    {
        return dataHeader.tdCount * static_cast<uint16>(sizeof(TD));
    }
    inline uint16 DataHeaderSize() const
    {
        return dataHeader.headerOffset;
    }
    inline void SetDataHeaderSize(uint16 offset)
    {
        dataHeader.headerOffset = offset;
    }
    inline void SetHasFreeItemId()
    {
        m_header.m_flags |= PAGE_HAS_FREE_LINES;
    }
    inline bool HasFreeItemId() const
    {
        return (m_header.m_flags & PAGE_HAS_FREE_LINES) != 0;
    }
    inline void SetIsNewPage(bool isNewPage)
    {
        if (isNewPage) {
            m_header.m_flags |= PAGE_IS_NEW_PAGE;
        } else {
            m_header.m_flags &= ~PAGE_IS_NEW_PAGE;
        }
    }
    inline bool IsNewPage() const
    {
        return (m_header.m_flags & PAGE_IS_NEW_PAGE) != 0;
    }

    inline void SetIsCrExtend(bool isCrExtend)
    {
        if (isCrExtend) {
            m_header.m_flags |= PAGE_IS_EXTEND_CR_PAGE;
        } else {
            m_header.m_flags &= ~PAGE_IS_EXTEND_CR_PAGE;
        }
    }
    inline bool GetIsCrExtend() const
    {
        return (m_header.m_flags & PAGE_IS_EXTEND_CR_PAGE) != 0;
    }

    inline void SetSegmentCreateXid(Xid xid)
    {
        dataHeader.segmentCreateXid = xid;
    }

    inline Xid GetSegmentCreateXid() const
    {
        return dataHeader.segmentCreateXid;
    }

    inline void SetTuplePrunable(bool prunable)
    {
        if (prunable) {
            m_header.m_flags |= PAGE_TUPLE_PRUNABLE;
        } else {
            m_header.m_flags &= ~PAGE_TUPLE_PRUNABLE;
        }
    }

    inline bool HasPrunableTuple() const
    {
        return (m_header.m_flags & PAGE_TUPLE_PRUNABLE) != 0;
    }

    inline void SetItemPrunable(bool prunable)
    {
        if (prunable) {
            m_header.m_flags |= PAGE_ITEM_PRUNABLE;
        } else {
            m_header.m_flags &= ~PAGE_ITEM_PRUNABLE;
        }
    }

    inline bool HasPrunableItem() const
    {
        return (m_header.m_flags & PAGE_ITEM_PRUNABLE) != 0;
    }

    /* Compact tuples of given compactTuples to remove holes. */
    uint16 CompactTuples(ItemIdCompact compactItems, uint32 nItems);

    uint16 GetFreeItemId();
    RetStatus ExtendTd(TDAllocContext &context);
    void RecycleTd(uint8 numRecycled);
    enum class TdReuseState {
        TD_IGNORE,
        TD_RECYCLE_REUSE,
        TD_RECYCLE_UNUSED,
        TD_IS_IN_PROGRESS,
        TD_CONTENT_UPDATED
    };
    template<PageType page_type> TdReuseState TryReuseOneTdSlot(TDAllocContext &context, TdId tdId);
    template<PageType page_type> TdId TryReuseTdSlots(TDAllocContext &context);
    template<typename tup_type> void RefreshTupleTdStatus(int numSlot, const TdRecycleStatus *slots);
    TdId GetAvailableTd(int numSlot, const TdRecycleStatus *slots, bool canResetTd);

    template<PageType page_type> TdId AllocTd(TDAllocContext &context);

    template<typename tup_type> TdId GetTupleTdId(OffsetNumber offset);
    template<typename tup_type> TdId GetTupleTdId(ItemId *itemId);
    template<typename tup_type> TdId GetTupleLockerTdId(ItemId *itemId);
    template<typename tup_type> TdId GetTupleLockerTdId(OffsetNumber offset);
    template<typename tup_type> TupleTdStatus GetTupleTdStatus(OffsetNumber offset);
    template<typename tup_type> bool TestTupleTdStatus(OffsetNumber offset, TupleTdStatus status);
    template<typename tup_type> bool TestTupleTdStatus(ItemId *itemId, TupleTdStatus status);
    template<typename tup_type> bool TestTupleTdStatus(TupleTdStatus currentStatus, TupleTdStatus status);
    template<typename tup_type> void SetTupleTdStatus(OffsetNumber offset, TupleTdStatus status);

    template<typename tup_type, bool needFillCSN>
    bool JudgeTupCommitBeforeSpecCsn(
        PdbId pdbId, OffsetNumber offset, CommitSeqNo specCsn, bool &isDirty, CommitSeqNo *tupleCsn);
    void FillCsn(Transaction *transaction, UndoMgr *undoMgr);

    /* Rollback a transaction on page if transaction failed. Would call RollbackTdOneXidAsNeed for each undo record */
    RetStatus RollbackByXid(PdbId pdbId, Xid xid, BufMgrInterface *bufMgr, BufferDesc *bufferDesc,
                            BtreeUndoContext *btrUndoContext = nullptr);

    /**
     * Rollback an undo record on page for either aborted transaction or CR construction
     * @Param[IN] undoRec: the undo record we're going to rollback
     * @Param[IN] btrUndoContext: for Btree only. Caller must provide btrUndoContext for rollback on btree page.
     *                            for Heap page rollback, it should be a nullptr.
     * @Param[IN] tdOnPage: for Btree page only. The TD ID of td slot that is supposed to be restored may be different
     *                      with what we've recorded in the undoRec (if the page has been pruned and some of the TD
     *                      slots have been recycled and rearranged after the undoRed writen).
     *      nullptr: restore the td slot that retrived from undoRec by the TD ID recorded.
     *      a valid pointer: restore the exactly given td slot.
     **/
    RetStatus RollbackByUndoRec(UndoRecord *undoRec, BtreeUndoContext *btrUndoContext = nullptr,
                                TD *tdOnPage = nullptr);

    /* Construct cr page.
    *
    * only_use_once: If only use local buffer.
    * is_cr_page: If replay page is a cr page.
    * page_csn: the max csn remained in cr page.
    * expiration_csn: the min rollbacked csn from base page.
    *
    * Iff page_csn < snapshot.csn <= expiration_csn, cr page is visible
    * to the snapshot.
    */
    RetStatus ConstructCR(Transaction *transaction, CRContext *crCtx, BtreeUndoContext *btrUndoContext = nullptr);

    void DumpDataPageHeader(StringInfo str);
#ifdef DSTORE_USE_ASSERT_CHECKING
    bool CheckSanity();
    bool CheckItemIdSanity(ItemId *id) const;
#endif

    inline bool TestTupleTdStatus(TupleTdStatus currentStatus, TupleTdStatus status)
    {
        return currentStatus == status;
    }

protected:
    RetStatus RollbackTdOneXidForCRAsNeed(
        PdbId pdbId, TD &td, bool isCurrentXid, BtreeUndoContext *btrUndoContext, Snapshot snapshot);
    RetStatus RollbackTdOneXidAsNeed(
        PdbId pdbId, TD &td, BufMgrInterface *bufMgr, BufferDesc *bufferDesc, BtreeUndoContext *btrUndoContext);
    void ExtendCrPage();

private:
    template<PageType page_type> TdId DoAllocTd(TDAllocContext &context);
    RetStatus RollbackTdsForCR(binaryheap *tdHeap, Transaction *transaction, CRContext *crCtx,
                               BtreeUndoContext *btrUndoContext);
    template<typename tupleType> void UpdateTupleTdStatus(TdId tdId, TupleTdStatus src, TupleTdStatus dest);
};
STATIC_ASSERT_TRIVIAL(DataPage);

extern template void DataPage::SetTupleTdStatus<HeapDiskTuple>(OffsetNumber, TupleTdStatus);
extern template bool DataPage::TestTupleTdStatus<HeapDiskTuple>(OffsetNumber, TupleTdStatus);
}

#endif /* SRC_GAUSSKERNEL_INCLUDE_PAGE_STORAGE_DATA_PAGE_H */

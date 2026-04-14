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
 * dstore_heap_scan.cpp
 *
 *
 *
 * IDENTIFICATION
 *        include/page/dstore_heap_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_HEAP_PAGE_H
#define DSTORE_DSTORE_HEAP_PAGE_H

#include "page/dstore_data_page.h"
#include "page/dstore_fsm_page.h"
#include "page/dstore_itemid.h"
#include "common/dstore_common_utils.h"
#include "tuple/dstore_memheap_tuple.h"
#include "tuple/dstore_tuple_interface.h"

namespace DSTORE {

constexpr int HEAP_PAGE_MIN_FILL_FACTOR = 10;
constexpr int HEAP_PAGE_MAX_FILL_FACTOR = 100;

/*
 * For show any tuple to fetch tuple's all historic version.
 */
struct ShowAnyTupleContext {
    PdbId m_pdbId;
    ItemPointerData m_ctid;     /* which ctid is processed. */
    TD *m_td;                   /* td array for undo */
    TdId m_tdId;                /* undo start index of td[] */
    bool m_tupleInUndo;         /* whether tuple in CR page is fetched or not. */

    CommandId m_lastTupCid;     /* the last built tuple's deletion CommandId. */
    HeapTuple *m_lastTuple;     /* last built Tuple for this ctid，a newer version. */

public:
    static void CreateShowAnyTupleContext(PdbId pdbId, ShowAnyTupleContext *&showAnyTupContext);
    static void DestoryShowAnyTupleContext(ShowAnyTupleContext *&showAnyTupContext, bool skipLastTup);
    void CheckBeforeItemFetch(ItemPointerData ctid);
    bool IsItemFetchFinished();
    void GetLastTupleSnap(Xid &snapXid, CommandId &snapCid, bool &deleteXid);
    bool IsSnapMatchedTup(Xid snapXid, CommandId snapCid, bool deleteXid);
};

/*
 * HeapPage layout:
 *  PageHeader
 *  HeapPageHeader
 *  data:  TD array + ItemId array + tuple array
 */
struct HeapPage : public DataPage {
public:
    HeapPageHeader m_heapPageHeader;
    char m_data[BLCKSZ - HEAP_PAGE_DATA_OFFSET];

    inline void SetRecentDeadTupleMinCsn(uint64 csn)
    {
        m_heapPageHeader.recentDeadTupleMinCsn = csn;
    }

    inline uint64 GetRecentDeadTupleMinCsn() const
    {
        return m_heapPageHeader.recentDeadTupleMinCsn;
    }

    inline void SetFsmIndex(const FsmIndex fsmIndex)
    {
        m_heapPageHeader.fsmIndex = fsmIndex;
    }

    inline FsmIndex GetFsmIndex() const
    {
        return m_heapPageHeader.fsmIndex;
    }

    inline void AddPotentialDelItemSize(OffsetNumber offset)
    {
        ItemId *itemId = GetItemIdPtr(offset);
        m_heapPageHeader.potentialDelSize += itemId->GetLen();
    }

    inline void SetPotentialDelSize(uint16 delSize)
    {
        m_heapPageHeader.potentialDelSize = delSize;
    }

    inline uint16 GetPotentialDelSize() const
    {
        return m_heapPageHeader.potentialDelSize;
    }

    /* HeapPage init callback function use in GetNewPage() of DataSegment */
    static void InitHeapPage(struct BufferDesc *bufDesc, const PageId &selfPageId, const FsmIndex &fsmIndex);

    OffsetNumber AddTuple(const HeapDiskTuple *tuple, uint16 size,
                          OffsetNumber specifyOffset = INVALID_ITEM_OFFSET_NUMBER);
    void DelTuple(OffsetNumber offset);
    void UpdateTuple(OffsetNumber offset, HeapDiskTuple *diskTuple, uint32 diskTupleSize);
    void GetTuple(HeapTuple *tuple, OffsetNumber offset);
    HeapTuple *CopyTuple(OffsetNumber offset);
    HeapTuple *ExpandTupleSize(HeapTuple *oldTuple, uint32 newTupleSize) const;
    bool IsValidOffset(OffsetNumber offset);
    HeapTuple *GetVisibleLobTuple(PdbId pdbId, ItemPointerData &ctid);
    HeapTuple *GetVisibleTuple(PdbId pdbId, Transaction *txn, ItemPointerData &ctid, Snapshot snapshot, bool is_lob);
    RetStatus UndoHeap(UndoRecord *record, void *undoData = nullptr);
    RetStatus GetTupleXid(PdbId pdbId, Xid &xid, OffsetNumber offset);
    void PruneItems(const ItemIdDiff *itemIdDiff, uint16 nItems);
    uint16 ScanCompactableItems(ItemIdCompactData *compactItems, uint16 &notPrunedDelSize);
    void TryCompactTuples();
    HeapTuple *ShowAnyTupleFetch(ShowAnyTupleContext *anyTuple);

    inline TdId AllocTd(TDAllocContext &context)
    {
        return DataPage::AllocTd<PageType::HEAP_PAGE_TYPE>(context);
    }

    inline HeapDiskTuple *GetDiskTuple(OffsetNumber offset)
    {
        return static_cast<HeapDiskTuple *>(GetRowData(offset));
    }

    inline const HeapDiskTuple *GetDiskTuple(OffsetNumber offset) const
    {
        return static_cast<const HeapDiskTuple *>(GetRowData(offset));
    }

    inline TdId GetTupleTdId(OffsetNumber offset)
    {
        return DataPage::GetTupleTdId<HeapDiskTuple>(offset);
    }

    inline TdId GetTupleLockerTdId(OffsetNumber offset)
    {
        return DataPage::GetTupleLockerTdId<HeapDiskTuple>(offset);
    }

    inline bool TestTupleTdStatus(TupleTdStatus currentStatus, TupleTdStatus status)
    {
        return currentStatus == status;
    }

    inline bool TestTupleTdStatus(OffsetNumber offset, TupleTdStatus status)
    {
        return DataPage::TestTupleTdStatus<HeapDiskTuple>(offset, status);
    }

    inline void SetTupleTdStatus(OffsetNumber offset, TupleTdStatus status)
    {
        DataPage::SetTupleTdStatus<HeapDiskTuple>(offset, status);
    }
    template <bool needFillCSN>
    bool JudgeTupCommitBeforeSpecCsn(
        PdbId pdbId, OffsetNumber offset, CommitSeqNo specCsn, bool &isDirty, CommitSeqNo *tupleCsn = nullptr)
    {
        return DataPage::JudgeTupCommitBeforeSpecCsn<HeapDiskTuple, needFillCSN>(
            pdbId, offset, specCsn, isDirty, tupleCsn);
    }

    inline static uint32 MaxPossibleTupleSpace()
    {
        return (BLCKSZ - (MAXALIGN(HEAP_PAGE_DATA_OFFSET + MIN_TD_COUNT * sizeof(TD) +
                                   sizeof(ItemId))));
    }

    inline static uint32 MaxDefaultTupleSpace()
    {
        return (BLCKSZ - (MAXALIGN(HEAP_PAGE_DATA_OFFSET +
                                   DEFAULT_TD_COUNT * sizeof(TD) + sizeof(ItemId))));
    }

    inline uint32 MaxTupNumPerPage() const
    {
        return (BLCKSZ -
                   (MAXALIGN(HEAP_PAGE_DATA_OFFSET + (GetTdCount() * sizeof(TD))))) /
               (HEAP_DISK_TUP_HEADER_SIZE + sizeof(ItemId) + 1);
    }

    static bool TupBiggerThanPage(HeapTuple *tuple);

    bool IsDiskTupleDeleted(OffsetNumber offset);
    char *Dump(bool showTupleData = false);
    void PrevDumpPage(char *page);
#ifdef DSTORE_USE_ASSERT_CHECKING
    bool CheckHeapPageSanity();
#endif

private:
    HeapTuple *ConstructTupleFromUndoDelete(UndoRecord *record);
    HeapTuple *ConstructTupleFromInplaceUpdate(UndoRecord *record, HeapTuple *tuple);
    HeapTuple *ConstructTupleFromSamePageUpdate(UndoRecord *record);
    HeapTuple *ConstructTupleFromAnotherPageUpdateOldPage(UndoRecord *record);
    void ConstructCrTupleFromUndo(UndoRecord *record, HeapTuple **resTuple);
    RetStatus ConstructCrTuple(
        PdbId pdbId, Transaction *txn, ItemPointerData &ctid, TdId tdId, HeapTuple **resTuple, Snapshot snapshot);
    bool ConstructHistoricTuple(ShowAnyTupleContext *anyTuple, HeapTuple **resTuple);
    /* We need get undo data from undoDataRec when undo record append failed. */
    RetStatus ExecuteUndoForBatchInsert(UndoRecord *record, void* undoDataRec);
    void ExecuteUndoForInsert(UndoRecord *record);
    RetStatus ExecuteUndoForDelete(UndoRecord *record, void* undoDataRec);
    RetStatus ExecuteUndoForInplaceUpdate(UndoRecord *record, void* undoDataRec);
    RetStatus ExecuteUndoForSamePageAppendUpdate(UndoRecord *record, void* undoDataRec);
    RetStatus ExecuteUndoForOldPageAppendUpdate(UndoRecord *record, void* undoDataRec);
    void ExecuteUndoForNewPageAppendUpdate(UndoRecord *record);
    RetStatus CompactCRPageIfFreeSpaceLessThan(uint32 needSpace);
};
STATIC_ASSERT_TRIVIAL(HeapPage);

enum HeapPageSpaceStatus : uint8 {
    HEAP_PAGE_HAS_ENOUGH_SPACE = 0,
    HEAP_PAGE_NO_SPACE_AFTER_PRUNE,
    HEAP_PAGE_NO_SPACE_INVALID_TD
};

constexpr int MAX_HEAP_TUPLE_FIXED_SIZE = sizeof(ItemId) + sizeof(HeapDiskTuple);
constexpr int MAX_POSSIBLE_HEAP_TUPLES_PER_PAGE = static_cast<int>((((BLCKSZ - DATA_PAGE_HEADER_SIZE) - MIN_TD_COUNT *
    sizeof(TD)) / MAX_HEAP_TUPLE_FIXED_SIZE));

static_assert(sizeof(HeapPage) == BLCKSZ, "Heap page size must be equal to BLCKSZ");

constexpr int MAX_ACTIVE_HEAP_TUPLES_PER_PAGE = static_cast<int>((BLCKSZ -
			    (MAXALIGN(HEAP_PAGE_DATA_OFFSET + (DEFAULT_TD_COUNT * sizeof(TD))))) / (sizeof(ItemId)));
static_assert(MAX_ACTIVE_HEAP_TUPLES_PER_PAGE == TupleInterface::MAX_TUPLE_NUM_PER_PAGE,
		    "Max heap tuple numbers per page must be equal to MAX_TUPLE_NUM_PER_PAGE");

}  // namespace DSTORE

#endif  // DSTORE_STORAGE_HEAP_PAGE_H

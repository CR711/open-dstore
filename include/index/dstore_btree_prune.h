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
 * dstore_btree_prune.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree_prune.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_PRUNE_H
#define SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_PRUNE_H

#include "index/dstore_btree.h"

namespace DSTORE {

using BtreeTdTransferInfo = BtreeTdSplitInfo;

/*
 * BtreePagePrune: first stage of Btree page prune
 *
 * 1. Scan all TDs and tuples to check if the page is safe to prune
 * 2. Compactify live TDs and tuples to make more free space
 * 3. Mark leaf page hanging and add it into FSM if page is going to be empty after an ongoing deletion
 */
class BtreePagePrune : public Btree {
public:
    BtreePagePrune() = delete;

    BtreePagePrune(StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scankeyInfo,
                   BufferDesc *pageBuf); /* General constructor for prune */

    explicit BtreePagePrune(PdbId pdbId, BtrPage *page); /* Only for redo */
    ~BtreePagePrune();

    inline PdbId GetPdbId() override
    {
        return m_pdbId;
    }
    /*
     * Prune
     *
     * Prune a page before tuple inserting to avoid splitting as much as possible.
     */
    RetStatus Prune(OffsetNumber internalDelOffset = INVALID_ITEM_OFFSET_NUMBER,
                    OffsetNumber leafDelOffset = INVALID_ITEM_OFFSET_NUMBER);

    /*
     * CompactTdSpace
     *
     * Compactify TD space by rearranging active TDs slot by slot in sequential order of active_td_info.
     * Slots of frozen tds will be overwritten.
     *
     * Note that num_td_slots is always greater than num_active_tds since we need to make sure that the page still has
     * sufficient TD slots to put all active TDs in.
     * Only reset the last frozen TD slots for further using without moving the active TDs into the frontal slots.
     *
     * We do not update td_count here since the line-pointer offset computing is depending on the original td_count. We
     * are going to update td_count after line-pointer compaction.
     */
    void CompactTdSpace();

    /*
     * CompactItems
     *
     * Remove prunable ItemIds (line-pointers) and compactify them in sequential order.
     *
     * Contrary with heap, index's line-pointers are arranged in a specific order. Thus, they should be removed from a
     * page after deleted instead of reused to keep this order.
     *
     * We collect the information saved in line-pointers (into ItemIdCompact, which contains new offset-number of
     * line-pointer, tuple offset and aligned length) to make sure that we can still find the correct tuple after a
     * line-pointer compaction. Note that comparing with only collecting pointers to itemIds (line-pointers), it is
     * necessary to copy the information into a mediate structure since the actual address of itemIds will change
     * after compaction. We need to have a map of the new line-pointer offset-numbers and their corresponding tuples
     * to update the new offset of tuple after the tuple compaction.
     *
     * Pages's td_count and lower will be updated
     *
     * Results: ItemIdCompactData array containing the information to find the live tuples.
     */
    void CompactItems();

    /*
     * CompactTuples
     *
     * Remove prunable tuples and compactify them in sequential order.
     *
     * Compute and update the new upper value of the page.
     */
    void CompactTuples();

    /*
     * IsPageEmpty
     *
     * Check if the page if empty. Return true if the page is empty.
     * when need_prune is set true, clear the page after checking.
     */
    bool IsPageEmpty(bool needPrune = false);

    void Init();

    uint16 GetFixedTdCount() const
    {
        return m_fixedTdCount;
    }

    void SetFixedTdCount(uint8 tdCount)
    {
        m_fixedTdCount = tdCount;
    }

    uint16 GetTupleCount() const
    {
        return m_numTotalTuples;
    }

    void SetTupleCount(uint16 numTuples)
    {
        m_numTotalTuples = numTuples;
    }

    uint16 GetLiveTupleCount() const
    {
        return m_numLiveTuples;
    }

    RetStatus SetLiveTupleCountAndAllocLiveItems(uint16 numLiveTuples)
    {
        m_numLiveTuples = numLiveTuples;
        m_liveItems = static_cast<ItemIdCompact>(DstorePalloc(sizeof(ItemIdCompactData) * m_numLiveTuples));
        if (STORAGE_VAR_NULL(m_liveItems)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc failed when alloc for ItemIdCompactData."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            return DSTORE_FAIL;
        }
        return DSTORE_SUCC;
    }

    void SetTupleLiveStatus(bool *isTupleLive)
    {
        m_isTupleLive = isTupleLive;
    }

    bool IsPagePrunable() const
    {
        return m_isPagePrunable;
    }

    /*
     * GetCurrMaxCsn
     *
     * Returns the max csn seen after a call to ScanLiveTuples(checkCsn = true)
     *
     * Result: INVALID_CSN if a valid call to ScanLiveTuples was made
     *         maxCsn seen on the page.
     */
    CommitSeqNo GetCurrMaxCsn() const
    {
        return m_currMaxCsn;
    }

    uint16 GetLeafDelOffset() const
    {
        return m_leafDelOffset;
    }

protected:
    BtreePagePayload *GetPagePayload()
    {
        return &m_pagePayload;
    }

    bool *GetTupleLiveStatus()
    {
        return m_isTupleLive;
    }

    ItemIdCompact GetLiveItemsInfo()
    {
        return m_liveItems;
    }
    void GenerateWalForPrune(void *redoPtr, uint32 redoDataSize);

    /* Page info */
    BtreePagePayload m_pagePayload; /* BtreePagePayload contains leaf page we're likely to prune */

    CommitSeqNo m_currMaxCsn;
    bool m_isPagePrunable;
    bool m_prunedAll;

    /* TD info */
    TDAllocContext m_tdContext;
    uint8 m_fixedTdCount;
    uint8 m_origTdCount;
    bool m_canCompactTds{true}; /* false when in-progress TDs exist, blocks TD compaction but allows tuple prune */

    /*
     * Valid when we are deleting an indextuple of an internal page.
     * We should prune the indextuple whose offset is m_internalDelOffset.
     */
    uint16 m_internalDelOffset;

    /*
     * Valid when we are deleting an indextuple of a leaf page.
     * When we are deleting the indextuple whose offset is m_leafDelOffset, we may prune the leaf page if we are
     * failed to alloc td slot. Then the offset of the the indextuple to be deleted may be changed.
     * m_leafDelOffset will be updated to the new offset of the target deletion tuple after pruned.
     */
    uint16 m_leafDelOffset;

    /* Tuple info */
    uint16 m_numTotalTuples;
    uint16 m_numLiveTuples;
    bool *m_isTupleLive;

    /* A map of new itemId offset-number & tuple. Created after CompactItems */
    ItemIdCompact m_liveItems; /* number of live items must be equal to number of live tuples for index */

    PdbId m_pdbId;

private:
    /*
     * ScanActiveTds
     *
     * Scans all TD slots in sequential order on the page and collects active TDs into active_td_info.
     * active_td_info contains the current td_id and a new td_id that will be used after a compaction of TD space on the
     * page if needed. It also saves a pointer of TD for further compaction.
     *
     * A TD's "is_frozen" flag will be set to 1 if the corresponding transaction is committed and is visible to all
     * after scanning during this process. All relevant Tuples' td_id will also be set to TD_ID_FROZEN for later reusing
     *
     * Results: The fixed number of TD slots. Will be equal to the original td_count if there's no need to compact.
     */
    RetStatus ScanActiveTds();

    /*
     * ScanLiveTuples
     *
     * Scans all tuples in sequential order on the page and marks live tuples.
     *
     * m_isPagePrunable will be set to false and the scanning will be stopped immediately if any open transaction
     * found while checking tuple status. We should never prune a page that contains open transactions.
     *
     * Results: A bool array with marks of all tuples in sequential order.
     *          true: tuple is live; false: tuple has been deleted and can be pruned
     */
    RetStatus ScanLiveTuples(bool checkCsn = false);

    /*
     * IsTuplePrunable
     *
     * Check the status of a single tuple to determine if it can be pruned later.
     *
     * A tuple is prunable if
     *      1. item_id has a ITEM_ID_DEAD or ITEM_ID_UNUSED flag
     *      2. the tuple has been marked deleted && transaction is committed
     * *m_isPagePrunable will be set to false if any open transaction found.
     *
     * Result: true if a tuple has been deleted and can be pruned.
     *         false if a tuple is live.
     */
    bool IsTuplePrunable(OffsetNumber offnum, bool checkCsn);

    /*
     * SetCurrMaxCsn
     *
     * Sets the max csn seen after a call to ScanLiveTuples(checkCsn = true)
     */
    void SetCurrMaxCsn(CommitSeqNo maxCsn)
    {
        if (m_currMaxCsn == INVALID_CSN) {
            m_currMaxCsn = maxCsn;
        } else {
            m_currMaxCsn = DstoreMax(m_currMaxCsn, maxCsn);
        }
    }

    ScanKey m_scankeyInfo;
};

class BtreePageGPIPrune : public BtreePagePrune {
public:
    BtreePageGPIPrune() = delete;
    BtreePageGPIPrune(StorageRelation indexRel, BufferDesc *pageBuf, IndexInfo *indexInfo,
        GPIPartOidCheckInfo *gpiCheckInfo);
    ~BtreePageGPIPrune() = default;

    /*
     * Prune
     *
     * Prune a page before tuple inserting to avoid splitting as much as possible.
     */
    RetStatus GPIPrune(bool &needReleaseBuf);
    bool IsPartVisible(IndexTuple *tuple);
    bool GPITupleIsPrunable(OffsetNumber offnum);

protected:
    GPIPartOidCheckInfo *m_gpiCheckInfo;
};

}  // namespace DSTORE

#endif  // SRC_GAUSSKERNEL_INCLUDE_INDEX_STORAGE_BTREE_PRUNE_H

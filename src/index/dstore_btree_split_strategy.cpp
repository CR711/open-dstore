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
 * dstore_btree_split_strategy.cpp
 *
 * IDENTIFICATION
 *        dstore/src/index/dstore_btree_split_strategy.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "index/dstore_btree_split_strategy.h"
#include "index/dstore_btree_perf_unit.h"

namespace DSTORE {

/* Qsort-style comparator used by SortCandidateSplitPoints() */
static int SplitPointCompare(const void *arg1, const void *arg2)
{
    auto splitDelta1 = static_cast<const SplitPoint *>(arg1)->curDelta;
    auto splitDelta2 = static_cast<const SplitPoint *>(arg2)->curDelta;

    if (splitDelta1 > splitDelta2) {
        return 1;
    }
    if (splitDelta1 < splitDelta2) {
        return -1;
    }
    return 0;
}

/* Sort split point array using given fillfactor */
static void SortCandidateSplitPoints(uint16 nSplitPoints,
    SplitPoint *splitPoints, bool useFillFactorMult, double fillFactorMult)
{
    for (int i = 0; i < nSplitPoints; i++) {
        SplitPoint *split = splitPoints + i;
        double delta;

        if (useFillFactorMult) {
            delta = fillFactorMult * split->leftPageFreeSpace -
                    (1.0 - fillFactorMult) * split->rightPageFreeSpace;
        } else {
            delta = split->leftPageFreeSpace - split->rightPageFreeSpace;
        }

        if (delta < 0) {
            delta = -delta;
        }

        /* Save delta */
        split->curDelta = static_cast<int16>(delta);
    }

    qsort(splitPoints, nSplitPoints, sizeof(SplitPoint), SplitPointCompare);
}

/*
 * CompEqualAttrNum - get num of equal attr between tuples.
 * Fast but not always right, however, good enough for page split loc selection
 */
static uint32 CompEqualAttrNum(IndexInfo *indexInfo, IndexTuple* lastLeft, IndexTuple* firstRight)
{
    TupleDesc itupDesc = indexInfo->attributes;
    int keyAttrNum = indexInfo->indexKeyAttrsNum;
    uint32 euqalAttrNum = 1;
    for (int attnum = 1; attnum <= keyAttrNum; attnum++) {
        bool isNull1;
        bool isNull2;

        Datum datum1 = lastLeft->GetAttr(attnum, itupDesc, &isNull1);
        Datum datum2 = firstRight->GetAttr(attnum, itupDesc, &isNull2);
        Form_pg_attribute att = DstoreTupleDescAttr(itupDesc, attnum - 1);

        if (isNull1 != isNull2) {
            break;
        }
        if (!isNull1 && !datumImageEq(datum1, datum2, att->attbyval, att->attlen)) {
            break;
        }
        euqalAttrNum++;
    }

    return euqalAttrNum;
}

BtreeSplitStrategy::BtreeSplitStrategy(StorageRelation indexRel, IndexInfo *indexInfo, BtrPage *targetPage,
    OffsetNumber newItemOff, IndexTuple *newItem) : Btree(indexRel, indexInfo),
    m_targetPage(targetPage), m_newItemOff(newItemOff), m_newItem(newItem),
    m_newItemSize(newItem->GetSize() + sizeof(ItemId)), m_leftPageUsableSpace(0), m_rightPageUsableSpace(0),
    m_oldDataItemsTotalSpace(0), m_isLeaf(targetPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE)),
    m_isRightMost(targetPage->GetLinkAndStatus()->IsRightmost()), m_maxOffsetNumber(targetPage->GetMaxOffset()),
    m_maxSplitPointsNum(m_maxOffsetNumber), m_nSplitPoints(0), m_minFirstRightSize(BLCKSZ),
    m_splitPoints(nullptr)
{
    for (OffsetNumber offnum = m_targetPage->GetLinkAndStatus()->GetFirstDataOffset();
         offnum <= m_maxOffsetNumber; offnum = OffsetNumberNext(offnum)) {
        m_oldDataItemsTotalSpace += static_cast<int>(m_targetPage->GetItemIdPtr(offnum)->GetLen() + sizeof(ItemId));
    }
}

BtreeSplitStrategy::~BtreeSplitStrategy()
{
    DstorePfreeExt(m_splitPoints);
    m_indexRel = nullptr;
    m_newItem = nullptr;
    m_targetPage = nullptr;
}

RetStatus BtreeSplitStrategy::InitSplitPoint(uint8 addedTdCount)
{
    m_leftPageUsableSpace = static_cast<int>(BLCKSZ - (m_targetPage->DataHeaderSize() +
                                                       MAXALIGN(sizeof(BtrPageLinkAndStatus)) +
                                                       m_targetPage->TdDataSize() + addedTdCount * sizeof(TD)));
    m_rightPageUsableSpace = m_leftPageUsableSpace;

    /* The right page will have the same high key as the old page */
    if (!m_isRightMost) {
        ItemId *itemid = m_targetPage->GetItemIdPtr(BTREE_PAGE_HIKEY);
        m_rightPageUsableSpace -= static_cast<int>(itemid->GetLen() + sizeof(ItemId));
    }
    m_splitPoints = static_cast<SplitPoint*>(DstorePalloc(sizeof(SplitPoint) * m_maxSplitPointsNum));
    if (unlikely(m_splitPoints == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when BtreeSplitStrategy."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

/*
 * Find an appropriate place to split a page.
 *
 * 1. equalize the free space that will be on each split page
 * 2. try to leave newleft page full for the rightmost origin page
 */
OffsetNumber BtreeSplitStrategy::FindSplitLoc(bool *newItemOnLeft)
{
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_findSplitLocLatency);

    if (STORAGE_FUNC_FAIL(ScanThroughPage())) {
        return INVALID_ITEM_OFFSET_NUMBER;
    }

    uint32 perfectPenalty = UINT_MAX;
    FindSplitStrategy strategy = SetStrategy(&perfectPenalty);
    switch (strategy) {
        case FindSplitStrategy::SPLIT_AFTER_NEWITEM: {
            BtrSplitPointAfterNewItemSelector afterNewItemSelector(m_indexInfo, this);
            return afterNewItemSelector.GetBestSplitLoc(perfectPenalty, newItemOnLeft);
        }
        case FindSplitStrategy::SPLIT_SINGLE_VALUE: {
            StorageAssert(m_isLeaf);
            BtrSplitPointSingleValuesSelector singleValuesSelector(m_indexInfo, this);
            return singleValuesSelector.GetBestSplitLoc(perfectPenalty, newItemOnLeft);
        }
        case FindSplitStrategy::SPLIT_MANY_DUPLICATES: {
            StorageAssert(m_isLeaf);
            /* Shouldn't try to truncate away extra user attributes */
            StorageAssert(perfectPenalty == m_indexInfo->indexKeyAttrsNum);
            BtrSplitPointManyDuplicatesSelector manyDuplicatesSelector(m_indexInfo, this);
            return manyDuplicatesSelector.GetBestSplitLoc(perfectPenalty, newItemOnLeft);
        }
        case FindSplitStrategy::SPLIT_DEFAULT:
        default: {
            BtrSplitPointSelectorBase selectorBase(m_indexInfo, this);
            return selectorBase.GetBestSplitLoc(perfectPenalty, newItemOnLeft);
        }
    }
}

/*
 * Record a particular split point between two tuples (possibly the new item) on page
 */
void BtreeSplitStrategy::RecordSplitLoc(OffsetNumber firstRightOff,
                                        bool newItemOnLeft, int oldDataItemSpaceToLeft, uint32 firstRightOriginSize)
{
    if (firstRightOff == m_targetPage->GetLinkAndStatus()->GetFirstDataOffset()) {
        /* insufficient and problematic for perfect penalty computation */
        return;
    }
    bool newitemIsFirstright = (firstRightOff == m_newItemOff && !newItemOnLeft);

    uint32 firstRightSize;
    if (newitemIsFirstright) {
        firstRightSize = m_newItemSize;
    } else {
        firstRightSize = firstRightOriginSize;
    }

    /* Account for all the old tuples */
    int leftFree = static_cast<int>(m_leftPageUsableSpace - oldDataItemSpaceToLeft);
    int rightFree = static_cast<int>(m_rightPageUsableSpace - (m_oldDataItemsTotalSpace - oldDataItemSpaceToLeft));

    if (m_isLeaf) {
        leftFree -= static_cast<int>(firstRightSize + sizeof(ItemPointerData));
    } else {
        leftFree -= static_cast<int>(firstRightSize);
        /*
         * If we are not on the leaf level, we will be able to discard the key
         * data from the first item that winds up on the right page.
         */
        rightFree +=
            static_cast<int>(firstRightSize) - static_cast<int>(sizeof(IndexTuple) + sizeof(ItemId));
    }

    /* account for the new item */
    if (newItemOnLeft) {
        leftFree -= static_cast<int>(m_newItemSize);
    } else {
        rightFree -= static_cast<int>(m_newItemSize);
    }

    /* Record split if legal */
    if (leftFree >= 0 && rightFree >= 0) {
        StorageAssert(m_nSplitPoints <= m_maxSplitPointsNum);

        /* Determine smallest firstright tuple size among legal m_splitPoints */
        m_minFirstRightSize = DstoreMin(m_minFirstRightSize, firstRightSize);

        m_splitPoints[m_nSplitPoints].curDelta = 0;
        m_splitPoints[m_nSplitPoints].leftPageFreeSpace = leftFree;
        m_splitPoints[m_nSplitPoints].rightPageFreeSpace = rightFree;
        m_splitPoints[m_nSplitPoints].firstRightOff = firstRightOff;
        m_splitPoints[m_nSplitPoints].newItemOnLeft = newItemOnLeft;
        m_nSplitPoints++;
    }
}

/*
 * Scan tuples in page, and record legal split points
 */
RetStatus BtreeSplitStrategy::ScanThroughPage()
{
    int oldDataItemsToLeft = 0;
    ItemId* itemid;

    for (OffsetNumber offnum = m_targetPage->GetLinkAndStatus()->GetFirstDataOffset();
         offnum <= m_maxOffsetNumber; offnum = OffsetNumberNext(offnum)) {
        uint32 itemsz;

        itemid = m_targetPage->GetItemIdPtr(offnum);
        itemsz = itemid->GetLen() + sizeof(ItemId);

        if (offnum < m_newItemOff) {
            RecordSplitLoc(offnum, false, oldDataItemsToLeft, itemsz);
        } else if (offnum > m_newItemOff) {
            RecordSplitLoc(offnum, true, oldDataItemsToLeft, itemsz);
        } else {
            RecordSplitLoc(offnum, false, oldDataItemsToLeft, itemsz);
            RecordSplitLoc(offnum, true, oldDataItemsToLeft, itemsz);
        }
        oldDataItemsToLeft += static_cast<int>(itemsz);
    }
    StorageAssert(oldDataItemsToLeft == m_oldDataItemsTotalSpace);
    if (m_newItemOff > m_maxOffsetNumber) {
        RecordSplitLoc(m_newItemOff, false, m_oldDataItemsTotalSpace, 0);
    }

    if (m_nSplitPoints == 0) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

double BtreeSplitStrategy::GetFillFactor(bool *useFillFactor) const
{
    if (!m_isLeaf) {
        /* fillFactorMult only used on rightmost page */
        *useFillFactor = m_isRightMost;
        return BTREE_NONLEAF_FILLFACTOR / static_cast<double>(PERCENTAGE_DIVIDER);
    } else if (m_isRightMost) {
        /* Rightmost leaf page --  fillFactorMult always used */
        *useFillFactor = true;
        return GetBtreeSmgr()->GetFillFactor() / static_cast<double>(PERCENTAGE_DIVIDER);
    } else {
        /* Non-rightmost leaf page: use fill factor to leave headroom for subsequent inserts
         * and dead tuple buffering, reducing unnecessary splits under update-heavy workloads. */
        *useFillFactor = true;
        return BTREE_NONLEAF_FILLFACTOR / static_cast<double>(PERCENTAGE_DIVIDER);
    }
}

/*
 * "Split immediately after the would-be original page offset"
 */
bool BtreeSplitStrategy::SplitAfterNewItemOff()
{
    ItemId* itemid;
    IndexTuple* tup;
    uint32 keepnatts;

    uint16 nkeyatts = m_indexInfo->indexKeyAttrsNum;

    /* Single key indexes not considered here */
    if (nkeyatts == 1) {
        return false;
    }

    /* Ascending insertion pattern never inferred when new item is first */
    if (m_newItemOff == BTREE_PAGE_FIRSTKEY) {
        return false;
    }

    /*
     * Only apply optimization on pages with equisized tuples, since ordinal keys are likely to be fixed-width.
     * but we don't care items on the right side of newItem... The length of Hikey might be different as well
     */
    uint32 existingTupleLen = static_cast<uint32>(m_targetPage->GetItemIdPtr(FIRST_ITEM_OFFSET_NUMBER)
        ->GetOffset() - m_targetPage->GetItemIdPtr(m_newItemOff)->GetOffset());
    if (m_newItemSize * OffsetNumberPrev(m_newItemOff) != existingTupleLen) {
        return false;
    }

    /*
     * Avoid applying optimization when tuples are wider than a tuple
     * consisting of two non-NULL int8/int64 attributes (or four non-NULL
     * int32/int32 attributes)
     */
    uint32 twoIntAttrTupleSize = static_cast<uint32>(sizeof(IndexTuple) + sizeof(int64) * 2 + sizeof(ItemId));
    if (m_newItemSize > twoIntAttrTupleSize) {
        return false;
    }

    /*
     * At least the first attribute's value must be equal to the corresponding
     * value in previous tuple to apply optimization.  New item cannot be a
     * duplicate, either.
     */
    if (m_newItemOff > m_maxOffsetNumber) {
        itemid = m_targetPage->GetItemIdPtr(m_maxOffsetNumber);
        tup = m_targetPage->GetIndexTuple(itemid);
        keepnatts = CompEqualAttrNum(m_indexInfo, tup, m_newItem);
        if (keepnatts > 1U && keepnatts <= nkeyatts) {
            return true;
        }
        return false;
    }

    itemid = m_targetPage->GetItemIdPtr(OffsetNumberPrev(m_newItemOff));
    tup = m_targetPage->GetIndexTuple(itemid);

    BlockNumber lowblk = tup->GetHeapCtid().GetBlockNum();
    BlockNumber highblk = m_newItem->GetHeapCtid().GetBlockNum();
    bool isAdjacent = false;
    /* Make optimistic assumption of adjacency when heap blocks match */
    if (lowblk == highblk) {
        isAdjacent = true;
    }
    /* When heap block one up, second offset should be FirstOffsetNumber */
    if (lowblk + 1 == highblk && m_newItem->GetHeapCtid().GetOffset() == FIRST_ITEM_OFFSET_NUMBER) {
        isAdjacent = true;
    }
    if (!isAdjacent) {
        return false;
    }

    /* Check same conditions as rightmost item case, too */
    keepnatts = CompEqualAttrNum(m_indexInfo, tup, m_newItem);
    if (keepnatts > 1U && keepnatts <= nkeyatts) {
        return true;
    }

    return false;
}

FindSplitStrategy BtreeSplitStrategy::SetStrategy(uint32 *perfectPenalty)
{
    /*
     * SplitLoc = InsertLoc
     * Localized monotonically increasing insertions into a composite index.
     */
    if (m_isLeaf && !m_isRightMost && SplitAfterNewItemOff()) {
        return FindSplitStrategy::SPLIT_AFTER_NEWITEM;
    }

    bool useFillFactorMult;
    double fillFactorMult = GetFillFactor(&useFillFactorMult);
    SortCandidateSplitPoints(m_nSplitPoints, m_splitPoints, useFillFactorMult, fillFactorMult);

    *perfectPenalty = GetPerfectPenalty();

    /* Default strategy worked out (always works out with internal page). */
    if (!m_isLeaf) {
        return FindSplitStrategy::SPLIT_DEFAULT;
    }

    /*
     * If page (including new item) has many duplicates but is not entirely
     * full of duplicates, a many duplicates strategy split will be performed.
     *
     * If page is entirely full of duplicates, a single value strategy split
     * will be performed, or rightmost.
     */
    if (*perfectPenalty <= m_indexInfo->indexKeyAttrsNum) {
        *perfectPenalty = m_indexInfo->indexKeyAttrsNum;
        return FindSplitStrategy::SPLIT_MANY_DUPLICATES;
    }

    if (m_isRightMost) {
        return FindSplitStrategy::SPLIT_SINGLE_VALUE;
    }

    ItemId* itemid = m_targetPage->GetItemIdPtr(BTREE_PAGE_HIKEY);
    IndexTuple* hikey = m_targetPage->GetIndexTuple(itemid);
    *perfectPenalty = CompEqualAttrNum(m_indexInfo, hikey, m_newItem);
    if (*perfectPenalty <= m_indexInfo->indexKeyAttrsNum) {
        return FindSplitStrategy::SPLIT_SINGLE_VALUE;
    } else {
        return FindSplitStrategy::SPLIT_DEFAULT;
    }
}

/*
 * Use minRightItemSize for non-leaf page, distinguishing key num for leaf page
 */
uint32 BtreeSplitStrategy::GetPerfectPenalty()
{
    if (!m_isLeaf) {
        return m_minFirstRightSize;
    } else {
        SplitPoint leftpage = m_splitPoints[0];
        SplitPoint rightpage = m_splitPoints[m_nSplitPoints - 1];
        IndexTuple *leftmost;
        IndexTuple *rightmost;

        if (leftpage.newItemOnLeft && leftpage.firstRightOff == m_newItemOff) {
            leftmost = m_newItem;
        } else {
            leftmost = m_targetPage->GetIndexTuple(OffsetNumberPrev(leftpage.firstRightOff));
        }
        if (!rightpage.newItemOnLeft && rightpage.firstRightOff == m_newItemOff) {
            rightmost = m_newItem;
        } else {
            rightmost = m_targetPage->GetIndexTuple(rightpage.firstRightOff);
        }

        return CompEqualAttrNum(m_indexInfo, leftmost, rightmost);
    }
}

/*
 * Get penalty for caller's candidate split point.
 */
uint32 BtrSplitPointSelectorBase::GetSplitPenalty(SplitPoint *split)
{
    IndexTuple *lastleft;
    IndexTuple *firstright;

    if (!m_targetPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE)) {
        if (!split->newItemOnLeft && split->firstRightOff == m_newItemOff) {
            return m_newItemSize;
        }
        ItemId* itemid = m_targetPage->GetItemIdPtr(split->firstRightOff);
        return itemid->GetLen() + static_cast<uint32>(sizeof(ItemId));
    }

    if (split->newItemOnLeft && split->firstRightOff == m_newItemOff) {
        lastleft = m_newItem;
    } else {
        ItemId* itemid = m_targetPage->GetItemIdPtr(OffsetNumberPrev(split->firstRightOff));
        lastleft = m_targetPage->GetIndexTuple(itemid);
    }

    if (!split->newItemOnLeft && split->firstRightOff == m_newItemOff) {
        firstright = m_newItem;
    } else {
        ItemId* itemid = m_targetPage->GetItemIdPtr(split->firstRightOff);
        firstright = m_targetPage->GetIndexTuple(itemid);
    }

    return CompEqualAttrNum(m_indexInfo, lastleft, firstright);
}

/*
 * Find the "best" split point among candidate split points.
 */
OffsetNumber BtrSplitPointSelectorBase::GetBestSplitLoc(uint32 perfectpenalty, bool *newitemonleft)
{
    SplitPoint *finalPoint;
    uint32 bestPenalty = UINT_MAX;
    uint32 lowSplit = 0;
    for (uint32 i = lowSplit; i < m_nSplitPoints; i++) {
        uint32 penalty = GetSplitPenalty(m_splitPoints + i);
        if (penalty < bestPenalty) {
            bestPenalty = penalty;
            lowSplit = i;
        }

        if (penalty <= perfectpenalty) {
            break;
        }
    }

    finalPoint = &m_splitPoints[lowSplit];

    *newitemonleft = finalPoint->newItemOnLeft;
    return finalPoint->firstRightOff;
}

BtrSplitPointSelectorBase::BtrSplitPointSelectorBase(IndexInfo *indexInfo, BtreeSplitStrategy *strategy)
    : m_indexInfo(indexInfo), m_targetPage(strategy->m_targetPage), m_newItem(strategy->m_newItem),
    m_newItemSize(strategy->m_newItemSize), m_newItemOff(strategy->m_newItemOff),
    m_nSplitPoints(strategy->m_nSplitPoints), m_splitPoints(strategy->m_splitPoints)
{
}

BtrSplitPointSelectorBase::~BtrSplitPointSelectorBase()
{
    m_splitPoints = nullptr;
    m_indexInfo = nullptr;
    m_targetPage = nullptr;
    m_newItem = nullptr;
}

BtrSplitPointAfterNewItemSelector::BtrSplitPointAfterNewItemSelector(IndexInfo *indexInfo,
    BtreeSplitStrategy *strategy) : BtrSplitPointSelectorBase(indexInfo, strategy)
{
}

OffsetNumber BtrSplitPointAfterNewItemSelector::GetBestSplitLoc(uint32 perfectpenalty, bool *newitemonleft)
{
    /* find precise split point after m_newItemOff, split points not sorted */
    for (int i = 0; i < m_nSplitPoints; i++) {
        SplitPoint *split = m_splitPoints + i;
        if (split->newItemOnLeft && m_newItemOff == split->firstRightOff) {
            *newitemonleft = true;
            return m_newItemOff;
        }
    }
    return BtrSplitPointSelectorBase::GetBestSplitLoc(perfectpenalty, newitemonleft);
}

BtrSplitPointManyDuplicatesSelector::BtrSplitPointManyDuplicatesSelector(IndexInfo *indexInfo,
    BtreeSplitStrategy *strategy) : BtrSplitPointSelectorBase(indexInfo, strategy)
{
}

OffsetNumber BtrSplitPointManyDuplicatesSelector::GetBestSplitLoc(uint32 perfectpenalty, bool *newitemonleft)
{
    OffsetNumber result = BtrSplitPointSelectorBase::GetBestSplitLoc(perfectpenalty, newitemonleft);
    /* Fix "monotonically decreasing insertions", thus, 50:50 split new page and waste it */
    if (!m_targetPage->GetLinkAndStatus()->IsRightmost() && !*newitemonleft &&
        result >= m_newItemOff && result < m_newItemOff + LOC_DISTANCE_LIMIT) {
        *newitemonleft = m_splitPoints[0].newItemOnLeft;
        result = m_splitPoints[0].firstRightOff;
    }
    return result;
}

BtrSplitPointSingleValuesSelector::BtrSplitPointSingleValuesSelector(IndexInfo *indexInfo,
    BtreeSplitStrategy *strategy) : BtrSplitPointSelectorBase(indexInfo, strategy)
{
}

OffsetNumber BtrSplitPointSingleValuesSelector::GetBestSplitLoc(UNUSE_PARAM uint32 perfectpenalty, bool *newitemonleft)
{
    /* Split near the end of the page, use separate fillfactor */
    SortCandidateSplitPoints(m_nSplitPoints, m_splitPoints, true,
                             BTREE_SINGLEVAL_FILLFACTOR / static_cast<double>(PERCENTAGE_DIVIDER));
    *newitemonleft = m_splitPoints[0].newItemOnLeft;
    return m_splitPoints[0].firstRightOff;
}

}

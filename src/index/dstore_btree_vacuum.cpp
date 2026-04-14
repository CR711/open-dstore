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
 * dstore_btree_vacuum.cpp
 *
 * IDENTIFICATION
 *        dstore/src/index/dstore_btree_vacuum.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "index/dstore_btree_vacuum.h"
#include "index/dstore_btree_prune.h"
#include "index/dstore_btree_page_recycle.h"

namespace DSTORE {

BtreeVacuum::BtreeVacuum(StorageRelation indexRel, IndexInfo *indexInfo)
    : m_indexRel(indexRel), m_indexInfo(indexInfo),
      m_memContext(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY)),
      m_segScanContext(nullptr),
      m_bufMgr(indexRel->btreeSmgr->IsGlobalTempIndex() ? thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr())
{
    Init();
}

void BtreeVacuum::Init()
{
    m_segScanContext = DstoreNew(m_memContext) DataSegmentScanContext(m_bufMgr, m_indexRel, SmgrType::INDEX_SMGR);
    if (STORAGE_VAR_NULL(m_segScanContext)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("failed to alloc %lu", sizeof(DataSegmentScanContext)));
    }
}

BtreeVacuum::~BtreeVacuum()
{
    m_indexRel = nullptr;
    m_indexInfo = nullptr;
    m_memContext = nullptr;
    delete m_segScanContext;
    m_segScanContext = nullptr;
}

RetStatus BtreeVacuum::BtreeLazyVacuum()
{
    if (unlikely(m_segScanContext == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to BtreeLazyVacuum for m_segScanContext is null."));
        return DSTORE_FAIL;
    }
    PageId curPageId = m_segScanContext->GetFirstPageId();
    BufferDesc *bufferDesc;
    while (curPageId != INVALID_PAGE_ID) {
        bufferDesc =
            m_bufMgr->Read(this->GetPdbId(), curPageId, LW_EXCLUSIVE);
        if (bufferDesc == INVALID_BUFFER_DESC) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Read buffer failed when do BtreeLazyVacuum. Error code: %lld.", StorageGetErrorCode()));
            return DSTORE_FAIL;
        }

        BtrPage *bpage = static_cast<BtrPage *>(static_cast<void*>(bufferDesc->GetPage()));
        BtrPageLinkAndStatus *bpageMeta = bpage->GetLinkAndStatus();
        if (bpageMeta->TestType(BtrPageType::LEAF_PAGE)) {
            BtreePagePrune prunePage(m_indexRel, m_indexInfo, nullptr, bufferDesc);
            UNUSED_VARIABLE(prunePage.Prune(INVALID_ITEM_OFFSET_NUMBER));
            /* After prune, if the page is empty (no live data tuples), immediately start recycle pipeline.
             * PutIntoRecycleQueueIfEmpty handles all buffer release, so skip the UnlockAndRelease below. */
            if (prunePage.IsPagePrunable() && bpage->GetNonDeletedTupleNum() == 0 &&
                !bpageMeta->IsRightmost() && !bpageMeta->IsRoot()) {
                BtreePageRecycle recycle(m_indexRel);
                UNUSED_VARIABLE(recycle.PutIntoRecycleQueueIfEmpty(bufferDesc));
                curPageId = m_segScanContext->GetNextPageId();
                continue;
            }
        }

        m_bufMgr->UnlockAndRelease(bufferDesc);
        curPageId = m_segScanContext->GetNextPageId();
    }
    return DSTORE_SUCC;
}

RetStatus BtreeVacuum::BtreeVacuumGPI(GPIPartOidCheckInfo *gpiCheckInfo)
{
    Btree gpiBtree(m_indexRel, m_indexInfo);
    PageId curPageId = INVALID_PAGE_ID;
    if (STORAGE_FUNC_FAIL(gpiBtree.GetFirstLeafPage(curPageId))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("BtreeVacuumGPI: \"%s\" get first leaf page failed.", m_indexInfo->indexRelName));
        return DSTORE_FAIL;
    }

    BufferDesc *bufferDesc;
    while (curPageId != INVALID_PAGE_ID) {
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("BtreeVacuumGPI: Current request are canceled."));
            return DSTORE_FAIL;
        }
        bufferDesc = Btree::ReadAndCheckBtrPage(curPageId, LW_EXCLUSIVE, m_bufMgr, this->GetPdbId(), true);
        if (unlikely(bufferDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                   ErrMsg("BtreeVacuumGPI: \"%s\" Read buffer failed. Error code: %lld.", m_indexInfo->indexRelName,
                          StorageGetErrorCode()));
            return DSTORE_FAIL;
        }

        BtrPage *bpage = static_cast<BtrPage *>(static_cast<void *>(bufferDesc->GetPage()));
        BtrPageLinkAndStatus *bpageMeta = bpage->GetLinkAndStatus();
        if (!bpageMeta->TestType(BtrPageType::LEAF_PAGE)) {
            ErrLog(
                DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("BtreeVacuumGPI: \"%s\" read not leaf page, " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                       m_indexInfo->indexRelName, BTR_PAGE_HEADER_VAL(bpage), BTR_PAGE_LINK_AND_STATUS_VAL(bpageMeta)));
            m_bufMgr->UnlockAndRelease(bufferDesc);
            return DSTORE_FAIL;
        }

        curPageId = bpageMeta->next;
        BtreePageGPIPrune prunePage(m_indexRel, bufferDesc, m_indexInfo, gpiCheckInfo);
        bool needReleaseBuf = true;
        RetStatus ret = prunePage.GPIPrune(needReleaseBuf);
        if (needReleaseBuf) {
            m_bufMgr->UnlockAndRelease(bufferDesc);
        }
        if (ret != DSTORE_SUCC) {
            return ret;
        }
    }
    return DSTORE_SUCC;
}
}
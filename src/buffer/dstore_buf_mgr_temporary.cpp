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
 * Description: local buffer pool manager for temporary table.
 */
#include "buffer/dstore_buf_mgr_temporary.h"
#include "errorcode/dstore_framework_error_code.h"
#include "framework/dstore_instance.h"
#include "dfx/dstore_page_verify.h"
#include "page/dstore_index_page.h"
#include "transaction/dstore_transaction.h"
#include "common/error/dstore_error.h"

namespace DSTORE {

namespace {

RetStatus VerifyTmpPageBeforePersist(Page *page)
{
    if (page == nullptr || GetDfxVerifyLevel() == VerifyLevel::OFF || !IsPageVerifierRegistered(page->GetType())) {
        return DSTORE_SUCC;
    }

    page->SetChecksum();
    return VerifyPageInline(page);
}

}  // namespace

/* entry for buffer lookup hashtable */
struct BufferLookupEnt {
    BufferTag key;      /* Id of a disk page */
    BufferDesc *buffer; /* Pointer to BufferDesc */
};

TmpLocalBufMgr::TmpLocalBufMgr(int32 bufNums)
    : m_memoryContext(nullptr),
      m_bufHash(nullptr),
      m_buffers(nullptr),
      m_bufNums(bufNums),
      m_nextBufIdx(0),
      m_initialized(false),
      m_invalidBuffList{}
{}

RetStatus TmpLocalBufMgr::Init()
{
    AutoMemCxtSwitch autoSwtich{thrd->GetSessionMemoryCtx()};

    /* Step 1: init memory context, SHARED_CONTEXT for thread pool */
    m_memoryContext = DstoreAllocSetContextCreate(
        thrd->GetSessionMemoryCtx(), "tmpBufferHashMemory", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, MemoryContextType::SESSION_CONTEXT);
    if (STORAGE_VAR_NULL(m_memoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Failed to alloc tmpBufferHashMemory."));
        return DSTORE_FAIL;
    }

    /* Step 2: init hash table */
    HASHCTL info;
    info.keysize = sizeof(BufferTag);
    info.entrysize = sizeof(BufferLookupEnt);
    info.hash = tag_hash;
    info.dsize = info.max_dsize = hash_select_dirsize(m_bufNums);
    info.hcxt = m_memoryContext;

    m_bufHash = hash_create("Tmp Local Buffer Hash Table", m_bufNums, &info,
                            HASH_ELEM | HASH_FUNCTION | HASH_DIRSIZE | HASH_CONTEXT);
    if (STORAGE_VAR_NULL(m_bufHash)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Init tmp local buffer hash table failed."));
        DstoreMemoryContextDelete(m_memoryContext);
        m_memoryContext = nullptr;
        return DSTORE_FAIL;
    }

    /* Step 3: init buffer desc ring */
    m_buffers = static_cast<BufferDesc **>(DstoreMemoryContextAllocZero(m_memoryContext,
        sizeof(BufferDesc *) * m_bufNums));
    if (STORAGE_VAR_NULL(m_buffers)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Failed to alloc m_buffers."));
        hash_destroy(m_bufHash);
        m_bufHash = nullptr;
        DstoreMemoryContextDelete(m_memoryContext);
        m_memoryContext = nullptr;
        return DSTORE_FAIL;
    }
    m_nextBufIdx = 0;
    SListInit(&m_invalidBuffList);
    m_initialized = true;

    return DSTORE_SUCC;
}

void TmpLocalBufMgr::Destroy()
{
    if (!m_initialized) {
        return;
    }
    for (int32 i = 0; i < m_bufNums; ++i) {
        if (m_buffers[i] == nullptr) {
            continue;
        }
        if (m_buffers[i]->bufBlock != nullptr) {
            DstorePfreeAligned(m_buffers[i]->bufBlock);
        }
        DstorePfree(m_buffers[i]);
    }

    while (!SListIsEmpty(&m_invalidBuffList)) {
        slist_node *node = SListPopHeadNode(&m_invalidBuffList);
        InvalidBufferNode *invalidBufferNode = slist_container(InvalidBufferNode, node, node);
        DstorePfree(invalidBufferNode);
    }
    hash_destroy(m_bufHash);
    DstoreMemoryContextDelete(m_memoryContext);
}

BufferDesc *TmpLocalBufMgr::Read(const PdbId &pdbId, const PageId &pageId, UNUSE_PARAM LWLockMode mode,
                                 BufferPoolReadFlag flag, UNUSE_PARAM BufferRing bufRing /* = nullptr */)
{
    StorageReleasePanic(pdbId == INVALID_PDB_ID || pdbId > PDB_MAX_ID, MODULE_BUFMGR,
                        ErrMsg("Invalid pdbId: %hhu.", pdbId));
    StorageReleasePanic(!pageId.IsValid(), MODULE_BUFMGR,
                        ErrMsg("Invalid pageId:(%hu, %u).", pageId.m_fileId, pageId.m_blockId));

    /* Step 1: Init if needed. */
    if (unlikely(!m_initialized) && STORAGE_FUNC_FAIL(Init())) {
        return INVALID_BUFFER_DESC;
    }

    /* Step 2: Look up in hash table. If hash entry does not exist, create one. */
    AutoMemCxtSwitch autoSwtich{m_memoryContext};

    BufferDesc *bufDesc = INVALID_BUFFER_DESC;
    BufferTag tag = {pdbId, pageId};
    uint32 hcode = get_hash_value(m_bufHash, static_cast<const void *>(&tag));
    bool found = false;

    BufferLookupEnt *entry = static_cast<BufferLookupEnt *>(BufLookUp<HASH_ENTER>(m_bufHash, &tag, hcode, &found));
    StorageReleasePanic(entry == nullptr, MODULE_BUFMGR, ErrMsg("entry is null."));

    /* Step 3: Found in hash table. */
    if (found) {
        bufDesc = entry->buffer;
        StorageAssert(bufDesc != nullptr);
        if (likely((bufDesc->GetState() & Buffer::BUF_VALID) != 0)) {
            bufDesc->Pin<true>();
            return bufDesc;
        }
        /* if buffer is invalid, there must have been an IO error earlier */
        StorageAssert(bufDesc->state & Buffer::BUF_IO_ERROR);
        if (STORAGE_FUNC_FAIL(ReadBlock(bufDesc))) {
            return INVALID_BUFFER_DESC;
        }
        bufDesc->state &= (~Buffer::BUF_IO_ERROR);
        bufDesc->state |= Buffer::BUF_VALID;
        bufDesc->Pin<true>();
        return bufDesc;
    }

    /* Step 4: It is not in tmp local buffer pool. Create a new one. */
    bufDesc = GetAvailableBuffer();
    if (unlikely(bufDesc == INVALID_BUFFER_DESC)) {
        /* remove the new entry when OOM/IO error/out of buffer */
        (void)BufLookUp<HASH_REMOVE>(m_bufHash, &tag, hcode, nullptr);
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Tmp buffer Invalid."));
        return INVALID_BUFFER_DESC;
    }

    entry->buffer = bufDesc;
    /* if it is not new page, read from disk. BUF_OWNED_BY_ME is just for the assert in GetPage. */
    bufDesc->bufTag = tag;
    bufDesc->state |= (Buffer::BUF_OWNED_BY_ME | Buffer::BUF_TAG_VALID);
    if (!flag.IsNewPage() && STORAGE_FUNC_FAIL(ReadBlock(bufDesc))) {
        bufDesc->state |= Buffer::BUF_IO_ERROR;
        return INVALID_BUFFER_DESC;
    }

    bufDesc->state |= Buffer::BUF_VALID;
    bufDesc->Pin<true>();
    return bufDesc;
}

void TmpLocalBufMgr::InsertInvalidBufferList(BufferDesc *bufDesc)
{
    AutoMemCxtSwitch autoSwtich{m_memoryContext};

    InvalidBufferNode *bufferNode = static_cast<InvalidBufferNode *>(DstorePalloc0(sizeof(InvalidBufferNode)));
    if (unlikely(bufferNode == nullptr)) {
        /* This list is used to optimize performance when reuse buffer. */
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Alloc memory for InvalidBufferNode failed."));
        return;
    }

    bufferNode->bufferDesc = bufDesc;
    SListPushHead(&m_invalidBuffList, &bufferNode->node);
}

void TmpLocalBufMgr::GetFromInvalidBufferList(BufferDesc **outBufDesc)
{
    if (!SListIsEmpty(&m_invalidBuffList)) {
        slist_node *node = SListPopHeadNode(&m_invalidBuffList);
        InvalidBufferNode *invalidBufferNode = slist_container(InvalidBufferNode, node, node);
        *outBufDesc = invalidBufferNode->bufferDesc;
        DstorePfree(invalidBufferNode);
    } else {
        *outBufDesc = INVALID_BUFFER_DESC;
    }
}

BufferDesc *TmpLocalBufMgr::GetAvailableBuffer()
{
    StorageAssert(m_initialized);
    int32 bufIdx = m_nextBufIdx;
    BufBlock bufBlock = nullptr;
    AutoMemCxtSwitch autoSwtich{m_memoryContext};

    /* step 1: try to fine one invalid buffer(maybe InvalidateByBufTag) */
    BufferDesc *reuseBuffDest = INVALID_BUFFER_DESC;
    GetFromInvalidBufferList(&reuseBuffDest);
    if (reuseBuffDest != INVALID_BUFFER_DESC) {
        return reuseBuffDest;
    }

    /* Step 2: use new buffer desc first. */
    StorageReleasePanic(m_buffers == nullptr, MODULE_BUFMGR, ErrMsg("m_buffers is null."));
    if (m_buffers[bufIdx] == INVALID_BUFFER_DESC) {
        m_buffers[bufIdx] = static_cast<BufferDesc *>(DstorePalloc0(sizeof(BufferDesc)));
        if (unlikely(m_buffers[bufIdx] == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Alloc memory for tmp bufferDesc failed."));
            return INVALID_BUFFER_DESC;
        }
        bufBlock = static_cast<BufBlock>(DstorePallocAligned(BLCKSZ, ALIGNOF_BUFFER));
        if (unlikely(bufBlock == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Alloc memory for tmp bufBlock failed."));
            DstorePfreeExt(m_buffers[bufIdx]);
            return INVALID_BUFFER_DESC;
        }
        errno_t rc = memset_s(bufBlock, BLCKSZ, 0, BLCKSZ);
        storage_securec_check(rc, "\0", "\0");
        m_buffers[bufIdx]->InitBufferDesc(bufBlock, nullptr);
        m_nextBufIdx = (bufIdx + 1) % m_bufNums;
        return m_buffers[bufIdx];
    }

    /* Step 3: reuse buffer desc. */
    int32 i = 0;
    for (i = 0; i < m_bufNums; ++i) {
        StorageReleasePanic(m_buffers[bufIdx] == nullptr, MODULE_BUFMGR, ErrMsg("Tmp buffer is null. ID: %d.", bufIdx));
        if (m_buffers[bufIdx]->GetRefcount() == 0) {
            break;
        }
        bufIdx = (bufIdx + 1) % m_bufNums;
    }
    if (i == m_bufNums) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("There is no available buffer in temp local buffer pool!"));
        return INVALID_BUFFER_DESC;
    }

    BufferTag tag = m_buffers[bufIdx]->GetBufferTag();
    if (!tag.IsInvalid()) {
        if (m_buffers[bufIdx]->IsPageDirty() && STORAGE_FUNC_FAIL(WriteBlock(m_buffers[bufIdx]))) {
            ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("WriteBlock tmp dirty page failed."));
            return INVALID_BUFFER_DESC;
        }
        uint32 hcode = get_hash_value(m_bufHash, static_cast<const void *>(&tag));
        BufferLookupEnt *res = static_cast<BufferLookupEnt *>(BufLookUp<HASH_REMOVE>(m_bufHash, &tag, hcode, nullptr));
        StorageReleasePanic(res == nullptr, MODULE_BUFMGR, ErrMsg("Remove tmp buffer from hash table failed. "
            "bufTag:(%hhu, %hu, %u)", tag.pdbId, tag.pageId.m_fileId, tag.pageId.m_blockId));
    }

    bufBlock = m_buffers[bufIdx]->bufBlock;
    errno_t rc = memset_s(bufBlock, BLCKSZ, 0, BLCKSZ);
    storage_securec_check(rc, "\0", "\0");
    m_buffers[bufIdx]->InitBufferDesc(bufBlock, nullptr);
    m_nextBufIdx = (bufIdx + 1) % m_bufNums;
    return m_buffers[bufIdx];
}

RetStatus TmpLocalBufMgr::ReadBlock(BufferDesc *bufDesc)
{
    StorageAssert(m_initialized);
    BufferTag bufTag = bufDesc->GetBufferTag();
    if (g_storageInstance->GetPdb(bufDesc->GetPdbId()) == nullptr ||
        g_storageInstance->GetPdb(bufDesc->GetPdbId())->GetVFS() == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("StoragePdb or VFS is nullptr."));
    }
    VFSAdapter *vfs = g_storageInstance->GetPdb(bufDesc->GetPdbId())->GetVFS();
    RetStatus ret = DSTORE_SUCC;

    /* Step 1: read from vfs */
READ:
    PageId pageId = bufDesc->GetPageId();
    ret = vfs->ReadPageSync(pageId, bufDesc->GetPage());
    if (STORAGE_FUNC_FAIL(ret)) {
        if (StorageGetErrorCode() == VFS_WARNING_FILE_NOT_OPENED) {
            FileId fileId = bufDesc->GetPageId().m_fileId;
            char fileName[MAXPGPATH] = {0};

            if (STORAGE_FUNC_FAIL(vfs->GetFileNameFromFileId(fileId, fileName))) {
                return DSTORE_FAIL;
            }

            for (int i = 0; i < MAX_OPEN_TIMES && STORAGE_FUNC_FAIL(ret); ++i) {
                ret = vfs->OpenFile(fileId, fileName, DSTORE_FILE_OPEN_FLAG);
            }

            if (STORAGE_FUNC_SUCC(ret)) {
                goto READ;
            }
        }
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("VFS read page temp bufTag:(%hhu, %hu, %u) fail.",
            bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
        return DSTORE_FAIL;
    }

    /* Step 2: check page */
    Page* page = bufDesc->GetPage();
    if (!page->CheckPageCrcMatch()) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("read page bufTag:(%hhu, %hu, %u) check crc fail.",
            bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
        return DSTORE_FAIL;
    }

#ifndef UT
    if (page->GetType() != PageType::INVALID_PAGE_TYPE && page->GetSelfPageId() != pageId) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR,
               ErrMsg("Reading invalid page from disk, bufTag:(%hhu, %hu, %u), page type:%d, "
                      "page id:(%hu, %u) glsn(%lu) plsn(%lu) walId(%lu).",
                      bufTag.pdbId, pageId.m_fileId, pageId.m_blockId, static_cast<int>(page->GetType()),
                      page->GetSelfPageId().m_fileId, page->GetSelfPageId().m_blockId, bufDesc->GetPage()->GetGlsn(),
                      bufDesc->GetPage()->GetPlsn(), bufDesc->GetPage()->GetWalId()));
    }
#endif

    return ret;
}

RetStatus TmpLocalBufMgr::MarkDirty(BufferDesc *bufferDesc, UNUSE_PARAM bool needUpdateRecoveryPlsn)
{
    StorageAssert(m_initialized);
    if (STORAGE_FUNC_FAIL(VerifyTmpPageBeforePersist(bufferDesc->GetPage()))) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("TmpLocalBufMgr::MarkDirty blocked by page verify failure."));
        return DSTORE_FAIL;
    }
    bufferDesc->state |= Buffer::BUF_CONTENT_DIRTY;
    return DSTORE_SUCC;
}

RetStatus TmpLocalBufMgr::WriteBlock(BufferDesc *bufferDesc)
{
    StorageAssert(m_initialized);
    BufferTag bufTag = bufferDesc->GetBufferTag();
    PageId pageId = bufferDesc->GetPageId();
    VFSAdapter *vfs = nullptr;

    /* Step 1: clean dirty flag */
    bufferDesc->state &= (~Buffer::BUF_CONTENT_DIRTY);

    /* Step 2: Write page data. */
    StoragePdb *storagePdb = g_storageInstance->GetPdb(bufferDesc->GetPdbId());
    if (storagePdb == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("StoragePdb is nullptr."));
    }
    vfs = storagePdb->GetVFS();
    StorageReleasePanic(vfs == nullptr, MODULE_BUFMGR, ErrMsg("vfs is nullptr, pdb %u", bufferDesc->GetPdbId()));
    if (STORAGE_FUNC_FAIL(VerifyTmpPageBeforePersist(bufferDesc->GetPage()))) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("TmpLocalBufMgr::WriteBlock blocked by page verify failure."));
        return DSTORE_FAIL;
    }
    bufferDesc->GetPage()->SetChecksum();

    if (STORAGE_FUNC_FAIL(vfs->WritePageSync(pageId, bufferDesc->GetPage()))) {
        bufferDesc->state |= Buffer::BUF_IO_ERROR;
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("VFS write page bufTag:(%hhu, %hu, %u) fail, error:%lld",
            bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId, StorageGetErrorCode()));
        return DSTORE_FAIL;
    }
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("WriteBlock page bufTag:(%hhu, %hu, %u).", bufTag.pdbId,
        bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
#endif

    return DSTORE_SUCC;
}

RetStatus TmpLocalBufMgr::Flush(BufferTag &bufTag, UNUSE_PARAM void* aioCtx)
{
    uint32 hcode = get_hash_value(m_bufHash, static_cast<const void *>(&bufTag));
    bool found = false;
    BufferLookupEnt *entry = static_cast<BufferLookupEnt *>(BufLookUp<HASH_FIND>(m_bufHash, &bufTag, hcode, &found));
    if (entry == nullptr || entry->buffer == INVALID_BUFFER_DESC) {
        return DSTORE_SUCC;
    }
    BufferDesc *bufDesc = entry->buffer;
    return bufDesc->IsPageDirty() ? WriteBlock(bufDesc) : DSTORE_SUCC;
}

void TmpLocalBufMgr::Release(BufferDesc *bufferDesc)
{
    StorageAssert(m_initialized);
    bufferDesc->Unpin<true>();
}

void TmpLocalBufMgr::UnlockAndRelease(BufferDesc *bufferDesc, UNUSE_PARAM BufferPoolUnlockContentFlag flag)
{
    Release(bufferDesc);
}

RetStatus TmpLocalBufMgr::ConsistentRead(ConsistentReadContext &crContext,
                                         UNUSE_PARAM BufferRing bufRing /* = nullptr */)
{
    /* Step 1: Read the base page by pageId. */
    BufferDesc *baseBufDesc = Read(crContext.pdbId, crContext.pageId, LW_SHARED);
    STORAGE_CHECK_BUFFER_PANIC(baseBufDesc, MODULE_BUFFER, crContext.pageId);
    PageType pageType = baseBufDesc->GetPage()->GetType();
    if (pageType != PageType::HEAP_PAGE_TYPE && pageType != PageType::INDEX_PAGE_TYPE) {
        storage_set_error(BUFFER_INFO_CONSTRUCT_CR_NOT_DATA_PAGE);
        Release(baseBufDesc);
        return DSTORE_FAIL;
    }

    /* Step 2: Copy the base page to dest page. */
    crContext.crBufDesc = INVALID_BUFFER_DESC;
    errno_t rc = memcpy_s(static_cast<char *>(static_cast<void *>(crContext.destPage)), BLCKSZ,
        baseBufDesc->GetPage(), BLCKSZ);
    storage_securec_check(rc, "\0", "\0");

    /* Step 3: Construct a cr page that match with given snapshot. */
    CRContext crCtx{crContext.pdbId, INVALID_CSN, nullptr, nullptr, nullptr, false, true, crContext.snapshot,
        crContext.currentXid};
    RetStatus ret;
    if (pageType == PageType::INDEX_PAGE_TYPE) {
        ret = (static_cast<BtrPage *>(crContext.destPage))->ConstructCR(thrd->GetActiveTransaction(), &crCtx,
            static_cast<BtreeUndoContext *>(crContext.dataPageExtraInfo), this);
    } else {
        ret = crContext.destPage->ConstructCR(thrd->GetActiveTransaction(), &crCtx);
    }

    Release(baseBufDesc);
    return ret;
}

BufferDesc *TmpLocalBufMgr::ReadCr(UNUSE_PARAM BufferDesc *baseBufferDesc, UNUSE_PARAM Snapshot snapshot)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support"));
    return nullptr;
}

BufferDesc *TmpLocalBufMgr::ReadOrAllocCr(UNUSE_PARAM BufferDesc *baseBufDesc, UNUSE_PARAM uint64 lastPageModifyTime,
                                          UNUSE_PARAM BufferRing bufRing /* = nullptr */)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support"));
    return nullptr;
}

void TmpLocalBufMgr::FinishCrBuild(UNUSE_PARAM BufferDesc *crBufferDesc, UNUSE_PARAM CommitSeqNo pageMaxCsn)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support"));
}

RetStatus TmpLocalBufMgr::FlushAll(UNUSE_PARAM bool isBootstrap, UNUSE_PARAM bool onlyOwnedByMe,
                                   UNUSE_PARAM PdbId pdbId)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support"));
    return DSTORE_FAIL;
}

RetStatus TmpLocalBufMgr::TryFlush(UNUSE_PARAM BufferDesc *bufferDesc)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support"));
    return DSTORE_FAIL;
}

RetStatus TmpLocalBufMgr::LockContent(UNUSE_PARAM BufferDesc *bufferDesc, UNUSE_PARAM LWLockMode mode)
{
    return DSTORE_SUCC;
}

bool TmpLocalBufMgr::TryLockContent(UNUSE_PARAM BufferDesc *bufferDesc, UNUSE_PARAM LWLockMode mode)
{
    return true;
}

void TmpLocalBufMgr::PinAndLock(UNUSE_PARAM BufferDesc *bufferDesc, UNUSE_PARAM LWLockMode mode)
{}

void TmpLocalBufMgr::UnlockContent(UNUSE_PARAM BufferDesc *bufferDesc, UNUSE_PARAM BufferPoolUnlockContentFlag flag)
{}

RetStatus TmpLocalBufMgr::Invalidate(UNUSE_PARAM BufferDesc *bufferDesc)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support."));
    return DSTORE_FAIL;
}

RetStatus TmpLocalBufMgr::InvalidateByBufTag(BufferTag bufTag, bool needFlush)
{
    uint32 hcode = get_hash_value(m_bufHash, static_cast<const void *>(&bufTag));
    bool found = false;
    BufferLookupEnt *entry = static_cast<BufferLookupEnt *>(BufLookUp<HASH_FIND>(m_bufHash, &bufTag, hcode, &found));
    if (entry == nullptr || entry->buffer == INVALID_BUFFER_DESC) {
        ErrLog(DSTORE_WARNING, MODULE_BUFMGR, ErrMsg("Invalidate by buftag:(%hhu, %hu, %u) not found in hash.",
            bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
        return DSTORE_SUCC;
    }
    BufferDesc *bufDesc = entry->buffer;
    if (needFlush && bufDesc->IsPageDirty() && STORAGE_FUNC_FAIL(WriteBlock(bufDesc))) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Invalidate by buftag:(%hhu, %hu, %u) flush failed.",
            bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));
        return DSTORE_FAIL;
    }

    StorageAssert(bufDesc->GetRefcount() == 0);

    BufferLookupEnt *res = static_cast<BufferLookupEnt *>(
        BufLookUp<HASH_REMOVE>(m_bufHash, &bufTag, hcode, nullptr));
    StorageReleasePanic(res == nullptr, MODULE_BUFMGR, ErrMsg("Remove tmp buffer from hash table failed. "
        "bufTag:(%hhu, %hu, %u)", bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId));

    BufBlock bufBlock = bufDesc->bufBlock;
    bufDesc->InitBufferDesc(bufBlock, nullptr);

    InsertInvalidBufferList(bufDesc);
    return DSTORE_SUCC;
}

RetStatus TmpLocalBufMgr::InvalidateUsingGivenFileId(UNUSE_PARAM PdbId pdbId, UNUSE_PARAM FileId fileId)
{
    return DSTORE_SUCC;
}

RetStatus TmpLocalBufMgr::InvalidateBaseBuffer(UNUSE_PARAM BufferDesc *bufferDesc, UNUSE_PARAM BufferTag bufTag,
                                               UNUSE_PARAM bool needFlush)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support."));
    return DSTORE_FAIL;
}

RetStatus TmpLocalBufMgr::InvalidateUsingGivenPdbId(UNUSE_PARAM PdbId pdbId)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support."));
}

RetStatus TmpLocalBufMgr::BatchCreateNewPage(const PdbId &pdbId, PageId *pageId, uint64 pageCount,
                                             BufferDesc **newBuffer, UNUSE_PARAM BufferRing bufRing /* = nullptr */)
{
    for (uint64 i = 0; i < pageCount; ++i) {
        newBuffer[i] = Read(pdbId, pageId[i], LW_EXCLUSIVE, BufferPoolReadFlag::CreateNewPage());
        if (unlikely(newBuffer[i] == INVALID_BUFFER_DESC)) {
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

BufferDesc *TmpLocalBufMgr::RecoveryRead(UNUSE_PARAM const PdbId &pdbId, UNUSE_PARAM const PageId &pageId)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support."));
    return nullptr;
}

RetStatus TmpLocalBufMgr::GetPageDirectoryInfo(UNUSE_PARAM Size *length, UNUSE_PARAM char **errInfo,
                                               UNUSE_PARAM PageDirectoryInfo **pageDirectoryArr)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support."));
    return DSTORE_FAIL;
}

RetStatus TmpLocalBufMgr::GetPDBucketInfo(UNUSE_PARAM Size *length, UNUSE_PARAM char ***chashBucketInfo,
                                          UNUSE_PARAM uint32 startBucket, UNUSE_PARAM uint32 endBucket)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support."));
    return DSTORE_FAIL;
}

RetStatus TmpLocalBufMgr::GetBufDescPrintInfo(UNUSE_PARAM Size *length, UNUSE_PARAM char **errInfo,
                                              UNUSE_PARAM BufDescPrintInfo **bufferDescArr)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support."));
    return DSTORE_FAIL;
}

uint8 TmpLocalBufMgr::GetBufDescResponseType(UNUSE_PARAM BufferDesc *bufferDesc)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support."));
    return 0;
}

RetStatus TmpLocalBufMgr::DoWhenBufferpoolResize(UNUSE_PARAM Size bufferPoolNewSize,
                                                 UNUSE_PARAM StringInfoData &outputMessage)
{
    StorageReleasePanic(true, MODULE_BUFMGR, ErrMsg("not support."));
    return DSTORE_FAIL;
}

char *TmpLocalBufMgr::GetClusterBufferInfo(UNUSE_PARAM PdbId pdbId, UNUSE_PARAM FileId fileId,
                                           UNUSE_PARAM BlockNumber blockId)
{
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        return nullptr;
    }
    dumpInfo.append("The function is not applicable to temp local BufMgr.");
    return dumpInfo.data;
}

char *TmpLocalBufMgr::GetPdBucketLockInfo()
{
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        return nullptr;
    }
    return dumpInfo.data;
}

#ifdef DSTORE_USE_ASSERT_CHECKING
void TmpLocalBufMgr::AssertHasHoldBufLock(UNUSE_PARAM const PdbId pdbId, UNUSE_PARAM const PageId pageId,
                                          UNUSE_PARAM LWLockMode lockMode)
{}
#endif

}; /* namespace DSTORE */

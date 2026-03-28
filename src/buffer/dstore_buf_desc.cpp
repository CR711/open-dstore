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
 * dstore_buf_desc.cpp
 *      This file implements the functionality of buffer descriptor.
 *
 * IDENTIFICATION
 *      src/buffer/dstore_buf_desc.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/dstore_datatype.h"
#include "common/log/dstore_log.h"
#include "buffer/dstore_buf_mgr.h"
#include "buffer/dstore_buf_refcount.h"
#include "framework/dstore_instance.h"
#include "wal/dstore_wal.h"

namespace DSTORE {

void BufferDescController::InitController()
{
    LWLockInitialize(&ioInProgressLwlock.lock, LWLOCK_GROUP_BUF_DESC_IO_IN_PROGRESS);
    LWLockInitialize(&crAssignLwlock.lock, LWLOCK_GROUP_BUF_DESC_CR_ASSIGN);
    lastPageModifyTime.store(0, std::memory_order_release);
}

void BufferDesc::InitBufferDesc(BufBlock block, BufferDescController *ctrler)
{
    ASSERT_POINTER_ALIGNMENT(block, ALIGNOF_BUFFER);
    bufTag = INVALID_BUFFER_TAG;
    TsAnnotateRWLockCreate(&state);
    GsAtomicInitU64(&state, 0);

    lruNode.InitNode(this);
    crInfo.InitCRInfo();
    bufBlock = block;
    controller = ctrler;
    LWLockInitialize(&contentLwLock, LWLOCK_GROUP_BUF_DESC_CONTENT);

    for (uint32 i = 0; i < DIRTY_PAGE_QUEUE_MAX_SIZE; i++) {
        nextDirtyPagePtr[i] = INVALID_BUFFER_DESC;
        recoveryPlsn[i].store(INVALID_PLSN, std::memory_order_release);
    }

    pageVersionOnDisk = INVALID_PAGE_LSN;
    fileVersion = INVALID_FILE_VERSION;
}

void BufferDesc::UpdatePageVersion(Page *newPage)
{
    PageVersion newPageVersion = {newPage->GetGlsn(), newPage->GetPlsn()};
    if (newPageVersion == INVALID_PAGE_LSN || newPageVersion.IsPageVersionAllZero()) {
        return;
    }

    if (pageVersionOnDisk == INVALID_PAGE_LSN || pageVersionOnDisk < newPageVersion) {
        pageVersionOnDisk = newPageVersion;
        return;
    }
    StorageReleasePanic(pageVersionOnDisk > newPageVersion, MODULE_BUFMGR, ErrMsg("[AntiCache]page(%hu, %u) "
        "newPageVersion(%lu, %lu) < oldPageVerion(%lu, %lu)", newPage->GetFileId(), newPage->GetBlockNum(),
        newPageVersion.glsn, newPageVersion.plsn, pageVersionOnDisk.glsn, pageVersionOnDisk.plsn));
}
/*
 * Get reference count.
 * Note: this method should be called under BUF_LOCKED protect (calling LockHdr() before calling this function).
 */
uint64 BufferDesc::GetRefcount()
{
    uint64 currentState = GsAtomicReadU64(&state);
    return (currentState & Buffer::BUF_REFCOUNT_MASK);
}

void BufferDesc::ClearDirtyState(uint64 flags)
{
    uint64 bufState = LockHdr();
    StorageReleasePanic(bufState & Buffer::BUF_IS_WRITING_WAL, MODULE_BUFMGR,
        ErrMsg("Clear dirty state has writing wal flag, bufTag:(%hhu, %hu, %u) lsnInfo(walId %lu glsn %lu plsn %lu) "
        "state(%lu), recoveryPlsn(%lu)", GetPdbId(), GetPageId().m_fileId, GetPageId().m_blockId,
        GetPage()->GetWalId(), GetPage()->GetGlsn(), GetPage()->GetPlsn(), bufState,
        recoveryPlsn[DEFAULT_BGWRITER_SLOT_ID].load(std::memory_order_acquire)));
    bufState &= ~(Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY | flags);
    UnlockHdr(bufState);
}

/*
 * Lock buffer header - set BUF_LOCKED in buffer state.
 */
HOTFUNCTION uint64 BufferDesc::LockHdr()
{
    uint64 oldState = 0;
    SpinDelayStatus delayStatus = InitSpinDelay(__FILE__, __LINE__);

    for (;;) {
        /* set BUF_LOCKED flag */
        oldState = GsAtomicFetchOrU64(&state, Buffer::BUF_LOCKED);
        /* if it wasn't set before we're OK */
        if (!(oldState & Buffer::BUF_LOCKED)) {
            break;
        }
        PerformSpinDelay(&delayStatus, contentLwLock.spinsPerDelay);
    }

    AdjustSpinsPerDelay(&delayStatus, contentLwLock.spinsPerDelay);
    TsAnnotateRWLockAcquired(&state, 1);
    return oldState | Buffer::BUF_LOCKED;
}

HOTFUNCTION void BufferDesc::UnlockHdr(uint64 flags)
{
    TsAnnotateRWLockReleased(&state, 1);
    uint32 tmpState = (flags >> Buffer::BUF_REFCOUNT_BIT_NUM) & ((~Buffer::BUF_LOCKED) >> Buffer::BUF_REFCOUNT_BIT_NUM);
    GsAtomicWriteU32(((volatile uint32 *)&state) + 1, tmpState);
}

HOTFUNCTION uint64 BufferDesc::WaitHdrUnlock()
{
    uint64 bufState;
    SpinDelayStatus delayStatus = InitSpinDelay(__FILE__, __LINE__);

    bufState = GsAtomicReadU64(static_cast<volatile uint64*>(&state));
    while (bufState & Buffer::BUF_LOCKED) {
        PerformSpinDelay(&delayStatus, contentLwLock.spinsPerDelay);
        bufState = GsAtomicReadU64(static_cast<volatile uint64*>(&state));
    }

    return bufState;
}

bool BufferDesc::FastLockHdrIfReusable(const BufferTag &inBufTag, bool justValidTag, bool ignoreDirtyPage)
{
    const uint64 oldState = GsAtomicFetchOrU64(&this->state, Buffer::BUF_LOCKED);
    if ((oldState & Buffer::BUF_LOCKED) != 0) {
        /*
         * When it's locked by others, we assume it's most likely not reusable for us. Therefore, we bravely give it
         * up and try other buffers on the LRU list.
         */
        return false;
    }

    bool isBufTagReusable = justValidTag ? (!bufTag.IsInvalid()) : (bufTag.IsInvalid() || bufTag != inBufTag);
    if (this->GetRefcount() == 0 && isBufTagReusable && ((oldState & Buffer::BUF_IS_WRITING_WAL) == 0) &&
        !(ignoreDirtyPage && (oldState & (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY)))) {
            /* Return with the header locked. */
            TsAnnotateRWLockAcquired(&state, 1);
            return true;
    }

    /* We still fail to reuse it, unlock the header and bail. */
    (void)GsAtomicFetchAndU64(&state, ~Buffer::BUF_LOCKED);
    return false;
}

HOTFUNCTION bool BufferDesc::IsHdrLocked()
{
    uint64 bufferState = GsAtomicReadU64(&state);
    return static_cast<bool>(bufferState & Buffer::BUF_LOCKED);
}

template<bool isGlobalTempTable>
HOTFUNCTION void BufferDesc::Pin()
{
    if (isGlobalTempTable) {
        state += Buffer::BUF_REFCOUNT_ONE;
        StorageReleasePanic(((state & Buffer::BUF_REFCOUNT_MASK) == Buffer::BUF_REFCOUNT_MASK), MODULE_BUFMGR,
                            ErrMsg("refcount in temp buffer desc is overflow, state:%lu", state));
        return;
    }
    BufPrivateRefCount *privateRefCount = thrd->GetBufferPrivateRefCount();
    if (STORAGE_VAR_NULL(privateRefCount)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("PrivateRefCount is nullptr."));
    }

    /* When the secondly and thirdly parameter all both true, the ret value must not be NULL. */
    PrivateRefCountEntry *privateRef = privateRefCount->GetPrivateRefcount(this);
    StorageAssert(privateRef != nullptr);

    if (privateRef->refcount == 0) {
        SharedPin();
    }

    if (unlikely(privateRef->refcount > 0xffff)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
            ErrMsg("the pin of buffer bufTag:(%hhu, %hu, %u) may leak, the refcount is %d",
                bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                privateRef->refcount));
    }
    privateRef->refcount++;
}

/*
 * Pin Shared buffer among multi-threads.
 * Pin() is based on it when one thread has use it pin the first time,
 * because it involves race on Shared buffer modifing with other threads.
 * note: dont use it directly.
 */
HOTFUNCTION void BufferDesc::SharedPin()
{
    uint64 bufState;
    for (;;) {
        /* Increase refcount */
        bufState = GsAtomicFetchAddU64(&state, Buffer::BUF_REFCOUNT_ONE);
        if (bufState & Buffer::BUF_LOCKED) {
            /* Decrease refcount */
            bufState = GsAtomicSubFetchU64(&state, static_cast<int64>(Buffer::BUF_REFCOUNT_ONE));
            (void)WaitHdrUnlock();
            continue;
        }
        StorageAssert((bufState & Buffer::BUF_REFCOUNT_MASK) != Buffer::BUF_REFCOUNT_MASK);
        break;
    }
}

HOTFUNCTION void BufferDesc::PinForAio()
{
    SharedPin();
}

HOTFUNCTION void BufferDesc::PinUnderHdrLocked()
{
    BufPrivateRefCount *privateRefCount = thrd->GetBufferPrivateRefCount();
    StorageAssert(privateRefCount != nullptr);
    UNUSE_PARAM uint64 bufState = GsAtomicReadU64(&state);
    StorageAssert((bufState & Buffer::BUF_LOCKED) != 0);

    /* When the secondly and thirdly parameter all both true, the ret value must not be NULL. */
    PrivateRefCountEntry *privateRef = privateRefCount->GetPrivateRefcount(this);
    StorageAssert(privateRef != nullptr);

    if (unlikely(privateRef->refcount > 0xffff)) {
        ErrLog(DSTORE_ERROR, (MODULE_BUFMGR),
            ErrMsg("the pin of buffer bufTag:(%hhu, %hu, %u) may leak, the refcount is %d",
                bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                privateRef->refcount));
    }

    if (privateRef->refcount == 0) {
        StorageAssert((bufState & Buffer::BUF_REFCOUNT_MASK) != Buffer::BUF_REFCOUNT_MASK);
        bufState = GsAtomicFetchAddU64(&state, Buffer::BUF_REFCOUNT_ONE);
        if (unlikely((bufState & Buffer::BUF_REFCOUNT_MASK) > 0xffffff)) {
            ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
                ErrMsg("the pin of buffer bufTag:(%hhu, %hu, %u) may leak, the refcount is %lu",
                    bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                    (bufState & Buffer::BUF_REFCOUNT_MASK)));
        }
        StorageReleasePanic(((bufState & Buffer::BUF_REFCOUNT_MASK) == Buffer::BUF_REFCOUNT_MASK),
            MODULE_BUFMGR, ErrMsg("refcount in buffer desc is overflow, state:%lu", bufState));
    }
    privateRef->refcount++;
}

template<bool isGlobalTempTable>
HOTFUNCTION void BufferDesc::Unpin()
{
    if (isGlobalTempTable) {
        state -= Buffer::BUF_REFCOUNT_ONE;
        StorageReleasePanic(((state & Buffer::BUF_REFCOUNT_MASK) == Buffer::BUF_REFCOUNT_MASK), MODULE_BUFMGR,
                            ErrMsg("double unpin in temp buffer desc, state:%lu", state));
        return;
    }
    BufPrivateRefCount *privateRefCount = thrd->GetBufferPrivateRefCount();
    StorageAssert(privateRefCount != nullptr);

    /* if error happend in GetPrivateRefCountEntry , can not do UnlockBufHdrNew */
    PrivateRefCountEntry *privateRef = privateRefCount->GetPrivateRefcount(this, false, false);
    if (privateRef == nullptr || privateRef->refcount <= 0) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("privateRef is null or refcount <= 0."));
        return;
    }

    privateRef->refcount--;
    StorageReleasePanic(privateRef->refcount < 0, MODULE_BUFMGR,
        ErrMsg("refcount in buffer desc (%hhu, %hu, %u) is overflow, private refcount:%d state:%lu",
            bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId, privateRef->refcount, state));
    if (privateRef->refcount == 0) {
        /* I'd better not still hold any locks on the buffer */
        StorageAssert(!LWLockHeldByMe(&contentLwLock));
        StorageAssert(!LWLockHeldByMe(controller->GetIoInProgressLwLock()));
        SharedUnpin();
        privateRefCount->ForgetPrivateRefcountEntry(privateRef);
    }
}

/* better not to use it directly. the same as SharedPin */
HOTFUNCTION void BufferDesc::SharedUnpin()
{
#ifndef ENABLE_THREAD_CHECK
    StorageAssert(GetRefcount() > 0);
#endif
    /* Decrement the shared reference count */
    uint64 bufState;
    for (;;) {
        /* Increase refcount */
        bufState = GsAtomicFetchSubU64(&state, Buffer::BUF_REFCOUNT_ONE);
        if (bufState & Buffer::BUF_LOCKED) {
            /* Decrease refcount */
            bufState = GsAtomicAddFetchU64(&state, static_cast<int64>(Buffer::BUF_REFCOUNT_ONE));
            (void)WaitHdrUnlock();
            continue;
        }
        StorageReleasePanic(((bufState & Buffer::BUF_REFCOUNT_MASK) == Buffer::BUF_REFCOUNT_MASK), MODULE_BUFMGR,
                            ErrMsg("refcount in buffer desc is overflow, state:%lu", bufState));
        break;
    }
}

HOTFUNCTION void BufferDesc::UnpinForAio()
{
    SharedUnpin();
}

/*
 * Returns true iff the buffer is pinned privately
 * (also checks for valid buffer number).
 *
 * NOTE: what we check here is that *this* backend holds a pin on
 * the buffer. We do not care whether some other backend also does.
 */
bool BufferDesc::IsPinnedPrivately()
{
    BufPrivateRefCount *privateRefCount = thrd->GetBufferPrivateRefCount();
    StorageAssert(privateRefCount != nullptr);

    PrivateRefCountEntry *ref = privateRefCount->GetPrivateRefcount(this, false, true);
    return ((ref != nullptr) && (ref->refcount > 0));
}

bool BufferDesc::IsInDirtyPageQueue(const int64 slotId) const
{
    return recoveryPlsn[slotId].load(std::memory_order_acquire) != INVALID_PLSN;
}

/* Note: acquires wal write LWLock to be released by SetPageEndWriteWal(). */
HOTFUNCTION void BufferDesc::SetPageIsWritingWal()
{
    uint64 oldBufState;
    uint64 bufState;
    StorageReleaseBufferCheckPanic(!LWLockHeldByMeInMode(&contentLwLock, LW_EXCLUSIVE), MODULE_BUFMGR, bufTag,
        "BufferDesc is not locked by me");
    for (;;) {
        oldBufState = WaitHdrUnlock();
        bufState = oldBufState;
        StorageAssert((bufState & Buffer::BUF_IS_WRITING_WAL) != Buffer::BUF_IS_WRITING_WAL);
        StorageReleaseBufferCheckPanic(g_storageInstance->GetType() == StorageInstanceType::DISTRIBUTE_COMPUTE &&
            (bufState & Buffer::BUF_OWNED_BY_ME) == 0, MODULE_BUFMGR, bufTag, "Page not owned when start write wal");
        bufState |= Buffer::BUF_IS_WRITING_WAL;
        if (GsAtomicCompareExchangeU64(&state, &oldBufState, bufState)) {
            break;
        }
    }
}

HOTFUNCTION void BufferDesc::SetPageEndWriteWal()
{
    uint64 oldBufState;
    uint64 bufState;
    for (;;) {
        oldBufState = WaitHdrUnlock();
        bufState = oldBufState;
        StorageAssert((bufState & Buffer::BUF_IS_WRITING_WAL) == Buffer::BUF_IS_WRITING_WAL);
        StorageReleaseBufferCheckPanic(g_storageInstance->GetType() == StorageInstanceType::DISTRIBUTE_COMPUTE &&
            (bufState & Buffer::BUF_OWNED_BY_ME) == 0, MODULE_BUFMGR, bufTag, "Page not owned when end write wal");
        StorageReleaseBufferCheckPanic((bufState & Buffer::BUF_CONTENT_DIRTY) == 0, MODULE_BUFMGR, bufTag,
            "Page not dirty when end write wal");
        bufState &= ~Buffer::BUF_IS_WRITING_WAL;
        if (GsAtomicCompareExchangeU64(&state, &oldBufState, bufState)) {
            break;
        }
    }
}

/*
 * The caller of the function must hold the page lock
 */
HOTFUNCTION void BufferDesc::WaitIfIsWritingWal()
{
    uint64 bufState;
    StorageAssert(LWLockHeldByMe(&contentLwLock));
    /*
     * Note that we must have acquired content lock here, otherwise we could get a spurious buffer state and mistakenly
     * think there is no wal write underway.
     */
    bufState = GetState(false);
    while (bufState & Buffer::BUF_IS_WRITING_WAL) {
        GaussUsleep(1);
        bufState = GetState(false);
    }
}

void BufferDesc::WaitIfIoInProgress()
{
    uint64 bufState = GetState(false);
    while ((bufState & Buffer::BUF_IO_IN_PROGRESS) != 0U) {
        GaussUsleep(1);
        bufState = GetState(false);
    }
}

void BufferDesc::InvalidateCrPage()
{
    PageType pageType = this->GetPage()->GetType();
    if (pageType == PageType::HEAP_PAGE_TYPE || pageType == PageType::INDEX_PAGE_TYPE) {
        AcquireCrAssignLwlock(LW_EXCLUSIVE);
        SetCrUnusable();
        ReleaseCrAssignLwlock();
    }
}
void BufferDesc::PrintBufferDesc(char *str, Size maxSize) const
{
    BufferTag localBufTag = this->bufTag;
    errno_t rc = sprintf_s(str, maxSize, "(bufTag:(%hhu, %hu, %u) State:%lx)",
        localBufTag.pdbId, localBufTag.pageId.m_fileId, localBufTag.pageId.m_blockId, this->state);
    storage_securec_check_ss(rc);
}

char *BufferDesc::PrintBufferDesc()
{
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("cannot allocate memory for bufferDesc dump info."));
        return nullptr;
    }
    dumpInfo.append("Buffer:%p bufTag:(%hhu, %hu, %u)\n", this,
        bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId);

    uint64 bufferState = GetState(false);
    PrivateRefCountEntry *entry = thrd->GetBufferPrivateRefCount()->GetPrivateRefcount(this, false, false);
    dumpInfo.append("state: 0x%016lx pin share:%lu local:%d\n", bufferState, bufferState & Buffer::BUF_REFCOUNT_MASK,
                    entry == nullptr ? -1 : entry->refcount);
    PrintBufferState(bufferState, &dumpInfo);
    dumpInfo.append("\nlru:\n");
    dumpInfo.append("    queue index:%u type:%d usage:%d\n",
                    lruNode.lruIndex, static_cast<int>(lruNode.m_type.load()), lruNode.m_usage.load());

    if (bufferState & Buffer::BUF_CR_PAGE) {
        dumpInfo.append("It is a CR bufferdesc, base bufferdesc :%p \n", crInfo.baseBufferDesc);
    } else {
        const char *str = crInfo.isUsable ? "cr usable" : "cr unusable";
        dumpInfo.append("It is a base bufferdesc, CR buffer desc: %p, %s", crInfo.crBuffer, str);
    }

    dumpInfo.append("buf block:%p\n", bufBlock);

    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR, ErrMsg("%s", dumpInfo.data));

    return dumpInfo.data;
}

void BufferDesc::PrintBufferState(uint64 bufferState, StringInfoData *dumpInfo)
{
    dumpInfo->append("bufState: ");
    uint8 flagCnt = 0;
    PrintBufSingleFlagByState(bufferState, dumpInfo, &flagCnt);
}

const char *BufferDesc::GetBufSingleNodeFlagString(uint64 bufFlagBit) const
{
    /* omit useless namespace prefix in printing */
    switch (bufFlagBit) {
        case Buffer::BUF_LOCKED:
            return "BUF_LOCKED";
        case Buffer::BUF_CONTENT_DIRTY:
            return "BUF_CONTENT_DIRTY";
        case Buffer::BUF_VALID:
            return "BUF_VALID";
        case Buffer::BUF_TAG_VALID:
            return "BUF_TAG_VALID";
        case Buffer::BUF_IO_IN_PROGRESS:
            return "BUF_IO_IN_PROGRESS";
        case Buffer::BUF_IO_ERROR:
            return "BUF_IO_ERROR";
        case Buffer::BUF_HINT_DIRTY:
            return "BUF_HINT_DIRTY";
        case Buffer::BUF_CR_PAGE:
            return "BUF_CR_PAGE";
        case Buffer::BUF_IS_WRITING_WAL:
            return "BUF_IS_WRITING_WAL";
        default:
            break;
    }
    return "INVALID_BUF_FLAG_EN";
}

void BufferDesc::PrintBufSingleFlagByState(uint64 bufferState, StringInfoData *dumpInfo, uint8 *flagCnt)
{
    if (!(bufferState & Buffer::BUF_ALL_SINGLE_FLAGS)) {
        return;
    }

    uint8 flagCntTmp = *flagCnt;
    for (uint8 idx = 0; idx < Buffer::BUF_ALL_SINGLE_FLAGS_NUM; idx++) {
        if (!(bufferState & Buffer::BUF_ALL_SINGLE_FLAGS_ARRAY[idx])) {
            continue;
        }
        if (flagCntTmp % Buffer::BUF_FLAG_MAX_PRINT_ONE_ROW == 0 && flagCntTmp != 0) {
            dumpInfo->append("\n");
        }
        dumpInfo->append("%s|", GetBufSingleNodeFlagString(Buffer::BUF_ALL_SINGLE_FLAGS_ARRAY[idx]));
        flagCntTmp++;
    }
    *flagCnt = flagCntTmp;
}

template void BufferDesc::Pin<true>();
template void BufferDesc::Pin<false>();
template void BufferDesc::Unpin<true>();
template void BufferDesc::Unpin<false>();

} /* namespace DSTORE */

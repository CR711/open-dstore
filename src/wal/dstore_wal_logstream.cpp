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
 * dstore_wal_logstream.cpp
 *
 * IDENTIFICATION
 * src/wal/dstore_wal_logstream.cpp
 *
 * ---------------------------------------------------------------------------------------
 *
 */
#include "wal/dstore_wal_logstream.h"
#include "common/memory/dstore_mctx.h"
#include "common/algorithm/dstore_hsearch.h"
#include "control/dstore_control_file.h"
#include "wal/dstore_wal.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "common/log/dstore_log.h"
#include "port/dstore_port.h"
#include "wal/dstore_wal_reader.h"
#include "wal/dstore_wal_perf_statistic.h"
#include "common/algorithm/dstore_checksum_impl.h"
#include "wal/dstore_wal_perf_unit.h"
#include "common/concurrent/dstore_atomic.h"
#include "lock/dstore_lwlock.h"
#include "buffer/dstore_checkpointer.h"
#include "diagnose/dstore_wal_diagnose.h"
#include "common/fault_injection/dstore_wal_fault_injection.h"
#include "fault_injection/fault_injection.h"
namespace DSTORE {

static constexpr uint32 WAL_WAIT_PLSN_SLEEP = 1;
static timespec g_maxWaitDataTime = {0, 1000 * 300};
static constexpr uint32 MICRO_TO_NANO_FACTOR = 1000;
static constexpr uint8 STREAM_ARRAY_EXPAND_FACTOR = 2;
static constexpr uint32 WAL_WAIT_SLOT_BLOCK_SIZE_SCALE = 30;
static constexpr uint32 WAL_WAIT_SLOTS_SIZE = 2048;

static constexpr uint32 WAL_WAIT_MAX_TIME = 1;
static constexpr uint32 WAL_WAIT_PLSN_LOOP = 10;
static constexpr uint32 WAL_MAX_RETRY_COUNT = 500;
static constexpr uint32 WAL_MAX_RETRY_TIME = 10000;
constexpr uint32 MIN_SINGLE_BUFFER_BLOCK_SIZE = 128;

static const uint32 g_sleepInterval = 10000;   /* 10ms */
static const uint32 g_waitCountThreshold = 6000 * 5;      /* 10ms * 6000 * 5 = 5min */
static const uint32 g_sleepFrequency = 100;  /* 100 * 10ms = 1s */
static const uint32 g_printCount = 500;     /* 10ms * 500 = 5s */

void WalFileFlushCallback(::FileDescriptor *fd, off_t offset, ErrorCode errorCode, void *asyncContext)
{
    if (unlikely(errorCode != 0 || asyncContext == nullptr)) {
        if (errorCode == VFS_ERROR_IO_FENCING_REFUSE) {
            STORAGE_PROCESS_FORCE_EXIT(true,
                "WalFileFlushCallback meets %lld(IO fencing refuse) so we will exit gaussdb process.", errorCode);
        }
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("WalFileFlushCallback func param invalid, errorCode %lld.", errorCode));
        return;
    }
    WalStream *stream = static_cast<WalStream *>(asyncContext);
    stream->HandleFileFlushResult(fd, offset);
}

uint64 ComputeWaitPlsnSlotNo(uint64 plsn)
{
    uint64 plsnWaitSlotBlockSize = g_storageInstance->GetGuc()->bgWalWriterMinBytes / WAL_WAIT_SLOT_BLOCK_SIZE_SCALE;
    uint64 slot = ((plsn - 1) / plsnWaitSlotBlockSize) & (WAL_WAIT_SLOTS_SIZE - 1);
    return slot;
}

WalStream::WalStream(DstoreMemoryContext memoryContext, WalId walId, WalFileManager *walFileManager,
                     uint64 walFileSize, PdbId pdbId)
    : m_memoryContext(memoryContext),
      m_walStreamBuffer(nullptr),
      m_walId(walId),
      m_usage(WalStreamUsage::WAL_STREAM_USAGE_INVALID),
      m_state(WalStreamState::USING),
      m_selfCkpt(false),
      m_isDemoting(false),
      m_isPromoting(false),
      m_bgWalWriter(nullptr),
      m_walFileSize(walFileSize),
      m_pdbId(pdbId),
      m_diorw(WalReadWriteWithDio()),
      m_dioReadBuffer(nullptr),
      m_dioReadBufferSize(0),
      m_dioWriteTailBuffer(nullptr),
      m_maxWrittenToFilePlsn(INVALID_PLSN),
      m_maxFlushFinishPlsn(0),
      m_standbyRedoFinishPlsn(INVALID_PLSN),
      m_walRecovery(memoryContext, this, walFileSize, walId),
      m_diskWalRecovery(memoryContext, this, walFileSize, walId),
      m_redoFinishedPlsn(0),
      m_isInRecovery(false),
      m_walFileManager(walFileManager),
      m_writingWalFile(nullptr),
      m_flushingWalFile(nullptr),
      m_eachWriteLenLimit(USE_PAGE_STORE ? g_storageInstance->GetGuc()->walEachWriteLenghthLimit : walFileSize),
      m_vfs(nullptr),
      m_consensusPlsn(INVALID_END_PLSN),
      m_hasFinishedReceiving(false),
      m_redoThread(nullptr),
      m_standbyRedoThread(nullptr),
      m_plsnWaitSlot(InitWaitPlsnSlots()),
      m_lastSlotNo(0),
      m_diskWalRecoveryThread(nullptr),
      m_diskWalRecoveryNeedStop(false),
      m_needRecoverWithDiskWalRecovery(false),
      m_zeroCopyRegistered(false),
      m_collectWalReadIoStat(false),
      m_collectWalWriteIoStat(false),
      m_readIoStat(),
      m_writeIoStat()
{
}

WalStream::~WalStream()
{
    if (m_asyncStopAndDropThread != nullptr) {
        m_asyncStopAndDropThread->join();
        delete m_asyncStopAndDropThread;
        m_asyncStopAndDropThread = nullptr;
    }
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu] delete m_asyncStopAndDropThread.", m_pdbId, m_walId));
    /* Step 1: Stop bgWriter thread */
    if (m_bgWalWriter != nullptr) {
        m_bgWalWriter->Stop();
    }
    if (m_bgWalWriter != nullptr) {
        delete m_bgWalWriter;
        m_bgWalWriter = nullptr;
    }
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu] delete bgWalWriter.", m_pdbId, m_walId));
    m_standbyRedoThread = nullptr;
    FreeWaitPlsnSlots();
    /* Step 2: delete other resources, m_walStreamBuffer and m_walFileManager not delete by WalStream by corresponding
     * manager */
    m_walRecovery.Destroy();
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu] destroy walRecovery.", m_pdbId, m_walId));
    if (m_diskWalRecoveryThread != nullptr && m_diskWalRecoveryThread->joinable()) {
        m_diskWalRecoveryNeedStop = true;
        m_diskWalRecoveryThread->join();
        delete m_diskWalRecoveryThread;
    }
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu] delete m_diskWalRecoveryThread.", m_pdbId, m_walId));
    m_diskWalRecoveryThread = nullptr;
    m_diskWalRecovery.Destroy();
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu] destroy m_diskWalRecovery.", m_pdbId, m_walId));
    m_memoryContext = nullptr;
    m_walFileManager->Destroy();
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu] destroy m_walFileManager.", m_pdbId, m_walId));
    m_walFileManager = nullptr;
    if (m_walStreamBuffer != nullptr) {
        delete m_walStreamBuffer;
        m_walStreamBuffer = nullptr;
    }
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu] delete m_walStreamBuffer.", m_pdbId, m_walId));

    if (m_dioReadBuffer != nullptr) {
        DstorePfreeAligned(m_dioReadBuffer);
        m_dioReadBuffer = nullptr;
    }
    m_dioReadBufferSize = 0;
    if (m_dioWriteTailBuffer != nullptr) {
        DstorePfreeAligned(m_dioWriteTailBuffer);
        m_dioWriteTailBuffer = nullptr;
    }
    if (m_zeroCopyRegistered) {
        UnsetBufferZeroCopy();
    }
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu] UnsetBufferZeroCopy.", m_pdbId, m_walId));
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu] release WalStream.", m_pdbId, m_walId));
    m_pdbId = INVALID_PDB_ID;
}

void WalStream::Init(WalStreamBuffer *walStreamBuffer)
{
    m_walStreamBuffer = walStreamBuffer;
    StorageReleasePanic(g_storageInstance->GetPdb(m_pdbId) == nullptr, MODULE_WAL,
                        ErrMsg("WalStream init get pdb failed, pdbId(%u).", m_pdbId));
    m_vfs = g_storageInstance->GetPdb(m_pdbId)->GetVFS();

    m_dioWriteTailBuffer = (uint8 *)DstorePallocAligned(WAL_BLCKSZ, WAL_BLCKSZ, m_memoryContext);
    StorageReleasePanic(m_dioWriteTailBuffer == nullptr, MODULE_WAL,
                        ErrMsg("alloc memory for m_dioWriteTailBuffer fail!"));
    errno_t rc = memset_s(m_dioWriteTailBuffer, WAL_BLCKSZ, 0, WAL_BLCKSZ);
    storage_securec_check(rc, "\0", "\0");
}

RetStatus WalStream::Init(WalStreamBuffer **walStreamBuffer)
{
    if (walStreamBuffer != nullptr) {
        if (WalReadWriteWithDio()) {
            if ((WAL_BLCKSZ % WAL_DIO_BLOCK_SIZE != 0)) {
                ErrLog(DSTORE_WARNING, MODULE_WAL,
                    ErrMsg("create dio wal buffer failed for WAL_BLCKSZ:%d WAL_DIO_BLOCK_SIZE:%lu != 0",
                        WAL_BLCKSZ, WAL_DIO_BLOCK_SIZE));
                return DSTORE_FAIL;
            }
        }
        [[maybe_unused]] AutoMemCxtSwitch autoSwitch {m_memoryContext};
        char* alignedMem = (char*)DstorePallocAligned(sizeof(WalStreamBuffer), DSTORE_CACHELINE_SIZE);
        if (alignedMem == nullptr) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("palloc space of alignedMem fail."));
            return DSTORE_FAIL;
        }
        uint32 bufferBlockCount = static_cast<uint32>(g_storageInstance->GetGuc()->walBuffers);
        if (bufferBlockCount < MIN_SINGLE_BUFFER_BLOCK_SIZE) {
            storage_set_error(WAL_ERROR_INVALID_PARAM);
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Pdb %u WalStream %lu bufferBlockCount %u bufferSize %lu",
            m_pdbId, m_walId, bufferBlockCount, static_cast<uint64>(bufferBlockCount) * WAL_BLCKSZ));
        *walStreamBuffer = DstoreNew(alignedMem) WalStreamBuffer(m_memoryContext, bufferBlockCount);
        if (unlikely(*walStreamBuffer == nullptr)) {
            storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
            ErrLog(DSTORE_PANIC, MODULE_WAL,
                ErrMsg("Allocate space OOM, AllocateBuffer fail when try to allocate space for walBuffer."));
            return DSTORE_FAIL;
        }

        if (STORAGE_FUNC_FAIL((*walStreamBuffer)->Init(0, m_walFileSize))) {
            return DSTORE_FAIL;
        }
    }
    Init(walStreamBuffer == nullptr ? nullptr : *walStreamBuffer);
    if (m_walStreamBuffer != nullptr) {
        GsAtomicWriteU64(&m_lastSlotNo, ComputeWaitPlsnSlotNo(m_walStreamBuffer->GetLastScannedPlsn()));
    } else {
        GsAtomicWriteU64(&m_lastSlotNo, 0);
    }
    return DSTORE_SUCC;
}

WalStreamUsage WalStream::GetStreamUsage() const
{
    return m_usage;
}

const char *WalStream::StreamUsageToStr(WalStreamUsage usage)
{
    switch (usage) {
        case WalStreamUsage::WAL_STREAM_USAGE_WRITE_WAL:
            return "WRITE_WAL";
        case WalStreamUsage::WAL_STREAM_USAGE_ONLY_READ:
            return "ONLY_READ";
        default:
            return "INVALID";
    }
}

void WalStream::SetStreamUsage(WalStreamUsage usage)
{
    m_usage = usage;
}

WalStreamState WalStream::GetWalStreamState() const
{
    return m_state;
}

void WalStream::SetStreamSelfCkpt(bool selfCkpt)
{
    m_selfCkpt.store(selfCkpt, std::memory_order_release);
}

bool WalStream::IsStreamSelfCkpt() const
{
    return m_selfCkpt.load(std::memory_order_acquire);
}

void WalStream::SetStreamDemoting(bool isDemoting)
{
    if (IsStreamPromoting()) {
        return; /* promoted wal stream can not demote. */
    }
    m_isDemoting.store(isDemoting, std::memory_order_release);
}

bool WalStream::IsStreamDemoting() const
{
    return m_isDemoting.load(std::memory_order_acquire);
}

void WalStream::SetStreamPromoting(bool isPromoting)
{
    if (IsStreamDemoting()) {
        return; /* demoting wal stream can not promote. */
    }
    m_isPromoting.store(isPromoting, std::memory_order_release);
}

bool WalStream::IsStreamPromoting() const
{
    return m_isPromoting.load(std::memory_order_acquire);
}

void WalStream::SetWalStreamState(WalStreamState state)
{
    if (state == WalStreamState::SYNC_DONE && m_state > WalStreamState::USING) {
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg(
            "Current wal stream %lu state is %u, no need set to %u, pbdId %u.",
            m_walId, static_cast<uint32>(m_state), static_cast<uint32>(state), m_pdbId));
        return; /* do not rollback WalStreamState. */
    }
    m_state = state;
}

const char *WalStream::StreamStateToStr(WalStreamState state)
{
    return g_walStreamStateStr[static_cast<uint8>(state)];
}

void WalStream::InitFlushParams(uint64 lastEndPlsn)
{
    m_maxWrittenToFilePlsn = lastEndPlsn;
    m_maxFlushFinishPlsn = lastEndPlsn;
}

PlsnWaitSlot *WalStream::InitWaitPlsnSlots()
{
    PlsnWaitSlot *plsnWaitSlot = DstoreNew(m_memoryContext) PlsnWaitSlot[WAL_WAIT_SLOTS_SIZE]();
    StorageReleasePanic(plsnWaitSlot == nullptr, MODULE_WAL, ErrMsg("alloc memory for plsnWaitSlot fail!"));
    return plsnWaitSlot;
}

void WalStream::FreeWaitPlsnSlots()
{
    if (m_plsnWaitSlot != nullptr) {
        delete[] m_plsnWaitSlot;
    }
    m_plsnWaitSlot = nullptr;
}

void WalStream::NotifySlotLeaderIfNecessary(uint64 slot)
{
#ifdef UT
    if (m_plsnWaitSlot[slot].GetPlsnWaiterCount() == 0) {
        return;
    }
#endif
    std::unique_lock<std::mutex> slotNotifyLock(m_plsnWaitSlot[slot].m_waitMtx);
    m_plsnWaitSlot[slot].m_waitCv.notify_one();
}

void WalStream::UpdateNowFlushedPlsn(uint64 &nowFlushedPlsn)
{
    nowFlushedPlsn = GsAtomicReadU64(&m_maxFlushFinishPlsn);
    return;
}

void WalStream::WaitPlsnSlots(uint64 slot, uint64 targetPlsn, uint64 nowFlushedPlsn)
{
    uint64 retryTime = 0;
    uint8 sleepTime = 10;
#ifdef UT
    m_plsnWaitSlot[slot].IncreaseWaitCount();
#endif
    FAULT_INJECTION_NOTIFY(DstoreWalFI::WAIT_PLSN_SLOT);
    while (nowFlushedPlsn < targetPlsn) {
        if (unlikely(thrd->GetCore() == nullptr)) {
            GaussUsleep(sleepTime);
        } else {
            /* Let the first worker thread who own the wait lock be blocked by conditional variable. All other threads
             * wait for the lock. Once flush thread notify this cv, the lock owner will release this lock, and let other
             * threads grab the lock by the order recoded in lock wait queue. One thing need to be mentioned here is
             * that LWLockAcquireOrWait only returns true when it is immediately free, otherwise it will wait until lock
             * is free and return false.
             */
            if (LWLockAcquireOrWait(&m_plsnWaitSlot[slot].m_waitLock, LW_EXCLUSIVE)) {
                {
                    std::unique_lock<std::mutex> slotNotifyLock(m_plsnWaitSlot[slot].m_waitMtx);
                    m_plsnWaitSlot[slot].m_waitCv.wait_for(slotNotifyLock,
                                                           std::chrono::milliseconds(WAL_WAIT_MAX_TIME));
                }
                LWLockRelease(&m_plsnWaitSlot[slot].m_waitLock);
            }
        }
        UpdateNowFlushedPlsn(nowFlushedPlsn);
        if (retryTime++ == WAL_MAX_RETRY_TIME) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("Wait PDB:%d Wal:%lu endPlsn:%lu persist, current flushedPlsn is %lu, writtenPlsn is %lu",
                m_pdbId, m_walId, targetPlsn, nowFlushedPlsn, GetMaxWrittenToFilePlsn()));
            retryTime = 0;
        }
    }
#ifdef UT
    m_plsnWaitSlot[slot].DecreaseWaitCount();
#endif
    return;
}

WalId WalStream::GetWalId() const
{
    return m_walId;
}

uint64 WalStream::GetWalFileSize() const
{
    return m_walFileSize;
}

bool WalStream::IsDioReadWrite()
{
    return m_diorw;
}

template<bool isDcf>
void WalStream::CopyToBuffer(const uint8 *data, uint32 walLen, uint64 startPlsn, uint64 endPlsn)
{
    uint64 curPlsn = startPlsn;
    uint8 *bufPos = m_walStreamBuffer->GetBufferBlock(curPlsn);
    /* In DIO mode, the block where endPlsn is located can be reused only when the whole block has been flushed.
       So we add the extra (WAL_BLCKSZ - endPlsn % WAL_BLCKS) to make sure the whole block is flushed.
       (WAL_BLCKSZ - endPlsn % WAL_BLCKS) means the length from endPlsn to the end of the block.
    */
    WaitUntilCorrespondBufferCanReuse(endPlsn + WAL_BLCKSZ - endPlsn % WAL_BLCKSZ);
    uint32 freeSpace = CalculateFreeSpaceInBlock(curPlsn);
    const uint8 *walData = data;
    int rc;
    uint64 bufferSize = m_walStreamBuffer->GetBufferSize();
    if (curPlsn % m_walFileSize == WAL_FILE_HDR_SIZE && !isDcf) {
        WalFileHeaderData *header = STATIC_CAST_PTR_TYPE(bufPos - WAL_FILE_HDR_SIZE, WalFileHeaderData *);
        header->lastRecordRemainLen = 0;
    }
    while (walLen > freeSpace) {
        /* if freespace less than sizeof(int32), need check whether there is a problem */
        /* Write what fits on this seg, and continue on the next seg. */
        rc = memcpy_s(static_cast<void *>(bufPos), bufferSize - (curPlsn % bufferSize), walData, freeSpace);
        storage_securec_check(rc, "\0", "\0");

        walData += freeSpace;
        walLen -= freeSpace;
        curPlsn += freeSpace;

        /* WalStreamBuffer return buffer of Block size each time */
        bufPos = m_walStreamBuffer->GetBufferBlock(curPlsn);
        if (curPlsn % m_walFileSize == 0 && !isDcf) {
            WalFileHeaderData *header = STATIC_CAST_PTR_TYPE(bufPos, WalFileHeaderData *);
            header->lastRecordRemainLen = walLen;
            curPlsn += WAL_FILE_HDR_SIZE;
            bufPos += WAL_FILE_HDR_SIZE;
        }
        freeSpace = CalculateFreeSpaceInBlock(curPlsn);
    }
    rc = memcpy_s(static_cast<void *>(bufPos), freeSpace, walData, walLen);
    storage_securec_check(rc, "\0", "\0");
    curPlsn += walLen;

    if (curPlsn != endPlsn) {
        thrd->GetCore()->threadPerfCounter->walPerfCounter->Reset();
        storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
        ErrLog(DSTORE_PANIC, MODULE_WAL,
               ErrMsg("Plsn is error when insert Wal group to WAL buffer in WalStream Append process."));
    }
}

bool WalStream::IsInRecovering() const
{
    return m_isInRecovery.load(std::memory_order_relaxed);
}

void WalStream::SetInRecovering(bool isInRecovery)
{
    ErrLog(DSTORE_DEBUG1, MODULE_WAL, ErrMsg("SetInRecovering %d", isInRecovery?1:0));
    m_isInRecovery.store(isInRecovery, std::memory_order_relaxed);
}

WalGroupLsnInfo WalStream::SingleAppend(uint8 *data, uint32 len)
{
    /* Step 1: reserve insert location */
    uint64 startPlsn = INVALID_PLSN;
    uint64 endPlsn = INVALID_PLSN;
    m_walStreamBuffer->ReserveInsertLocation(len, startPlsn, endPlsn);
    WalRecordAtomicGroup *group = reinterpret_cast<WalRecordAtomicGroup *>(data);
    uint32 offset = offsetof(WalRecordAtomicGroup, crc) + sizeof((static_cast<WalRecordAtomicGroup *>(nullptr))->crc);
    group->crc = CompChecksum(data + offset, group->groupLen - offset, CHECKSUM_CRC);
    WalGroupLsnInfo result = {m_walId, startPlsn, endPlsn};
    /* Step 2: copy to buffer */
    /* if this wal at the beginning of the segment, should add segment header reserve */
    CopyToBuffer(data, len, startPlsn, endPlsn);

    /* Step 3: mark copy done and change buffer status */
    m_walStreamBuffer->MarkInsertFinish(startPlsn, endPlsn);

    /* Step 4: wake up wal bgwriter if in sleeping */
    m_bgWalWriter->WakeUpIfSleeping();

    return result;
}

void WalStream::StandbyPdbAppend(const uint8 *data, uint32 len, uint64 startPlsn, uint64 endPlsn)
{
    m_walStreamBuffer->SetInsertCtl(endPlsn);
    /* we do not need to update WalFileHeader in this case */
    CopyToBuffer<true>(data, len, startPlsn, endPlsn);
    m_walStreamBuffer->MarkInsertFinish(startPlsn, endPlsn);
    m_bgWalWriter->WakeUpIfSleeping();
}

void WalStream::InsertRecordGroupLeader(ThreadCore *leader)
{
    WalBufferInsertPos insertPos = {};
    uint32 size = leader->walInsertStatus->length;
    uint8 *data = leader->walInsertStatus->data;
    WalGroupLsnInfo *recordPos = leader->walInsertStatus->recordPos;
    WalRecordAtomicGroup *group = reinterpret_cast<WalRecordAtomicGroup *>(data);
    m_walStreamBuffer->ReserveInsertByteLocation(size, insertPos);
    recordPos->m_startPlsn = m_walStreamBuffer->WalBytePosToPlsn(insertPos.startBytePos, false);
    recordPos->m_endPlsn = m_walStreamBuffer->WalBytePosToPlsn(insertPos.endBytePos, true);

    uint32 offset = offsetof(WalRecordAtomicGroup, crc) + sizeof((static_cast<WalRecordAtomicGroup *>(nullptr))->crc);
    group->crc = CompChecksum(data + offset, group->groupLen - offset, CHECKSUM_CRC);

    CopyToBuffer(data, size, recordPos->m_startPlsn, recordPos->m_endPlsn);

    m_walStreamBuffer->MarkInsertFinish(recordPos->m_startPlsn, recordPos->m_endPlsn);
}

void WalStream::InsertRecordGroupFollowers(ThreadCore *leader, uint32 head)
{
    static const uint32 CRC_OFFSET =
        offsetof(WalRecordAtomicGroup, crc) + sizeof((static_cast<WalRecordAtomicGroup *>(nullptr))->crc);
    WalBufferInsertPos insertPos = {};
    uint32 totalSize = 0;
    uint32 recordSize = 0;

    /* Walk the list and update the status of all walinserts. */
    uint32 nextIdx = head;
    uint32 leaderIdx = leader->selfIdx;
    ThreadCore *follower = nullptr;
    WalBatchInsertStatus *status = nullptr;
    ThreadCoreMgr *threadCoreMgr = g_storageInstance->GetThreadCoreMgr();
    uint32 cnt = 0;
    while (nextIdx != leaderIdx) {
        cnt++;
        follower = threadCoreMgr->GetSpecifiedCore(nextIdx);
        if (STORAGE_VAR_NULL(follower)) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("InsertRecordGroupFollowers get specified core failed."));
            return;
        }
        status = follower->walInsertStatus;
        recordSize = status->length;
        StorageAssert(recordSize != 0);
        /* Calculate total size in the group. */
        totalSize += recordSize;

        /* Move to next proc in list. */
        nextIdx = GsAtomicReadU32(&status->walGroupNextMember);
    }
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT, ErrMsg("follower cnt %d totalSize %d!", cnt, totalSize));
#endif
    if (likely(totalSize != 0)) {
        m_walStreamBuffer->ReserveInsertByteLocation(totalSize, insertPos);
    }

    nextIdx = head;
    uint32 size = 0;
    uint8 *data = nullptr;
    /* The lead thread insert wal records in the group one by one. */
    while (nextIdx != leaderIdx) {
        follower = threadCoreMgr->GetSpecifiedCore(nextIdx);
        if (STORAGE_VAR_NULL(follower)) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("InsertRecordGroupFollowers get specified core failed."));
            return;
        }
        status = follower->walInsertStatus;
        size = status->length;
        data = status->data;
        WalRecordAtomicGroup *group = reinterpret_cast<WalRecordAtomicGroup *>(data);

        group->crc = CompChecksum(data + CRC_OFFSET, group->groupLen - CRC_OFFSET, CHECKSUM_CRC);

        status->recordPos->m_startPlsn = m_walStreamBuffer->WalBytePosToPlsn(insertPos.startBytePos, false);
        status->recordPos->m_endPlsn = m_walStreamBuffer->WalBytePosToPlsn(insertPos.startBytePos + size, true);
        CopyToBuffer(data, size, status->recordPos->m_startPlsn, status->recordPos->m_endPlsn);
        insertPos.startBytePos += size;
        /* Move to next proc in list. */
        nextIdx = GsAtomicReadU32(&status->walGroupNextMember);
        m_walStreamBuffer->MarkInsertFinish(status->recordPos->m_startPlsn, status->recordPos->m_endPlsn);
    }
}

static void WakeUpProc(uint32 wakeidx)
{
    ThreadCoreMgr *threadCoreMgr = g_storageInstance->GetThreadCoreMgr();
    ThreadCore *core = nullptr;
    while (wakeidx != INVALID_THREAD_CORE_ID) {
        core = threadCoreMgr->GetSpecifiedCore(wakeidx);
        StorageReleasePanic(core == nullptr, MODULE_WAL, ErrMsg("WakeUpProc get specified core failed."));
        wakeidx = GsAtomicReadU32(&core->walInsertStatus->walGroupNextMember);
        GsAtomicWriteU32(&core->walInsertStatus->walGroupNextMember, INVALID_THREAD_CORE_ID);
        GS_WRITE_BARRIER();
        core->walInsertStatus->isInProgressing = false;
    }
    GS_WRITE_BARRIER();
}

#define FOLLOWER_TRIGER_SLEEP_LOOP_COUNT 1000
#define FOLLOWER_SLEEP_USECS 1
WalGroupLsnInfo WalStream::BatchAppend(uint8 *data, uint32 len)
{
    uint32 numaNum = g_storageInstance->GetGuc()->numaNodeNum;
    if (unlikely(numaNum == 0)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Numa num is 0."));
    }
    ThreadCore *threadCore = thrd->GetCore();

    /* Add ourselves to the list of processes needing a group wal status update. */
    WalGroupLsnInfo result = {m_walId, INVALID_PLSN, INVALID_PLSN};
    threadCore->walInsertStatus->AddWalStatus(&result, data, len);

    int groupNum = static_cast<int>((threadCore->selfIdx / numaNum) % GROUP_INSERT_LOCKS_COUNT);
    if (unlikely((thrd->m_numaId < 0) || static_cast<uint32>(thrd->m_numaId) >= numaNum)) {
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
               ErrMsg("Numa id %d is invalidated with numa Num %u.", thrd->m_numaId, numaNum));
    }
    WalInsertLock *walInsertLock = m_walStreamBuffer->GetWalInsertLock(thrd->m_numaId % numaNum, groupNum);
    if (STORAGE_VAR_NULL(walInsertLock)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("GetWalInsertLock with numa id %d fail.", thrd->m_numaId));
    }

    uint32 nextidx = GsAtomicReadU32(&walInsertLock->walGroupFirst);
    while (true) {
        GsAtomicWriteU32(&threadCore->walInsertStatus->walGroupNextMember, nextidx);

        /* Ensure all previous writes are visible before follower continues. */
        GS_WRITE_BARRIER();

        if (GsAtomicCompareExchangeU32(&walInsertLock->walGroupFirst, &nextidx,
                                       static_cast<uint32>(threadCore->selfIdx))) {
            break;
        }
    }

    /*
     * If the list was not empty, the leader will insert all wal in the same wal insert slot.
     * It is impossible to have followers without a leader because the first process that
     * has added itself to the list will always have nextidx as INVALID_THREAD_CORE_ID.
     */
    if (nextidx != INVALID_THREAD_CORE_ID) {
        int extra_waits = 0;
        /* Sleep until the leader updates our WAL insert status and wakeup me. */
        for (;;) {
            if (++extra_waits % FOLLOWER_TRIGER_SLEEP_LOOP_COUNT == 0) {
                GaussUsleep(FOLLOWER_SLEEP_USECS);
            }
            GS_READ_BARRIER();
            if (!threadCore->walInsertStatus->isInProgressing) {
                break;
            } else {
                (void)sched_yield();
            }
        }

        /*
         * We needs a memory barrier between reading "walGroupMember" and "walGroupReturntRecPtr'
         * in case of memory reordering in relaxed memory model like ARM.
         */
        GS_MEMORY_BARRIER();
        return result;
    }
    /* I am the leader; Let me insert my own WAL first. */
    ThreadCore *leader = threadCore;
    InsertRecordGroupLeader(leader);
    m_bgWalWriter->WakeUpIfSleeping();
    /*
     * Clear the list of processes waiting for group wal insert, saving a pointer to the head of the list.
     * Trying to pop elements one at a time could lead to an ABA problem.
     */
    uint32 head = GsAtomicExchangeU32(&walInsertLock->walGroupFirst, INVALID_THREAD_CORE_ID);

    bool hasFollower = (head != static_cast<uint32>(leader->selfIdx));
    if (hasFollower) {
        /* I have at least one follower and I will next insert their WAL for them. */
        InsertRecordGroupFollowers(leader, head);
        /*
         * Wake all waiting threads up.
         */
        WakeUpProc(head);
    }

    /* Step 2: wake up wal bgwriter if in sleeping */
    m_bgWalWriter->WakeUpIfSleeping();

    return result;
}

WalGroupLsnInfo WalStream::Append(uint8 *data, uint32 len)
{
#ifdef __aarch64__
    if (likely(g_storageInstance->GetGuc()->numaNodeNum != 1)) {
        return BatchAppend(data, len);
    } else {
        return SingleAppend(data, len);
    }
#else
    return SingleAppend(data, len);
#endif
}

inline uint32 WalStream::CalculateFreeSpaceInBlock(uint64 plsn)
{
    return WAL_BLCKSZ - (plsn) % WAL_BLCKSZ;
}

uint64 WalStream::GetMaxFlushedPlsn() const
{
    return GsAtomicReadU64(const_cast<uint64 *>(&m_maxFlushFinishPlsn));
}

constexpr uint32 MAX_SLEEP_WAIT_WAL_SIZE = 1024 * 1024;
void WalStream::WaitTargetPlsnPersist(uint64 targetPlsn)
{
    LatencyStat::Timer timer(&WalPerfUnit::GetInstance().m_waitPlsnFlushedLatency);
    uint64 slot = 0;
    /* If we are in redo wal (recovery or standby), all wal has been flushed, just return success */
    if (IsInRecovering()) {
        return;
    }
    /* Step 1: check if we have already flushed further than targetPlsn, just return if so */
    uint64 nowFlushedPlsn = GsAtomicReadU64(&m_maxFlushFinishPlsn);
    if (targetPlsn <= nowFlushedPlsn) {
        return;
    }

    /* Step 2: wake up wal bgwriter if in sleeping */
    /* not initialize bgWalWriter when in recovery process, but bufferMgr flush pages need call WaitTargetPlsnPersist */
    if (m_bgWalWriter != nullptr) {
        m_bgWalWriter->WakeUpIfSleeping();
    }
    /* Step 3: start waiting on condition var, until flush callback notify condition var is bigger than targetPlsn */
    if (targetPlsn - nowFlushedPlsn < MAX_SLEEP_WAIT_WAL_SIZE) {
        int64 waitCount = 0;
        while (++waitCount <= WAL_WAIT_PLSN_LOOP) {
            GaussUsleep(WAL_WAIT_PLSN_SLEEP);
            nowFlushedPlsn = GsAtomicReadU64(&m_maxFlushFinishPlsn);
            if (nowFlushedPlsn >= targetPlsn) {
                return;
            }
        }
    }

    slot = ComputeWaitPlsnSlotNo(targetPlsn);
    WaitPlsnSlots(slot, targetPlsn, nowFlushedPlsn);

    return;
}

uint64 WalStream::GetMaxAppendedPlsn() const
{
    uint64 plsn = 0;
    if (unlikely(m_walStreamBuffer == nullptr)) {
        return plsn;
    }
    m_walStreamBuffer->GetFinalInsertLocation(plsn);
    return plsn;
}

uint64 WalStream::GetStandbyPdbRecoveryPlsn() const
{
    uint64 recoveryPlsn;
    m_startRecoveryPlsnCnt.fetch_add(1, std::memory_order_acq_rel);
    if ((recoveryPlsn = GetStandbyRedoFinishPlsn()) == INVALID_PLSN) {
        recoveryPlsn = m_walRecovery.GetWorkersRecoveryPlsn();
    }
    m_startRecoveryPlsnCnt.fetch_sub(1, std::memory_order_acq_rel);
    return recoveryPlsn;
}

uint64 WalStream::GetStandbyPdbRedoFinishedPlsn() const
{
    return m_walRecovery.GetCurrentRedoDonePlsn();
}

void WalStream::UpdateMaxFlushedPlsn(uint64 plsn)
{
    (void)WalUtils::TryAtomicSetBiggerU64(&m_maxFlushFinishPlsn, plsn);
    /* not initialize bgWalWriter when in recovery process, but bufferMgr flush pages need call WaitTargetPlsnPersist,
     * and need call HandleFlushResult here to update m_maxFlushFinishPlsn */
    if (m_bgWalWriter != nullptr) {
        m_bgWalWriter->WakeUpIfSleeping();
    }
}


void WalStream::BindBgWalWriter(BgWalWriter *bgWriter)
{
    if (m_bgWalWriter == bgWriter) {
        return;
    }
    if (m_bgWalWriter != nullptr) {
        m_bgWalWriter->Stop();
        delete m_bgWalWriter;
    }
    bgWriter->Run();
    m_bgWalWriter = bgWriter;
}

void WalStream::DestroyBgWalWriter() noexcept
{
    if (m_bgWalWriter != nullptr) {
        m_bgWalWriter->Stop();
        delete m_bgWalWriter;
        m_bgWalWriter = nullptr;
    }
}

void WalStream::WaitAllWritenWalFlushed()
{
    WaitTargetPlsnPersist(GsAtomicReadU64(&m_maxWrittenToFilePlsn));
}

void WalStream::SanityCheckLSN(uint64 startPlsn, uint64 endPlsn) const
{
    StorageAssert(endPlsn >= startPlsn);
    UNUSE_PARAM uint64 offset = startPlsn % m_walFileSize;
    StorageAssert(offset == 0 || offset >= WAL_FILE_HDR_SIZE);
    offset = endPlsn % m_walFileSize;
    StorageAssert(offset == 0 || offset >= WAL_FILE_HDR_SIZE);
    /* No need to protect m_flushedPlsn because is modified below and only in wal bgwriter thread */
    StorageAssert(startPlsn == m_maxWrittenToFilePlsn);
}

uint64 WalStream::Flush()
{
    LatencyStat::Timer timer(&WalPerfUnit::GetInstance().m_walFlushWaitMoreData, false);

    /* Step 1: get flush data from wal buffer */
    uint64 startPlsn;
    uint64 endPlsn;
    uint64 firstStartPlsn = 0;
    uint8 *data = nullptr;
    uint8 *firstDataPtr = nullptr;
    bool reachBufferTail;

    uint64 len = 0;
    struct timespec start;
    struct timespec end;
    if (unlikely(clock_gettime(CLOCK_MONOTONIC, &start) != 0)) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("Get flush start time failed"));
    }

    timer.Start();
    uint64 retryTimes = 0;
    do {
        m_walStreamBuffer->GetNextFlushData(startPlsn, endPlsn, data, reachBufferTail);
        if (firstDataPtr == nullptr) {
            firstStartPlsn = startPlsn;
            firstDataPtr = data;
        }
        len = endPlsn - firstStartPlsn;
        if (unlikely(clock_gettime(CLOCK_MONOTONIC, &end) != 0)) {
            ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("Get flush end time failed"));
        }
        if (len >= g_storageInstance->GetGuc()->bgWalWriterMinBytes ||
            WalUtils::IsOvertimed(start, end, g_maxWaitDataTime) || reachBufferTail) {
            break;
        }
        if (retryTimes++ == WAL_MAX_RETRY_TIME) {
            ErrLog(DSTORE_DEBUG1, MODULE_WAL,
                ErrMsg("[PDB:%d Wal:%lu]Wal flush get no enough data, startPlsn: %lu, endPlsn: %lu, minBytes: %lu",
                m_pdbId, m_walId, startPlsn, endPlsn, g_storageInstance->GetGuc()->bgWalWriterMinBytes));
            retryTimes = 0;
        }
    } while (true);
    timer.End();
    if (IsCollectWalWriteIoStat()) {
        m_writeIoStat.waitCount += 1;
        m_writeIoStat.waitTime += WalUtils::TimeDiffInMicroseconds(start, end);
    }

    /* Step 2: write data to WalFile */
    if (len == 0) {
        return len;
    }
    /* Step 3: check flush data */
    SanityCheckLSN(firstStartPlsn, endPlsn);
    /* Step 4: flush data if write success */
    if (STORAGE_FUNC_FAIL(FlushWalBlock(firstStartPlsn, firstDataPtr, len))) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("FlushWalBlock failed"));
        return 0;
    }

    /* There is no need to release data, whose lifecycle is controlled by WalBuffer */
    uint64 startSlot = GsAtomicReadU64(&m_lastSlotNo);
    uint64 endSlot = ComputeWaitPlsnSlotNo(GsAtomicReadU64(&m_maxFlushFinishPlsn));
    if (startSlot <= endSlot) {
        for (uint64 slotNo = startSlot; slotNo <= endSlot; slotNo++) {
            NotifySlotLeaderIfNecessary(slotNo);
        }
    } else {
        /* Sometimes startSlot is close to the right bound of total slotNo, and endSlot wraparounds to the left. At this
         * moment, startSlotNo is larger than endSlotNo. */
        for (uint64 slotNo = startSlot; slotNo < WAL_WAIT_SLOTS_SIZE; slotNo++) {
            NotifySlotLeaderIfNecessary(slotNo);
        }
        for (uint64 slotNo = 0; slotNo <= endSlot; slotNo++) {
            NotifySlotLeaderIfNecessary(slotNo);
        }
    }
    GsAtomicWriteU64(&m_lastSlotNo, endSlot);
    return len;
}

RetStatus WalStream::FlushWalBlock(uint64 startPlsn, uint8 *data, uint64 dataLen, bool isAsync)
{
    if (unlikely(startPlsn < m_maxWrittenToFilePlsn || startPlsn < m_maxFlushFinishPlsn)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
               ErrMsg("FlushWalBlock startPlsn %lu less than maxWrittenToFilePlsn %lu or "
                      "maxFlushFinishPlsn %lu",
                      startPlsn, m_maxWrittenToFilePlsn, m_maxFlushFinishPlsn));
    }
    LatencyStat::Timer timer(&WalPerfUnit::GetInstance().m_walFlushToDiskLatency);
    uint64 endPlsn = startPlsn + dataLen;

    /* write data to WalFile */
    RetStatus result = DSTORE_SUCC;
    uint64 startTime = 0;
    if (IsCollectWalWriteIoStat()) {
        startTime = GetSystemTimeInMicrosecond();
    }
    result = Write(startPlsn, data, dataLen, isAsync);
    if (STORAGE_FUNC_FAIL(result)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("FlushWalBlock failed to write to file"));
        return DSTORE_FAIL;
    }

    if (IsCollectWalWriteIoStat() && startTime != 0) {
        m_writeIoStat.writeCount += 1;
        m_writeIoStat.writeTime += (GetSystemTimeInMicrosecond() - startTime);
        m_writeIoStat.writeLen += dataLen;
    }

    m_maxWrittenToFilePlsn = endPlsn;

    /* flush data if write success */
    if (IsCollectWalWriteIoStat()) {
       startTime = GetSystemTimeInMicrosecond();
    }
    result = Flush(startPlsn, endPlsn);
    if (STORAGE_FUNC_FAIL(result)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Wal bgwriter flush data failed when call m_walFile to Sync"));
        return DSTORE_FAIL;
    }
    if (!isAsync) {
        UpdateMaxFlushedPlsn(endPlsn);
    }

    if (IsCollectWalWriteIoStat() && startTime != 0) {
        m_writeIoStat.syncCount += 1;
        m_writeIoStat.syncTime += (GetSystemTimeInMicrosecond() - startTime);
        m_writeIoStat.syncLen += dataLen;
    }

    return DSTORE_SUCC;
}

RetStatus WalStream::SetBufferZeroCopy()
{
    if (m_zeroCopyRegistered) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("Try duplicate register zero copy info."));
        return DSTORE_FAIL;
    }
    if (m_walStreamBuffer->GetBufferSize() > UINT32_MAX) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Register buffer exceeds or equal max size:4GB."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("SetBufferZeroCopy BufferSize %lu", m_walStreamBuffer->GetBufferSize()));
    m_zeroCopyRegistered = true;
    return DSTORE_SUCC;
}

void WalStream::UnsetBufferZeroCopy() noexcept
{
    if (!m_zeroCopyRegistered) {
        return;
    }
    m_zeroCopyRegistered = false;
}

RetStatus WalStream::FetchLastWalGroupInfo(uint64 redoPlsn, uint64 &lastGroupEndPlsn)
{
    WalReaderConf conf = {m_walId, redoPlsn, this, nullptr, m_walFileSize, WalReadSource::WAL_READ_FROM_DISK};
    WalRecordReader *reader = DstoreNew(m_memoryContext) WalRecordReader(m_memoryContext, conf);
    if (unlikely(reader == nullptr)) {
        storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Allocate Wal reader memory failed"));
        return DSTORE_FAIL;
    }
    RetStatus walReaderInitResult = reader->Init();
    if (unlikely(walReaderInitResult == DSTORE_FAIL)) {
        delete reader;
        storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Wal reader init failed"));
        return DSTORE_FAIL;
    }
    const WalRecordAtomicGroup *lastGroup = nullptr;
    uint64 endPlsn = redoPlsn;
    while (reader->ReadNext(&lastGroup) == DSTORE_SUCC) {
        if (lastGroup == nullptr) {
            delete reader;
            lastGroupEndPlsn = endPlsn;
            return DSTORE_SUCC;
        }
        while (reader->GetNextWalRecord() != nullptr) {
            endPlsn = reader->GetCurRecordEndPlsn();
        }
    }

    delete reader;
    storage_set_error(WAL_ERROR_INTERNAL_ERROR);
    return DSTORE_FAIL;
}

void WalStream::WaitRecoveryFinish()
{
    if (m_redoThread == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("WaitRecoveryFinish m_redoThread is abnormal."));
        return;
    }
    m_redoThread->join();
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Wal %lu in pdb(%hhu) recovery finish, recovery thread(%lu) exit.",
        m_walId, m_pdbId, m_redoThread->native_handle()));
    delete m_redoThread;
    m_redoThread = nullptr;
}

bool WalStream::IsRecoveryRecycleFinish(bool *isStopRecycle)
{
    return GetWalFileManager()->IsRecoveryRecycleFinish(isStopRecycle);
}

RetStatus WalStream::Recovery(
    PdbId pdbId, RedoMode redoMode,  uint64 term, bool tryRecoveryFromDisk)
{
    /* Step1: Init WalRecovery according to redoMode */
    SetInRecovering(true);
    if (STORAGE_FUNC_FAIL(m_walRecovery.Init(redoMode, pdbId, term))) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("WalRecovery init failed."));
        SetInRecovering(false);
        return DSTORE_FAIL;
    }
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (unlikely(pdb == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("WalRecovery get pdb %u failed.", pdbId));
        SetInRecovering(false);
        return DSTORE_FAIL;
    }
    /* Step2: Do WalRecovery work */
    if (redoMode == RedoMode::RECOVERY_REDO &&
        pdb->GetPdbRoleMode() != PdbRoleMode::PDB_STANDBY &&
        STORAGE_FUNC_FAIL(m_walRecovery.BuildDirtyPageSetAndPageWalRecordListHtab())) {
        SetInRecovering(false);
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(m_walRecovery.Recovery())) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("WalRecovery recovery failed."));
        SetInRecovering(false);
        return DSTORE_FAIL;
    }

    /* Step2: Init disk WalRecovery if necessary */
    if (NeedStartDiskWalRecovery(tryRecoveryFromDisk, &m_walRecovery)) {
        InitAndStartDiskWalRecovery(&m_walRecovery);
    }

    /* Step3: If WalStreamUsage is for write wal, we need to init WalStream plsn info fetch from WalRecovery */
    if (m_usage == WalStreamUsage::WAL_STREAM_USAGE_WRITE_WAL) {
        uint64 lastGroupEndPlsn = m_walRecovery.GetLastGroupEndPlsn();
        if (m_walStreamBuffer != nullptr && STORAGE_FUNC_FAIL(m_walStreamBuffer->InitPlsn(lastGroupEndPlsn, this))) {
            SetInRecovering(false);
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("m_walStreamBuffer InitPlsn failed."));
            return DSTORE_FAIL;
        }
        InitFlushParams(lastGroupEndPlsn);
        Truncate(lastGroupEndPlsn);
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Get last group end plsn:%lu", lastGroupEndPlsn));
        g_maxWaitDataTime.tv_nsec = g_storageInstance->GetGuc()->walFlushTimeout * MICRO_TO_NANO_FACTOR;
        if (redoMode == RedoMode::RECOVERY_REDO) {
            WalStreamManager *walStreamMgr = pdb->GetWalMgr()->GetWalStreamManager();
            if (walStreamMgr->InitWalStreamBgWriter(this) != DSTORE_SUCC) {
                SetInRecovering(false);
                ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("bgWriter init failed."));
                return DSTORE_FAIL;
            }
        }
    }
    SetInRecovering(false);
    /**
     * Update redoFinishedPlsn when redo is running later.
     */
    GsAtomicWriteU64(&m_redoFinishedPlsn, m_walRecovery.GetLastGroupEndPlsn());
    return DSTORE_SUCC;
}

void WalStream::InitAndStartDiskWalRecovery(WalRecovery *walRecovery)
{
    if (m_diskWalRecoveryThread != nullptr && m_diskWalRecoveryThread->joinable()) {
        m_diskWalRecoveryNeedStop = true;
        m_diskWalRecoveryThread->join();
        m_diskWalRecoveryNeedStop = false;
    }

    m_diskWalRecovery.Init(m_walRecovery.GetRedoMode(), m_pdbId, m_walRecovery.GetTerm());
    m_diskWalRecoveryThread = new std::thread(&WalStream::DiskWalRecoveryWorkerMain, this,
        walRecovery->GetDiskRecoveryStartPlsn(), walRecovery->GetRecoveryStartPlsn());
}

void WalStream::CheckpointAfterRedo(PdbId pdbId, uint64 term)
{
    bool dirtyPageFlushed = false;
    WalRecovery *walRecovery = GetWalRecovery();
    SetInRecovering(true);
    uint64 endPlsn = walRecovery->GetLastGroupEndPlsn();
    /* Flush all dirty page in WalRecovery, if success then we try make checkpoint for this WalStream */
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery endPlsn:%lu try flush all dirtypage start",
        pdbId, m_walId, endPlsn));
    if (walRecovery->GetTerm() != term) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]flush all dirtypage failed with endPlsn:%lu at "
            "term %lu, with different walRecovery term %lu.", pdbId, m_walId, endPlsn, term, walRecovery->GetTerm()));
        SetInRecovering(false);
        return;
    }
    if (STORAGE_FUNC_SUCC(walRecovery->FlushAllDirtyPages())) {
        dirtyPageFlushed = true;
    }
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]WalRecovery endPlsn:%lu try flush all walRecovery dirtypage finish",
        pdbId, m_walId, endPlsn));
    walRecovery->Destroy();

    if (dirtyPageFlushed && GetStreamUsage() == WalStreamUsage::WAL_STREAM_USAGE_ONLY_READ) {
        if (NeedStartDiskWalRecovery(true, walRecovery)) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("[PDB:%u WAL:%lu]WalRecovery try flush all diskWalRecovery dirtypage start", pdbId, m_walId));
            if (STORAGE_FUNC_FAIL(GetDiskWalRecovery()->FlushDirtyPages())) {
                ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery try flush dirty pages between "
                    "diskCheckPoint:%lu and recoveryStartPlsn:%lu failed", pdbId, m_walId,
                    GetDiskWalRecovery()->GetRecoveryStartPlsn(), GetDiskWalRecovery()->GetDiskRecoveryStartPlsn()));
                SetInRecovering(false);
                return;
            }
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("[PDB:%u WAL:%lu]WalRecovery try flush all diskWalRecovery dirtypage finish", pdbId, m_walId));
        }
        GetDiskWalRecovery()->Destroy();
        StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
        StorageReleasePanic(pdb == nullptr, MODULE_WAL,
                            ErrMsg("CheckpointAfterRedo get pdb failed, pdbId(%u).", pdbId));
        if (STORAGE_FUNC_FAIL(pdb->CreateCheckpointForWalStream(this, endPlsn))) {
            ErrLog(DSTORE_WARNING, MODULE_WAL,
                ErrMsg("[PDB:%u WAL:%lu]WalRecovery try create checkpoint failed", pdbId, m_walId));
        } else {
            ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]WalRecovery create diskcheckpoint at %lu success "
                "then try mark wal RECOVERY_DROPPING", pdbId, m_walId, endPlsn));
            (void)SetWalStreamState(WalStreamState::RECOVERY_DROPPING);
            WalStreamManager *walStreamMgr = pdb->GetWalMgr()->GetWalStreamManager();
            if (STORAGE_FUNC_FAIL(
                walStreamMgr->UpdateWalStreamStateInControlFile(m_walId, WalStreamState::RECOVERY_DROPPING))) {
                ErrLog(DSTORE_ERROR, MODULE_WAL,
                    ErrMsg("[PDB:%u WAL:%lu]WalRecovery try set wal stream state failed", pdbId, m_walId));
            }
            GetWalFileManager()->StartupRecycleWalFileWorker(true);
        }
    }
    SetInRecovering(false);
}

uint64 WalStream::GetRedoFinishedPlsn()
{
    return GsAtomicReadU64(&m_redoFinishedPlsn);
}

uint64 WalStream::GetMaxWrittenToFilePlsn()
{
    return GsAtomicReadU64(&m_maxWrittenToFilePlsn);
}

void WalStream::WaitUntilCorrespondBufferCanReuse(uint64 endPlsn)
{
    uint32 retryTime = 0;
    if (m_maxFlushFinishPlsn + m_walStreamBuffer->GetBufferSize() >= endPlsn) {
        return;
    }
    ErrLog(DSTORE_WARNING, MODULE_WAL,
           ErrMsg("[PDB:%u WAL:%lu] Wait wal buffer can reuse, maxFlushFinishPlsn:%lu, endPlsn:%lu", m_pdbId, m_walId,
                  m_maxFlushFinishPlsn, endPlsn));
    while (m_maxFlushFinishPlsn + m_walStreamBuffer->GetBufferSize() < endPlsn) {
        std::this_thread::yield();
        if (retryTime++ == WAL_MAX_RETRY_TIME) {
            ErrLog(DSTORE_WARNING, MODULE_WAL,
                   ErrMsg("[PDB:%u WAL:%lu] Wait wal buffer can reuse, maxFlushFinishPlsn:%lu, endPlsn:%lu", m_pdbId,
                          m_walId, m_maxFlushFinishPlsn, endPlsn));
            GaussUsleep(1);
            retryTime = 0;
        }
    }
}

WalRecovery *WalStream::GetWalRecovery()
{
    return &m_walRecovery;
}

RetStatus WalStream::GetRedoInfo(WalRedoInfo *walRedoInfo)
{
    if (m_walRecovery.GetWalRecoveryStage() >= WalRecoveryStage::RECOVERY_REDO_DONE) {
        /* if redo done, no value in RecoveryEndPlsn & CurrentRedoDonePlsn, return process = 100% */
        walRedoInfo->progress = 100;
        return DSTORE_SUCC;
    }
    walRedoInfo->recovery_start_plsn = m_walRecovery.GetRecoveryStartPlsn();
    walRedoInfo->recovery_end_plsn = m_walRecovery.GetRecoveryEndPlsn();
    walRedoInfo->curr_redo_plsn = m_walRecovery.GetCurrentRedoDonePlsn();
    if (walRedoInfo->curr_redo_plsn >= walRedoInfo->recovery_start_plsn &&
        walRedoInfo->recovery_end_plsn > walRedoInfo->recovery_start_plsn) {
        walRedoInfo->progress = static_cast<uint64_t>(
            (static_cast<double>(walRedoInfo->curr_redo_plsn - walRedoInfo->recovery_start_plsn) /
             static_cast<double>(walRedoInfo->recovery_end_plsn - walRedoInfo->recovery_start_plsn)) *
            100.0);
        std::chrono::duration<double> spendSecs =
            std::chrono::high_resolution_clock::now() - m_walRecovery.GetStartRedoTime();
        walRedoInfo->redo_speed = static_cast<uint64>(
            (static_cast<double>(walRedoInfo->curr_redo_plsn - walRedoInfo->recovery_start_plsn) /
             spendSecs.count()));
    }
    return DSTORE_SUCC;
}

WalRecovery *WalStream::GetDiskWalRecovery()
{
    return &m_diskWalRecovery;
}

WalDirtyPageEntry *WalStream::GetDirtyPageEntryArrayCopy(uint64 startPlsn, long &retArraySize)
{
    retArraySize = 0;
    /* wait for dirty page set built here, incase check recovery plsn before init m_walRecovery */
    if (m_walRecovery.GetWalRecoveryStage() == WalRecoveryStage::RECOVERY_DIRTY_PAGE_FLUSHED &&
        startPlsn == m_walRecovery.GetRecoveryEndPlsn()) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("Wal:%lu GetDirtyPageEntryArrayCopy skip because recovery finished", m_walId));
        retArraySize = 0;
        return nullptr;
    }
    m_walRecovery.WaitDirtyPageSetBuilt();
    if (startPlsn == m_walRecovery.GetRecoveryStartPlsn()) {
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Wal:%lu GetDirtyPageEntryArrayCopy of %lu in m_walRecovery",
            m_walId, startPlsn));
        return m_walRecovery.GetDirtyPageEntryArrayCopy(retArraySize);
    }
    if (startPlsn != m_diskWalRecovery.GetRecoveryStartPlsn()) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("Wal:%lu GetDirtyPageEntryArrayCopy of %lu failed, walRecovery %lu diskWalRecovery startPlsn %lu",
                m_walId, startPlsn, m_walRecovery.GetRecoveryStartPlsn(), m_diskWalRecovery.GetRecoveryStartPlsn()));
        return nullptr;
    }

    m_needRecoverWithDiskWalRecovery = true;
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("Wal:%lu GetDirtyPageEntryArrayCopy of plsn %lu in m_walRecovery + m_diskWalRecovery",
            m_walId, startPlsn));
    long walRecoverySize = 0;
    WalDirtyPageEntry *resultPart1 = m_walRecovery.GetDirtyPageEntryArrayCopy(walRecoverySize);
    if (resultPart1 == nullptr) {
        return m_diskWalRecovery.GetDirtyPageEntryArrayCopy(retArraySize);
    }
    long diskWalRecoverySize = 0;
    m_diskWalRecovery.WaitDirtyPageSetBuilt();
    WalDirtyPageEntry *resultPart2 = m_diskWalRecovery.GetDirtyPageEntryArrayCopy(diskWalRecoverySize);
    if (resultPart2 == nullptr) {
        retArraySize = walRecoverySize;
        return resultPart1;
    }

    long totalSize = diskWalRecoverySize + walRecoverySize;
    DstoreMemoryContext oldContext =
        DstoreMemoryContextSwitchTo(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    Size memorySize = static_cast<unsigned long>(totalSize) * sizeof(WalDirtyPageEntry);
    WalDirtyPageEntry *dirtyPageEntryArray = static_cast<WalDirtyPageEntry *>(DstorePalloc(memorySize));
    Size memorySizePart1 = static_cast<unsigned long>(walRecoverySize) * sizeof(WalDirtyPageEntry);
    Size memorySizePart2 = static_cast<unsigned long>(diskWalRecoverySize) * sizeof(WalDirtyPageEntry);
    if (dirtyPageEntryArray == nullptr) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("palloc space of dirtyPageEntryArray size %ld fail.", totalSize));
        goto EXIT;
    }
    DstoreMemcpySafelyForHugeSize(dirtyPageEntryArray, memorySizePart1, resultPart1, memorySizePart1);

    DstoreMemcpySafelyForHugeSize(STATIC_CAST_PTR_TYPE(dirtyPageEntryArray, char *) + memorySizePart1,
        memorySize - memorySizePart1, resultPart2, memorySizePart2);
    retArraySize = walRecoverySize + diskWalRecoverySize;

EXIT:
    DstorePfreeExt(resultPart1);
    DstorePfreeExt(resultPart2);
    (void)DstoreMemoryContextSwitchTo(oldContext);
    return dirtyPageEntryArray;
}

void WalStream::StartStandbyPdbRecovery()
{
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_WAL,
                        ErrMsg("StartStandbyPdbRecovery get pdb failed, pdbId(%u).", m_pdbId));
    if (pdb->GetPdbRoleMode() != PdbRoleMode::PDB_STANDBY) {
        return;
    }
    if (m_standbyRedoThread != nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("Pdb standby redo thread already started."));
        return;
    }
    m_standbyRedoThread = new std::thread(&WalStream::StandbyRedoWalWorkerMain, this, RedoMode::PDB_STANDBY_REDO);
    while (m_walRecovery.GetWalRecoveryStage() != WalRecoveryStage::RECOVERY_REDO_STARTED) {
        GaussUsleep(1000L);
    }
}

void WalStream::StandbyRedoWalWorkerMain(RedoMode redoMode)
{
    WalUtils::SignalBlock();
    (void)g_storageInstance->CreateThreadAndRegister(m_pdbId, false, "worker", true,
                                                     ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);
    (void)pthread_setname_np(pthread_self(), "ReadFromBboxWrk");
    (void)Recovery(m_pdbId, redoMode);
    g_storageInstance->UnregisterThread();
}

void WalStream::DiskWalRecoveryWorkerMain(uint64 startPlsn, uint64 endPlsn)
{
    WalUtils::SignalBlock();
    (void)g_storageInstance->CreateThreadAndRegister(m_pdbId, false, "DiskWalRcyWrk", false);
    (void)pthread_setname_np(pthread_self(), "DiskWalRcyWrk");
    WalUtils::HandleWalThreadCpuBind("DiskWalRcyWrk");
    (void)m_diskWalRecovery.BuildDirtyPageSet(startPlsn, endPlsn);
    WalUtils::HandleWalThreadCpuUnbind("DiskWalRcyWrk");
    g_storageInstance->UnregisterThread();
}

void WalStream::PauseRecycle() const
{
    m_walFileManager->PauseRecycle();
}

void WalStream::RerunRecycle() const
{
    m_walFileManager->RerunRecycle();
}

void WalStream::WriteAdaptedData(bool isAsync, uint8 *adaptedData, uint64 adaptedDataLen,
                                 uint64 adaptedOffset, bool *needWriteTail)
{
    /* needWriteTail false means non-dio mode, write all data directly
       needWriteTail true means dio mode, write alinged data in the previous part.
    */
    RetStatus retStatus = DSTORE_SUCC;
    if (adaptedDataLen > 0) {
        if (isAsync) {
            retStatus = m_writingWalFile->PwriteAsync(adaptedData, adaptedDataLen,
                static_cast<off_t>(adaptedOffset), nullptr);
        } else {
            retStatus = m_writingWalFile->PwriteSync(adaptedData, adaptedDataLen,
                static_cast<off_t>(adaptedOffset));
        }

        if (STORAGE_FUNC_FAIL(retStatus)) {
            storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("PwriteAsync wal file failed."));
        }
    }

    // needWriteTail true means dio mode, write alinged data in the tail part.
    if (*needWriteTail) {
        adaptedOffset += adaptedDataLen;
        if (isAsync) {
            retStatus = m_writingWalFile->PwriteAsync(m_dioWriteTailBuffer, WAL_BLCKSZ,
                static_cast<off_t>(adaptedOffset), nullptr);
        } else {
            retStatus = m_writingWalFile->PwriteSync(m_dioWriteTailBuffer, WAL_BLCKSZ,
                static_cast<off_t>(adaptedOffset));
        }
        if (STORAGE_FUNC_FAIL(retStatus)) {
            storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("PwriteAsync wal file for Dio Tail failed."));
        }
        errno_t rc = memset_s(m_dioWriteTailBuffer, WAL_BLCKSZ, 0, WAL_BLCKSZ);
        storage_securec_check(rc, "\0", "\0");
        *needWriteTail = false;
    }
}

RetStatus WalStream::RemoveAllWalFiles()
{
    if (m_bgWalWriter != nullptr) {
        m_bgWalWriter->Stop();
    }
    if (STORAGE_FUNC_FAIL(m_walFileManager->RemoveAllFiles())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus WalStream::Write(uint64 plsn, uint8 *data, uint64 dataLen, bool isAsync)
{
    uint64 restSpace = 0;
    uint64 curWriteBytes = 0;
    uint64 startOffset = plsn % m_walFileSize;
    if (m_writingWalFile == nullptr) {
        m_writingWalFile = m_walFileManager->GetWalFileByPlsn(plsn);
        m_walFileManager->SetWritingFile(m_writingWalFile);
    }
    uint8 *adaptedData;
    uint64 adaptedDataLen;
    uint64 adaptedOffset;
    while (dataLen > 0) {
        restSpace = m_walFileSize - startOffset;
        curWriteBytes = restSpace <= dataLen ? restSpace : dataLen;
        if (startOffset == 0) {
            SetWalFileHeader(data, plsn);
            if (plsn >= m_writingWalFile->GetStartPlsn() + m_walFileSize) {
                m_writingWalFile = m_walFileManager->GetNextWalFile(m_writingWalFile, true);
                m_walFileManager->SetWritingFile(m_writingWalFile);
            }
            StorageAssert(m_writingWalFile->GetStartPlsn() == plsn);
        }
        /* split the writeBytes, because of the limit of communication and wal file size */
        uint64 limitWriteBytes = 0;
        uint64 targetBytes = curWriteBytes;
        bool needWriteTail = false;
        while (targetBytes > 0) {
            limitWriteBytes = m_eachWriteLenLimit <= targetBytes ? m_eachWriteLenLimit : targetBytes;
            adaptedData = data;
            adaptedDataLen = limitWriteBytes;
            adaptedOffset = startOffset;
            if (unlikely(m_writingWalFile->IsDioRw())) {
                AdaptDioWrite(plsn, &adaptedData, &adaptedDataLen, &adaptedOffset, &needWriteTail);
                m_writingWalFile->SetMaxValidDataOffset(startOffset + limitWriteBytes);
            }

            WriteAdaptedData(isAsync, adaptedData, adaptedDataLen, adaptedOffset, &needWriteTail);

            data += limitWriteBytes;
            targetBytes -= limitWriteBytes;
            startOffset += limitWriteBytes;
            plsn += limitWriteBytes;
        }
        startOffset = plsn % m_walFileSize;
        dataLen -= curWriteBytes;
    }
    return DSTORE_SUCC;
}

void WalStream::SetWalFileHeader(uint8 *data, uint64 startPlsn) const
{
    WalFileHeaderData *walFileHeaderData = STATIC_CAST_PTR_TYPE(data, WalFileHeaderData *);
    walFileHeaderData->fileSize = m_walFileSize;
    walFileHeaderData->timelineId = 0;
    walFileHeaderData->startPlsn = startPlsn;
    walFileHeaderData->version = 0;
    walFileHeaderData->magicNum = WAL_FILE_HEAD_MAGIC;
    walFileHeaderData->crc = WalFileHeaderData::ComputeHdrCrc(data);
}

void WalStream::HandleFileFlushResult(FileDescriptor *fd, off_t offset)
{
    uint64 curStartPlsn;
    uint64 curFileFlushOffset;

    m_walFileManager->GetFileSharedLock();
    WalFile *walFile = m_walFileManager->GetWalFileByFileDescriptor(fd);
    if (STORAGE_VAR_NULL(walFile)) {
        m_walFileManager->ReleaseSharedFileLock();
        fprintf(stderr, "The target wal file don't exist, offset %ld\n", offset);
        return;
    }
    StorageAssert(offset >= 0);
    curStartPlsn = walFile->GetStartPlsn();
    if (unlikely(walFile->IsDioRw())) {
        uint64 validDataOffset = walFile->GetMaxValidDataOffset();
        StorageAssert(validDataOffset < INT64_MAX);
        if (likely(offset > static_cast<off_t>(validDataOffset))) {
            offset = static_cast<off_t>(validDataOffset);
        }
    }
    curFileFlushOffset = curStartPlsn + static_cast<uint64>(offset);
    if (!walFile->SetFlushedPlsn(curFileFlushOffset)) {
        m_walFileManager->ReleaseSharedFileLock();
        return;
    }

    if (GetMaxFlushedPlsn() >= curStartPlsn) {
        UpdateMaxFlushedPlsn(curFileFlushOffset);
        if (static_cast<uint64>(offset) == m_walFileSize) {
            PushEndFlushPlsn(walFile);
        }
    }
    m_walFileManager->ReleaseSharedFileLock();
}

void WalStream::PushEndFlushPlsn(WalFile *walFile)
{
    WalFile *nextFile = m_walFileManager->GetNextWalFile(walFile, false);
    while (nextFile) {
        uint64 afterFileFlushPlsn = nextFile->GetFlushedPlsn();
        UpdateMaxFlushedPlsn(afterFileFlushPlsn);
        /* this segment file not fully flushed, no need to judge next segment file */
        if (afterFileFlushPlsn < nextFile->GetStartPlsn() + m_walFileSize) {
            break;
        }
        nextFile = m_walFileManager->GetNextWalFile(nextFile, false);
    }
}

void WalStream::AdaptDioWrite(uint64 plsn, uint8 **buf, uint64 *count, uint64 *offset, bool *needWriteTail)
{
    uint64 adaptedPlsn = plsn - plsn % WAL_BLCKSZ;
    uint8 *adaptedBuf = m_walStreamBuffer->GetBufferBlock(adaptedPlsn);
    int rc = 0;
    StorageAssert(DstoreIsAlignedAddr(adaptedBuf, WAL_DIO_BLOCK_SIZE));
    uint64 adaptedCount = ((plsn + *count - 1) / WAL_BLCKSZ + 1) * WAL_BLCKSZ - adaptedPlsn;
    if ((plsn + *count) % WAL_BLCKSZ != 0) {
        *needWriteTail = true;
        adaptedCount -= WAL_BLCKSZ;
        rc = memcpy_s(m_dioWriteTailBuffer, WAL_BLCKSZ, adaptedBuf + adaptedCount,
                      plsn + *count - (adaptedPlsn + adaptedCount));
        storage_securec_check(rc, "\0", "\0");
    }
    uint64 adaptedOffset = adaptedPlsn % m_walFileSize;
    *buf = adaptedBuf;
    *count = adaptedCount;
    *offset = adaptedOffset;
}

RetStatus WalStream::Read(uint64 plsn, uint8 *data, uint64 readLen, uint64 *resultLen)
{
    uint64 offset = plsn % m_walFileSize;
    size_t curReadBytes = 0;
    size_t restSpace = 0;
    ssize_t resultSize = 0;
    *resultLen = 0;

    int retryCnt = 0;
    constexpr int retryMax = 1000;
    constexpr int sleepTime = 10;

    WalFile *readFile = m_walFileManager->GetWalFileByPlsn(plsn);
    if (unlikely(readFile == nullptr)) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("WalStream Read not find wal file of plsn %lu.", plsn));
        return DSTORE_SUCC;
    }

    WalDioReadAdaptor dioReadAdaptor{m_memoryContext, WAL_BLCKSZ};
    while (readLen > 0 && readFile != nullptr) {
        restSpace = DstoreMin(m_walFileSize - offset, m_eachWriteLenLimit);
        curReadBytes = readLen < restSpace ? readLen : restSpace;
        uint64 startTime = 0;
        if (IsCollectWalWriteIoStat()) {
            startTime = GetSystemTimeInMicrosecond();
        }

        retryCnt = 0;
RETRY:
        if (unlikely(retryCnt++ >= retryMax)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Pread wal file failed finally. Retry %d times.", retryCnt));
            return DSTORE_FAIL;
        }

        if (readFile->IsDioRw()) {
            if (STORAGE_FUNC_FAIL(dioReadAdaptor.Read(data, curReadBytes, readFile, offset, &resultSize))) {
                ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Pread wal file with dio failed. retry %d times.", retryCnt));
                GaussUsleep(sleepTime);
                goto RETRY;
            }
        } else {
            if (STORAGE_FUNC_FAIL(readFile->Read(data, curReadBytes, offset, &resultSize))) {
                ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Pread wal file failed. retry %d times.", retryCnt));
                GaussUsleep(sleepTime);
                goto RETRY;
            }
        }

        if (resultSize <= 0) {
            return DSTORE_SUCC;
        }
        if (IsCollectWalReadIoStat() && startTime != 0) {
            GsAtomicFetchAddU64(&(m_readIoStat.readCount), 1);
            GsAtomicFetchAddU64(&(m_readIoStat.readLen), curReadBytes);
            GsAtomicFetchAddU64(&(m_readIoStat.actualReadLen), resultSize);
            GsAtomicFetchAddU64(&(m_readIoStat.readTime), GetSystemTimeInMicrosecond() - startTime);
        }

        Size tmpResultSize = static_cast<Size>(resultSize);
        data += tmpResultSize;
        readLen -= tmpResultSize;
        offset += tmpResultSize;
        (*resultLen) += tmpResultSize;

        if (unlikely(offset > m_walFileSize)) {
            ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("WalStream Read result:%ld exceed walFile size: %lu.",
                resultSize, m_walFileSize));
            return DSTORE_FAIL;
        }
        if (offset == m_walFileSize) {
            readFile = m_walFileManager->GetNextWalFile(readFile, false);
            offset = 0;
        }
    }
    return DSTORE_SUCC;
}

uint64 WalStream::GetPrevReadStartPoint(uint64 plsn)
{
    uint64 result = INVALID_PLSN;
    WalFile *plsnLoatedWalFile = m_walFileManager->GetWalFileByPlsn(plsn);
    if (plsnLoatedWalFile == nullptr) {
        return result;
    }
    uint64 firstRecordStartPlsn = plsnLoatedWalFile->GetFirstWalGroupPlsn(m_memoryContext);
    if (firstRecordStartPlsn < plsn) {
        return firstRecordStartPlsn;
    }

    WalFile *nowWalFile = m_walFileManager->GetHeadWalFile();
    if (nowWalFile == nullptr) {
        return result;
    }
    while (nowWalFile != plsnLoatedWalFile) {
        firstRecordStartPlsn = nowWalFile->GetFirstWalGroupPlsn(m_memoryContext);
        if (firstRecordStartPlsn < plsn) {
            result = result < firstRecordStartPlsn ? firstRecordStartPlsn : result;
        }
    }
    return result;
}

FileDescriptor *WalStream::GetWalFileDescriptor(uint64 plsn)
{
    WalFile *walFile = m_walFileManager->GetWalFileByPlsn(plsn);
    if (walFile == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
               ErrMsg("GetWalFileDescriptor of plsn:%lu failed for WalFile not exist", plsn));
        return nullptr;
    }
    return walFile->GetFileDescriptor();
}

RetStatus WalStream::Flush(uint64 startPlsn, uint64 endPlsn)
{
    if (USE_PAGE_STORE) {
        return DSTORE_SUCC;
    }
    if (m_flushingWalFile == nullptr) {
        m_flushingWalFile = m_walFileManager->GetWalFileByPlsn(startPlsn);
    }
    uint64 fileStartPlsn = m_flushingWalFile->GetStartPlsn();
    StorageAssert(startPlsn >= fileStartPlsn && startPlsn < fileStartPlsn + m_walFileSize);
    RetStatus retStatus;
    while (true) {
        retStatus = m_flushingWalFile->Fsync();
        if (STORAGE_FUNC_FAIL(retStatus)) {
            storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Fsync wal file failed."));
            return DSTORE_FAIL;
        }
        uint64 tmpStartPlsn = fileStartPlsn;
        if (endPlsn >= fileStartPlsn + m_walFileSize) {
            m_flushingWalFile = m_walFileManager->GetNextWalFile(m_flushingWalFile, false);
            if (m_flushingWalFile == nullptr) {
                StorageAssert(endPlsn == fileStartPlsn + m_walFileSize);
                break;
            }
            fileStartPlsn = m_flushingWalFile->GetStartPlsn();
        }
        if (endPlsn <= tmpStartPlsn + m_walFileSize) {
            break;
        }
    }
    return DSTORE_SUCC;
}

RetStatus WalStream::Truncate(uint64 plsn)
{
    off_t offset = static_cast<off_t>(plsn % m_walFileSize);
    RetStatus retStatus;

    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("CDB:%d WalStream:%lu Truncateplsn %lu.", m_pdbId, m_walId, plsn));
    WalFile *walFile = m_walFileManager->GetWalFileByPlsn(plsn);
    if (STORAGE_VAR_NULL(walFile)) {
        storage_set_error(WAL_ERROR_INTERNAL_ERROR);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("WalStream Truncate not find wal file of plsn %lu.", plsn));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_INFO, MODULE_WAL, ErrMsg("WalFile StartPlsn:%lu Truncate at %ld.", walFile->GetStartPlsn(), offset));
    retStatus = walFile->Truncate(offset);
    if (STORAGE_FUNC_FAIL(retStatus)) {
        storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Truncate vfs file failed."));
        return DSTORE_FAIL;
    }

    walFile = m_walFileManager->GetNextWalFile(walFile, false);
    while (walFile) {
        ErrLog(DSTORE_INFO, MODULE_WAL, ErrMsg("WalFile StartPlsn:%lu Truncate at %d.",
            walFile->GetStartPlsn(), 0));
        walFile->Truncate(0);
        if (STORAGE_FUNC_FAIL(retStatus)) {
            storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Truncate vfs file failed."));
            return DSTORE_FAIL;
        }
        walFile = m_walFileManager->GetNextWalFile(walFile, false);
    }
    m_maxFlushFinishPlsn = plsn;
    m_maxWrittenToFilePlsn = plsn;
    return DSTORE_SUCC;
}

constexpr uint32 WAIT_FLUSH_MICROSEC = 1000L;
RetStatus WalStream::StopBgWriter()
{
    /* make sure all pending wal threads have been flushed */
    if (m_bgWalWriter != nullptr) {
        uint32 loopCount = 0;
        constexpr uint32 reportCount = 10000;
        while (GetMaxAppendedPlsn() != GetMaxFlushedPlsn()) {
            GaussUsleep(WAIT_FLUSH_MICROSEC);
            if (loopCount++ % reportCount == 0) {
                ErrLog(DSTORE_WARNING, MODULE_WAL,
                       ErrMsg("[PDB:%u WAL:%lu] stop walwriter wait for flush, appendedPlsn = %lu, flushPlsn = %lu",
                              m_pdbId, m_walId, GetMaxAppendedPlsn(), GetMaxFlushedPlsn()));
            }
        }
        m_bgWalWriter->Stop();
        delete m_bgWalWriter;
        m_bgWalWriter = nullptr;
    }
    return DSTORE_SUCC;
}

WalFileManager *WalStream::GetWalFileManager()
{
    return m_walFileManager;
}

uint64 WalStream::GetStandbyMaxFlushedPlsn() const
{
    uint64 startPlsn = GetStandbyPdbRedoFinishedPlsn();
    ErrLog(DSTORE_DEBUG1, MODULE_WAL,
        ErrMsg("GetStandbyFlushedPlsn: walId: %lu, check start plsn: %lu.", m_walId, startPlsn));

    RetStatus ret = DSTORE_SUCC;
    uint32 walTimeLineId = 0;
    uint64 walFileSize = static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize);
    uint64 nextFileStartPlsn = startPlsn - startPlsn % walFileSize;
    uint64 maxFlushedPlsn = INVALID_PLSN;
    char fileName[MAXPGPATH];
    bool fileExist = true;
    char *pdbWalPath = StorageInstance::GetPdbWalPath(g_storageInstance->GetGuc()->dataDir, m_pdbId);
    if (STORAGE_VAR_NULL(pdbWalPath)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetStandbyMaxFlushedPlsn get pdbWalPath failed."));
        return INVALID_PLSN;
    }
    while (fileExist) {
        int result = sprintf_s(fileName, MAXPGPATH, "%s/%08hX_%08X_%016llX", pdbWalPath, m_walId, walTimeLineId,
                               nextFileStartPlsn);
        storage_securec_check_ss(result);
        fileExist = m_vfs->FileExists(fileName);
        if (fileExist) {
            FileDescriptor *fd = nullptr;
            ret = m_vfs->OpenFile(fileName, DSTORE_WAL_FILE_OPEN_FLAG, &fd);
            if (STORAGE_FUNC_FAIL(ret)) {
                ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Check standby flushed plsn failed: can't open wal file: %s",
                    fileName));
                DstorePfreeExt(pdbWalPath);
                return INVALID_PLSN;
            }
            int64 curFileSize = m_vfs->GetSize(fd);
            if (curFileSize < 0) {
                ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Check standby flushed plsn failed: can't get wal file "
                    "size: %s", fileName));
                (void)m_vfs->CloseFile(fd);
                DstorePfreeExt(pdbWalPath);
                return INVALID_PLSN;
            }
            (void)m_vfs->CloseFile(fd);
            if (static_cast<uint64>(curFileSize) < walFileSize) {
                maxFlushedPlsn = nextFileStartPlsn + static_cast<uint64>(curFileSize);
                ErrLog(DSTORE_DEBUG1, MODULE_WAL,
                    ErrMsg("GetStandbyFlushedPlsn: walId: %lu, max flushed plsn: %lu.", m_walId, maxFlushedPlsn));
                DstorePfreeExt(pdbWalPath);
                return maxFlushedPlsn;
            }
            nextFileStartPlsn += walFileSize;
        }
    }
    maxFlushedPlsn = DstoreMax(nextFileStartPlsn, startPlsn);
    ErrLog(DSTORE_DEBUG1, MODULE_WAL,
        ErrMsg("GetStandbyFlushedPlsn: walId: %lu, max flushed plsn: %lu.", m_walId, maxFlushedPlsn));
    DstorePfreeExt(pdbWalPath);
    return maxFlushedPlsn;
}

WalStreamManager::WalStreamManager(DstoreMemoryContext memoryContext)
    : m_memoryContext(memoryContext),
      m_controlFile(nullptr),
      m_pdbId(INVALID_PDB_ID),
      m_writingWalStream(nullptr),
      m_streamAllocatePolicy(WalStreamAllocatePolicy::WAL_ALLOCATE_WRITER_BY_XID),
      m_totalWalStreamsCount(0),
      m_initialized(WalInitState::UNINITIALIZED)
{
    LWLockInitialize(&m_lwlock);
    DListInit(&m_walStreamsListHead);
}

WalStreamManager::~WalStreamManager()
{
    DestroyWalStreams();
    m_controlFile = nullptr;

    m_initialized = WalInitState::UNINITIALIZED;
    m_memoryContext = nullptr;
    m_pdbId = INVALID_PDB_ID;
    m_writingWalStream = nullptr;
}

void WalStreamManager::GetWalStreamFromCtlFile(VnodeControlWalStreamPageItemDatas *walStreamPageItemDatas,
                                               uint16 *streamCount)
{
    if (walStreamPageItemDatas == nullptr || streamCount == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetWalStreamFromCtlFile invalid params."));
        return;
    }
    RetStatus retStatus;
    *streamCount = 0;
    *walStreamPageItemDatas = static_cast<ControlWalStreamPageItemData **>(
        DstorePalloc0(sizeof(ControlWalStreamPageItemData *)));
    while (STORAGE_VAR_NULL(*walStreamPageItemDatas)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetWalStreamFromCtlFile alloc memory fail, retry it"));
        GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
        *walStreamPageItemDatas = static_cast<ControlWalStreamPageItemData **>(
            DstorePalloc0(sizeof(ControlWalStreamPageItemData *)));
    }
    WalId *allStreamIds = nullptr;
    uint32 allStreamCnt = m_controlFile->GetAllWalStreams(&allStreamIds);
    if (STORAGE_VAR_NULL(g_storageInstance->GetPdb(m_pdbId))) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetWalStreamFromCtlFile get pdb failed, pdbId(%u).", m_pdbId));
        return;
    }
    for (uint32 i = 0; i < allStreamCnt; i++) {
        ControlWalStreamPageItemData *pageInfo = nullptr;
        retStatus = m_controlFile->GetWalStreamInfo(allStreamIds[i], &pageInfo);
        if (STORAGE_FUNC_FAIL(retStatus)) {
            ErrLog(DSTORE_ERROR, MODULE_WAL,
                ErrMsg("ControlFile GetWalStreamInfo walId:%lu fail.", allStreamIds[i]));
            continue;
        }
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("GetWalStreamFromCtlFile get walstream walId:%lu for pdb %u. "
            "streamState %u", pageInfo->walId, m_pdbId, pageInfo->streamState));
        if (pageInfo->streamState == static_cast<uint8_t>(WalStreamState::CLOSE_DROPPING)) {
            retStatus = g_storageInstance->GetPdb(m_pdbId)->DeleteWalFileFromVfs(pageInfo);
            if (STORAGE_FUNC_FAIL(retStatus)) {
                ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetWalStreamFromCtlFile failed to delete wal stream:%lu files",
                    pageInfo->walId));
                continue;
            }
            retStatus = m_controlFile->DeleteWalStream(pageInfo->walId);
            if (STORAGE_FUNC_FAIL(retStatus)) {
                ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetWalStreamFromCtlFile failed to delete wal stream:%lu in "
                    "controlfile.", pageInfo->walId));
            }
            continue;
        }
        if (g_storageInstance->GetType() != StorageInstanceType::DISTRIBUTE_COMPUTE &&
            pageInfo->streamState == static_cast<uint8_t>(WalStreamState::USING)) {
            if (*streamCount > 0) {
                ErrLog(DSTORE_PANIC, MODULE_WAL,
                    ErrMsg("GetWalStreamFromCtlFile get invalid using walstream walId:%lu.", allStreamIds[i]));
            }
            retStatus = m_controlFile->GetWalStreamInfo(allStreamIds[i], walStreamPageItemDatas[0]);
            *streamCount += 1;
            if (STORAGE_FUNC_FAIL(retStatus)) {
                ErrLog(DSTORE_PANIC, MODULE_WAL,
                       ErrMsg("ControlFile GetWalStreamInfo walId:%lu fail.", allStreamIds[i]));
            }
        }
        m_controlFile->FreeWalStreamsInfo(pageInfo);
    }
    DstorePfreeExt(allStreamIds);

    if (*streamCount == 0) {
        uint32 retryCount = 0;
        retStatus = m_controlFile->CreateAndAllocateOneWalStream(walStreamPageItemDatas[0], INVALID_WAL_ID);

        while (STORAGE_FUNC_FAIL(retStatus) && retryCount < WAL_MAX_RETRY_COUNT) {
            GaussUsleep(g_sleepInterval);
            retStatus = m_controlFile->CreateAndAllocateOneWalStream(walStreamPageItemDatas[0], INVALID_WAL_ID);
            retryCount++;
        }
        *streamCount += 1;
        if (STORAGE_FUNC_FAIL(retStatus)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("ControlFile GetOneAvailableWalStream fail."));
        }
    }
}

RetStatus WalStreamManager::CreateWritingWalStreamWhenPromoting()
{
    RetStatus result = DSTORE_SUCC;
    VnodeControlWalStreamPageItemDatas streams = nullptr;
    uint16 streamCount = 0;

    if (m_controlFile == nullptr) {
        result = DSTORE_FAIL;
        goto END;
    }

    /* Step 1: fetch all wal stream config from control file */
    GetWalStreamFromCtlFile(&streams, &streamCount);

    if (g_storageInstance->GetGuc()->numaNodeNum == 0) {
        result = DSTORE_FAIL;
        goto END;
    }

    if (streamCount == 0) {
        result = DSTORE_SUCC;
        goto END;
    }

    /* Step 2: init each WalStream */
    for (int i = 0; i < streamCount; i++) {
        ControlWalStreamPageItemData *pageInfo = streams[i];
        /* If any log stream load fail, return error here */
        if ((result = LoadWalStreamFromConfig(*pageInfo, true)) != DSTORE_SUCC) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("LoadWalStreamFromConfig failed when initialize."));
            goto END;
        }
    }

    UpdateWritingWalStream();

    if (g_storageInstance->IsInit() &&
        g_storageInstance->GetType() == StorageInstanceType::DISTRIBUTE_COMPUTE && m_pdbId >= FIRST_USER_PDB_ID) {
        WalStream *walStream = GetWritingWalStream();
        StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);

        /* Step 3: add CheckPoint and start bgwriter */
        StorageReleasePanic((pdb->m_checkpointMgr == nullptr || !pdb->m_checkpointMgr->IsInitialized()), MODULE_WAL,
            ErrMsg("CreateWritingWalStreamWhenPromoting get pdb ckpMgr failed, pdbId(%u).", m_pdbId));
        pdb->m_checkpointMgr->AddOneCheckPoint(walStream->GetWalId(), 0);
        walStream->SetStreamSelfCkpt(false);
        result = pdb->CreateBgWriterThrdForWalStream(walStream, true);
        if (STORAGE_FUNC_FAIL(result)) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("pdb %u walId %lu bgwriter start failed",
                m_pdbId, walStream->GetWalId()));
            goto END;
        }
    }

    if (g_storageInstance->GetType() == StorageInstanceType::DISTRIBUTE_COMPUTE) {
        if (InitWalStreamBgWriter(m_writingWalStream) != DSTORE_SUCC) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("bgWriter init failed."));
            result = DSTORE_FAIL;
            goto END;
        }
    }

    GS_WRITE_BARRIER();
END:
    if (streams != nullptr && streamCount != 0) {
        m_controlFile->FreeWalStreamsInfoArray(streams, streamCount);
    }
    if (result != DSTORE_SUCC) {
        storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Wal stream manager create writing wal stream when promoting failed."));
        return DSTORE_FAIL;
    }
    return result;
}

RetStatus WalStreamManager::Init(ControlFile *controlFile)
{
    if (m_initialized == WalInitState::INITIALIZED) {
        return DSTORE_SUCC;
    }
    RetStatus result = DSTORE_SUCC;
    VnodeControlWalStreamPageItemDatas streams = nullptr;
    uint16 streamCount = 0;
    bool isStandbyPdb = false;

    if (controlFile == nullptr) {
        result = DSTORE_FAIL;
        goto END;
    }

    m_controlFile = controlFile;
    m_pdbId = controlFile->GetPdbId();
    if (g_storageInstance->GetPdb(m_pdbId) == nullptr) {
        result = DSTORE_FAIL;
        goto END;
    }
    isStandbyPdb = g_storageInstance->GetPdb(m_pdbId)->GetPdbRoleMode() == PdbRoleMode::PDB_STANDBY;

    if (g_storageInstance->GetGuc()->numaNodeNum == 0) {
        result = DSTORE_FAIL;
        goto END;
    }

    /* avoid generating extra wal streams in standby pdb */
    if (!isStandbyPdb) {
        /* Step 1: fetch all wal stream config from control file */
        GetWalStreamFromCtlFile(&streams, &streamCount);
        if (streamCount == 0) {
            result = DSTORE_SUCC;
            goto END;
        }

        /* Step 2: init each WalStream */
        for (int i = 0; i < streamCount; i++) {
            ControlWalStreamPageItemData *pageInfo = streams[i];
            /* If any log stream load fail, return error here */
            if ((result = LoadWalStreamFromConfig(*pageInfo)) != DSTORE_SUCC) {
                ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("LoadWalStreamFromConfig failed when initialize."));
                goto END;
            }
        }

        UpdateWritingWalStream();
    }

    GS_WRITE_BARRIER();
    m_initialized = WalInitState::INITIALIZED;
END:
    if (streams != nullptr && streamCount != 0) {
        controlFile->FreeWalStreamsInfoArray(streams, streamCount);
    }
    if (result != DSTORE_SUCC) {
        m_controlFile = nullptr;
        storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Wal stream manager initialize failed."));
        return DSTORE_FAIL;
    }
    return result;
}

RetStatus WalStreamManager::Reload() const
{
    return DSTORE_SUCC;
}

WalStream *WalStreamManager::GetWalStream(WalId walId)
{
    dlist_iter iter;
    if (thrd) {
        LockWalStreamsList(LW_SHARED);
    }
    GS_READ_BARRIER();
    dlist_foreach(iter, &m_walStreamsListHead) {
        WalStreamNode *walStreamNode = dlist_container(WalStreamNode, node, iter.cur);
        if (walStreamNode->walStream == nullptr) {
            continue;
        }
        if (walStreamNode->walStream->GetWalId() == walId) {
            if (thrd) {
                UnLockWalStreamsList();
            }
            return walStreamNode->walStream;
        }
    }
    if (thrd) {
        UnLockWalStreamsList();
    }
    return nullptr;
}

bool WalStreamManager::isWalIdOwnedRedoDone(WalId walId)
{
    dlist_iter iter;
    bool redoDone = false;
    if (thrd) {
        LockWalStreamsList(LW_SHARED);
    }
    GS_READ_BARRIER();
    dlist_foreach(iter, &m_walStreamsListHead) {
        WalStreamNode *walStreamNode = dlist_container(WalStreamNode, node, iter.cur);
        if (walStreamNode->walStream == nullptr) {
            continue;
        }
        if (walStreamNode->walStream->GetWalId() == walId) {
            redoDone = walStreamNode->walStream->IsStreamPromoting() || walStreamNode->walStream->IsStreamDemoting();
            if (thrd) {
                UnLockWalStreamsList();
            }
            return redoDone;
        }
    }
    if (thrd) {
        UnLockWalStreamsList();
    }
    return true;
}

WalStream *WalStreamManager::GetWritingWalStream()
{
    return m_writingWalStream;
}

void WalStreamManager::UpdateWritingWalStream()
{
    dlist_iter iter;
    if (thrd) {
        LockWalStreamsList(LW_SHARED);
    }
    dlist_foreach(iter, &m_walStreamsListHead) {
        WalStreamNode *walStreamNode = dlist_container(WalStreamNode, node, iter.cur);
        if (walStreamNode->walStream == nullptr) {
            continue;
        }
        if (WalStream::IsWalStreamForWrite(walStreamNode->walStream)) {
            m_writingWalStream = walStreamNode->walStream;
            ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("UpdateWritingWalStream %lu for pdb %u",
                m_writingWalStream->GetWalId(), m_pdbId));
            if (thrd) {
                UnLockWalStreamsList();
            }
            return;
        }
    }
    if (thrd) {
        UnLockWalStreamsList();
    }
    m_writingWalStream = nullptr;
    ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("UpdateWritingWalStream no valid walstream for pdb %u", m_pdbId));
}

WalStreamNode *WalStreamManager::GetNextWalStream(dlist_mutable_iter *iter, WalStreamFilter filter, bool shouldRefNode)
{
    WalStreamNode *refNode = nullptr;
    uint32_t refCnt = 0;
    if (thrd) {
        LockWalStreamsList(LW_SHARED);
    } else {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("GetNextWalStream cannot lock walstreams list."));
    }
    GS_READ_BARRIER();

    if (iter->end == nullptr) {
        /* access walStreamList first time. */
        iter->end = &m_walStreamsListHead.head;
        iter->cur = iter->end->next ? iter->end->next : iter->end;
        iter->next = iter->cur->next;
    } else {
        if (unlikely(shouldRefNode)) {
            /* unpin ref node */
            refNode = dlist_container(WalStreamNode, node, iter->cur);
            refCnt = GsAtomicSubFetchU32(&refNode->refCnt, 1);
            ErrLog(DSTORE_LOG, MODULE_WAL,
                    ErrMsg("Sub refCnt of WalStreamNode with pdbId %u stream %lu to %u.", m_pdbId,
                            refNode->walStream ? refNode->walStream->GetWalId() : INVALID_WAL_ID, refCnt));
        }
        iter->cur = iter->cur->next ? iter->cur->next : iter->next;
        iter->next = iter->cur->next;
    }

    refNode = nullptr;
    while (iter->cur != iter->end) {
        WalStreamNode *nextStreamNode = dlist_container(WalStreamNode, node, iter->cur);
        if (nextStreamNode->walStream == nullptr) {
            iter->cur = iter->cur->next ? iter->cur->next : iter->next;
            iter->next = iter->cur->next;
            continue;
        }
        if ((filter != nullptr && filter(nextStreamNode->walStream)) || filter == nullptr) {
            refNode = nextStreamNode;
            break;
        }
        iter->cur = iter->cur->next ? iter->cur->next : iter->next;
        iter->next = iter->cur->next;
    }

    if (unlikely(shouldRefNode && refNode != nullptr)) {
        /* Because this node will be reffered outside the lock, add refCnt to protect it */
        refCnt = GsAtomicAddFetchU32(&refNode->refCnt, 1);
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("Add refCnt of WalStreamNode with pdbId %u walId %lu to %u.", m_pdbId,
                      refNode->walStream ? refNode->walStream->GetWalId() : INVALID_WAL_ID, refCnt));
    }

    if (thrd) {
        UnLockWalStreamsList();
    }
    return refNode;
}

void WalStreamManager::DerefWalStreamNode(WalStreamNode* refNode)
{
    if (STORAGE_VAR_NULL(refNode)) {
        return;
    }

    uint32_t refCnt = GsAtomicSubFetchU32(&refNode->refCnt, 1);
    ErrLog(DSTORE_DEBUG1, MODULE_WAL,
           ErrMsg("Sub refCnt of WalStreamNode with pdbId %u stream %lu to %u.", m_pdbId,
                  refNode->walStream ? refNode->walStream->GetWalId() : INVALID_WAL_ID, refCnt));
    refNode = nullptr;
}

WalStreamNode *WalStreamManager::GetWalStreamNode(WalId walId)
{
    dlist_iter iter;
    if (thrd) {
        LockWalStreamsList(LW_SHARED);
    }
    GS_READ_BARRIER();
    dlist_foreach(iter, &m_walStreamsListHead)
    {
        WalStreamNode *walStreamNode = dlist_container(WalStreamNode, node, iter.cur);
        if (walStreamNode->walStream == nullptr) {
            continue;
        }
        if (walStreamNode->walStream->GetWalId() == walId) {
            uint32_t refCnt = GsAtomicAddFetchU32(&walStreamNode->refCnt, 1);
            ErrLog(DSTORE_LOG, MODULE_WAL,
                   ErrMsg("Add refCnt of WalStreamNode with pdbId %u walId %lu to %u.", m_pdbId,
                          walStreamNode->walStream ? walStreamNode->walStream->GetWalId() : INVALID_WAL_ID, refCnt));
            if (thrd) {
                UnLockWalStreamsList();
            }
            return walStreamNode;
        }
    }
    if (thrd) {
        UnLockWalStreamsList();
    }
    return nullptr;
}

bool WalStreamManager::IsSelfWritingWalStream(WalId walId)
{
    return walId == (m_writingWalStream == nullptr ? INVALID_WAL_ID : m_writingWalStream->GetWalId());
}

uint32 WalStreamManager::GetOwnedStreamIds(WalId **walIdArray, WalStreamFilter filter)
{
    /* Wait until the manager is fully initialized. */
    while (m_initialized != WalInitState::INITIALIZED) {
        GS_READ_BARRIER();
        std::this_thread::yield();
    }

    UNUSE_PARAM AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)};
    const uint32 totalStreamCount = m_totalWalStreamsCount;
    if (totalStreamCount == 0) {
        return 0;
    }

    *walIdArray = static_cast<WalId *>(DstorePalloc(sizeof(WalId) * totalStreamCount));
    while (STORAGE_VAR_NULL(*walIdArray)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetOwnedStreamIds palloc space of walIdArray fail, retry it"));
        GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
        *walIdArray = static_cast<WalId *>(DstorePalloc(sizeof(WalId) * totalStreamCount));
    }
    dlist_iter iter;
    uint32 streamCount = 0;
    LockWalStreamsList(LW_SHARED);
    GS_READ_BARRIER();
    dlist_foreach(iter, &m_walStreamsListHead) {
        WalStreamNode *walStreamNode = dlist_container(WalStreamNode, node, iter.cur);
        if (walStreamNode->walStream == nullptr) {
            continue;
        }
        if ((filter != nullptr && filter(walStreamNode->walStream)) || filter == nullptr) {
            (*walIdArray)[streamCount++] = walStreamNode->walStream->GetWalId();
        }
    }
    UnLockWalStreamsList();
    return streamCount;
}

RetStatus WalStreamManager::GetAllWalStreamInfo(WalStreamStateInfo **walStreamInfo, uint32 *walStreamCount)
{
    LockWalStreamsList(LW_SHARED);
    *walStreamCount = GetTotalWalStreamsCount();
    *walStreamInfo = static_cast<WalStreamStateInfo *>(DstorePalloc(sizeof(WalStreamStateInfo) * (*walStreamCount)));
    if (STORAGE_VAR_NULL(*walStreamInfo)) {
        UnLockWalStreamsList();
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetAllWalStreamInfo palloc space of walStreamInfo fail"));
        return DSTORE_FAIL;
    }
    dlist_iter iter;
    uint32 i = 0;
    dlist_foreach(iter, &m_walStreamsListHead) {
        WalStreamNode *walStreamNode = dlist_container(WalStreamNode, node, iter.cur);
        WalStream *walStream = walStreamNode->walStream;
        if (walStream == nullptr) {
            continue;
        }
        (*walStreamInfo)[i].Init();
        (*walStreamInfo)[i].walId = walStream->GetWalId();
        (*walStreamInfo)[i].pdbId = m_pdbId;
        (*walStreamInfo)[i].nodeId = g_storageInstance->GetGuc()->selfNodeId;
        errno_t nRet = strcpy_s((*walStreamInfo)[i].usage, MAX_STATE_INFO_LEN,
            WalStream::StreamUsageToStr(walStream->GetStreamUsage()));
        storage_securec_check(nRet, "\0", "\0");
        nRet = strcpy_s((*walStreamInfo)[i].state, MAX_STATE_INFO_LEN,
            WalStream::StreamStateToStr(walStream->GetWalStreamState()));
        storage_securec_check(nRet, "\0", "\0");
        (*walStreamInfo)[i].walFileCount =  walStream->GetWalFileManager()->GetAllWalFileCount();
        uint64 headPlsn = walStream->GetWalFileManager()->GetHeadWalFile()->GetStartPlsn();
        int result = sprintf_s((*walStreamInfo)[i].headFileName, MAX_WAL_FILE_NAME_LEN, "%08hX_%08X_%016llX",
            walStream->GetWalId(), 0, headPlsn);
        storage_securec_check_ss(result);
        uint64 tailPlsn = walStream->GetWalFileManager()->GetTailWalFile()->GetStartPlsn();
        result = sprintf_s((*walStreamInfo)[i].tailFileName, MAX_WAL_FILE_NAME_LEN, "%08hX_%08X_%016llX",
            walStream->GetWalId(), 0, tailPlsn);
        storage_securec_check_ss(result);
        if (walStream->GetStreamUsage() == WalStreamUsage::WAL_STREAM_USAGE_WRITE_WAL) {
            (*walStreamInfo)[i].maxAppendedPlsn =  walStream->GetMaxAppendedPlsn();
            (*walStreamInfo)[i].maxWrittenToFilePlsn =  walStream->GetMaxWrittenToFilePlsn();
            (*walStreamInfo)[i].maxFlushFinishPlsn =  walStream->GetMaxFlushedPlsn();
        }
        if (walStream->GetStreamUsage() == WalStreamUsage::WAL_STREAM_USAGE_ONLY_READ) {
            WalRecovery *walRecovery = walStream->GetWalRecovery();
            (*walStreamInfo)[i].term = walRecovery->GetTerm();
            nRet = strcpy_s((*walStreamInfo)[i].redoMode, MAX_STATE_INFO_LEN,
                WalRecovery::RedoModeToStr(walRecovery->GetRedoMode()));
            storage_securec_check(nRet, "\0", "\0");
            (*walStreamInfo)[i].redoStartPlsn =  walRecovery->GetRecoveryStartPlsn();
            (*walStreamInfo)[i].redoFinishedPlsn =  walRecovery->GetRecoveryEndPlsn();
            nRet = strcpy_s((*walStreamInfo)[i].redoStage, MAX_STATE_INFO_LEN,
                WalRecovery::WalRecoveryStageToStr(walRecovery->GetWalRecoveryStage()));
            storage_securec_check(nRet, "\0", "\0");
            (*walStreamInfo)[i].redoDonePlsn =  walRecovery->GetCurrentRedoDonePlsn();
        }
        i++;
    }
    UnLockWalStreamsList();
    return DSTORE_SUCC;
}

RetStatus WalStreamManager::TakeOverStreams(WalId *walIdArray, const uint32 walIdArrayLen)
{
    if (m_initialized != WalInitState::INITIALIZED) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("WalStreamManager not initialied"));
        return DSTORE_FAIL;
    }

    ControlWalStreamPageItemData *streamInfo;
    WalId walId;
    for (uint32 i = 0; i < walIdArrayLen; ++i) {
        walId = walIdArray[i];
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Start collect wal stream %lu at pdbId %hhu.", walId, m_pdbId));
        if (walId == INVALID_WAL_ID || GetWalStream(walId) != nullptr) {
            continue;
        }

        /* No need to return fail here if can not find stream info in control file, because this stream may be deleted
         * by others which means it has been recovery finished and marked dropping.
         */
        if (STORAGE_FUNC_FAIL(m_controlFile->GetWalStreamInfo(walId, &streamInfo))) {
            ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("Get wal stream %lu information from control file fail", walId));
            walIdArray[i] = INVALID_WAL_ID;
            continue;
        }

        if (STORAGE_FUNC_FAIL(LoadWalStreamForRead(*streamInfo))) {
            m_controlFile->FreeWalStreamsInfo(streamInfo);
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Load wal stream %lu for read fail", walId));
            return DSTORE_FAIL;
        }
        if (streamInfo->streamState == static_cast<uint8>(WalStreamState::RECOVERY_DROPPING)) {
            ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Get wal stream %lu already recovery finished", walId));
            walIdArray[i] = INVALID_WAL_ID;
        }
        m_controlFile->FreeWalStreamsInfo(streamInfo);
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Load wal %lu for read success.", walId));
    }
    return DSTORE_SUCC;
}

RetStatus WalStreamManager::LoadWalStreamFromConfig(const ControlWalStreamPageItemData &walStreamConfig,
    bool loadWhenPromoting)
{
    RetStatus result;
    uint64 lastGroupEndPlsn = 0;
    WalFileManager *fileManager = nullptr;
    WalStreamBuffer *buffer = nullptr;
    WalStream *stream = nullptr;
    ControlPdbInfoPageItemData* pdbInfo = nullptr;

    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("LoadWalStreamFromConfig pdb:%d walId %lu recoveryPlsn %lu", m_pdbId, walStreamConfig.walId,
                  walStreamConfig.lastWalCheckpoint.diskRecoveryPlsn));

    /* Step 1: create wal fileManager, buffer, stream */
    result = CreateStream(walStreamConfig, &fileManager, &buffer, &stream, false);
    if (STORAGE_FUNC_FAIL(result) || fileManager == nullptr || buffer == nullptr || stream == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("LoadWalStreamFromConfig failed when CreateStream."));
        return DSTORE_FAIL;
    }

    stream->SetStreamSelfCkpt(loadWhenPromoting);
    stream->SetWalStreamState(WalStreamState::USING);
    stream->SetStreamUsage(WalStreamUsage::WAL_STREAM_USAGE_WRITE_WAL);
    while (STORAGE_FUNC_FAIL(UpdateWalStreamStateInControlFile(walStreamConfig.walId, WalStreamState::USING))) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("LoadWalStreamFromConfig pdb:%d walId %lu failed for set control file "
            "wal stream state to USING failed, retry.", m_pdbId, walStreamConfig.walId));
    }
    /* Step 2: init WalBuffer and WalFileManager, Note stream should init flush parameters after init buffer */
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    if (unlikely(pdb == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("LoadWalStreamFromConfig get pdb failed, pdbId(%u).", g_defaultPdbId));
        return DSTORE_FAIL;
    }
    ControlFile *controlFile = pdb->GetControlFile();
    if (unlikely(controlFile == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
               ErrMsg("LoadWalStreamFromConfig get controlFile failed, pdbId(%u).", g_defaultPdbId));
        return DSTORE_FAIL;
    }
    pdbInfo = controlFile->GetPdbInfoById(m_pdbId);
    StorageReleasePanic(pdbInfo == nullptr, MODULE_PDBREPLICA, ErrMsg("pdbInfo is nullptr for pdbId %hhu", m_pdbId));
    if (pdbInfo->pdbSwitchStatus == PdbSwitchStatus::BEGIN_PROMOTE) {
        result = stream->FetchLastWalGroupInfo(walStreamConfig.lastWalCheckpoint.diskRecoveryPlsn, lastGroupEndPlsn);
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("FetchLastRecordInfo lastEndPlsn:%lu", lastGroupEndPlsn));
        if (STORAGE_FUNC_FAIL(result)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("LoadWalStreamFromConfig failed when FetchLastWalGroupInfo."));
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("PdbId:%d WalStream %lu init on load with lastEndPlsn:%lu", m_pdbId, stream->GetWalId(),
                      lastGroupEndPlsn));
        result = buffer->InitPlsn(lastGroupEndPlsn, stream);
        if (STORAGE_FUNC_FAIL(result)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("LoadWalStreamFromConfig failed when InitPlsn."));
            return DSTORE_FAIL;
        }

        stream->InitFlushParams(lastGroupEndPlsn);
        stream->Truncate(lastGroupEndPlsn);

        /* Step 4: construct BgWalWriter and bind it to WalStream */
        if (!IsTemplate(m_pdbId)) {
            result = InitWalStreamBgWriter(stream);
            if (STORAGE_FUNC_FAIL(result)) {
                ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("LoadWalStreamFromConfig failed when InitWalStreamBgWriter."));
                return DSTORE_FAIL;
            }
        }
    }

    return DSTORE_SUCC;
}

RetStatus WalStreamManager::LoadWalStreamForRead(const ControlWalStreamPageItemData &walStreamConfig)
{
    RetStatus result;
    WalFileManager *fileManager = nullptr;
    WalStream *stream = nullptr;

    /* Step 1: create wal fileManager, buffer, stream */
    result = CreateStream(walStreamConfig, &fileManager, nullptr, &stream, true);
    if (STORAGE_FUNC_FAIL(result) || fileManager == nullptr || stream == nullptr) {
        return DSTORE_FAIL;
    }
    stream->SetStreamSelfCkpt(true);
    stream->SetWalStreamState(static_cast<WalStreamState>(walStreamConfig.streamState));
    stream->SetStreamUsage(WalStreamUsage::WAL_STREAM_USAGE_ONLY_READ);
    if (walStreamConfig.streamState == static_cast<uint8_t>(WalStreamState::RECOVERY_DROPPING)) {
        fileManager->StartupRecycleWalFileWorker(true);
    }
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Load walId %lu stream for read, with state %u, pdb %u.",
        stream->GetWalId(), walStreamConfig.streamState, m_pdbId));
    GS_WRITE_BARRIER();
    return DSTORE_SUCC;
}

RetStatus WalStreamManager::LoadWalStreamForPdbStandbyRedo(const ControlWalStreamPageItemData &walStreamConfig)
{
    RetStatus result;
    WalFileManager *fileManager = nullptr;
    WalStream *stream = nullptr;
    const int wait_checkpointmgr_sleep = 1000;
    uint64 recoveryPlsn = walStreamConfig.lastWalCheckpoint.diskRecoveryPlsn;
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_WAL,
                        ErrMsg("LoadWalStreamForPdbStandbyRedo get pdb failed, pdbId(%u).", m_pdbId));

    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("LoadWalStreamForPdbStandbyRedo walId %lu pdbId %u recoveryPlsn %lu", walStreamConfig.walId, m_pdbId,
        recoveryPlsn));

    /* Step 1: create wal fileManager, buffer, stream */
    result = CreateStream(walStreamConfig, &fileManager, nullptr, &stream, true);
    if (STORAGE_FUNC_FAIL(result) || fileManager == nullptr || stream == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Create walStream walId %lu failed", walStreamConfig.walId));
        goto END;
    }
    stream->SetWalStreamState(static_cast<WalStreamState>(walStreamConfig.streamState));
    if (walStreamConfig.streamState == static_cast<uint8_t>(WalStreamState::RECOVERY_DROPPING)) {
        stream->SetStreamSelfCkpt(true);  /* no need do ckp. */
        fileManager->StartupRecycleWalFileWorker(true);
        goto END;
    } else {
        fileManager->StartupRecycleWalFileWorker(false);
    }

    /* Step2: add CheckPoint and start bgwriter */
    if (pdb->m_checkpointMgr != nullptr) {
        while (!pdb->m_checkpointMgr->IsInitialized()) {
            (void)GaussUsleep(wait_checkpointmgr_sleep);
            ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("pdbId %hhu walId %lu wait checkpointmgr init finish",
                stream->GetPdbId(), stream->GetWalId()));
        }
        pdb->m_checkpointMgr->AddOneCheckPoint(walStreamConfig.walId, walStreamConfig.lastCheckpointPLsn);
        if (STORAGE_FUNC_FAIL(pdb->CreateBgWriterThrdForWalStream(stream, false))) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("walId %lu bgwriter start failed", stream->GetWalId()));
            goto END;
        }
    }

    /* update streamUsage must be after AddOneCheckPoint. */
    stream->SetStreamUsage(WalStreamUsage::WAL_STREAM_USAGE_ONLY_READ);

END:
    if (result != DSTORE_SUCC) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
               ErrMsg("WalStreamManager initialize process failed when LoadWalStreamForRedo."));
        return DSTORE_FAIL;
    }
    GS_WRITE_BARRIER();
    return result;
}

RetStatus WalStreamManager::CreateStream(const ControlWalStreamPageItemData &walStreamConfig,
                                         WalFileManager **walFileManager, WalStreamBuffer **buffer, WalStream **stream,
                                         bool forRead)
{
    RetStatus result;
    int res = 0;
    char *pdbWalPath = StorageInstance::GetPdbWalPath(g_storageInstance->GetGuc()->dataDir, m_pdbId);
    *walFileManager = nullptr;
    if (buffer != nullptr) {
        *buffer = nullptr;
    }
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_WAL, ErrMsg("CreateStream get pdb failed, pdbId(%u).", m_pdbId));

    uint64 walFileSize = static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize);
#ifndef UT
    if (IsTemplate(m_pdbId)) {
        walFileSize = TEMPLATE_WAL_FILE_SIZE;
    }
#endif
    constexpr uint32 maxWalFileCount = 10000;
    uint16 maxFreeWalFileCount = 1;
    InitWalFilesPara para = {};
    FileControlInfo flushCallBackInfo = {};
    FileControlInfo zeroCopyInfo = {};
    WalStreamNode *walStreamNode = nullptr;

    /*
     * Step 1:
     * load WalFileManager by loading all segment files based on config's fileId;
     * including get tail plsn by reading all segment files;
     */

    *walFileManager = DstoreNew(m_memoryContext) WalFileManager(m_memoryContext);
    if (*walFileManager == nullptr) {
        goto CREATE_STREAM_FAIL_END;
    }

    /* Step 2: construct WalStream and add into private stream list */
    *stream = DstoreNew(m_memoryContext)
        WalStream(m_memoryContext, walStreamConfig.walId, *walFileManager, walFileSize, m_pdbId);
    if (STORAGE_VAR_NULL(*stream)) {
        goto CREATE_STREAM_FAIL_END;
    }

    /* Step 3: initialize WalStreamBuffer object, allocate memory space */
    if ((*stream)->Init(buffer) == DSTORE_FAIL) {
        goto CREATE_STREAM_FAIL_END;
    }

    /* Step 4: register WalStream into WalStreamManager */
    walStreamNode = static_cast<WalStreamNode *>(DstoreMemoryContextAllocZero(m_memoryContext, sizeof(WalStreamNode)));
    if (STORAGE_VAR_NULL(walStreamNode)) {
        goto CREATE_STREAM_FAIL_END;
    }
    walStreamNode->walStream = *stream;
    LockWalStreamsList(LW_EXCLUSIVE);
    DListPushTail(&m_walStreamsListHead, &(walStreamNode->node));
    m_totalWalStreamsCount++;
    GS_WRITE_BARRIER();
    UnLockWalStreamsList();
    PrintAllWalStreams(DSTORE_LOG);

    /* Step 5: register WalFileFlushCallback and create files */
    flushCallBackInfo.flushCallbackInfo = {WalFileFlushCallback, *stream};
    if (!forRead && USE_PAGE_STORE) {
        if (buffer != nullptr && (*buffer)->GetBufferSize() > UINT32_MAX) {
            DstorePfreeExt(pdbWalPath);
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Register buffer exceeds or equal max size:4GB."));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL((*stream)->SetBufferZeroCopy())) {
            DstorePfreeExt(pdbWalPath);
            return DSTORE_FAIL;
        }
    }

    para = {{}, pdb->GetVFS(), m_pdbId, walStreamConfig.walId,
            walStreamConfig.initWalFileCount, maxWalFileCount, maxFreeWalFileCount, walFileSize,
            walStreamConfig.lastWalCheckpoint.diskRecoveryPlsn, forRead,
            pdb->GetPdbRoleMode() == PdbRoleMode::PDB_STANDBY,
            walStreamConfig.createFilePara, flushCallBackInfo, zeroCopyInfo};
    res = sprintf_s(para.dir, MAXPGPATH, "%s", pdbWalPath);
    storage_securec_check_ss(res);
    result = (*walFileManager)->InitWalFiles(para, WalReadWriteWithDio());
    if (STORAGE_FUNC_FAIL(result)) {
        goto CREATE_STREAM_FAIL_END;
    }
    DstorePfree(pdbWalPath);
    return DSTORE_SUCC;

CREATE_STREAM_FAIL_END:
    DstorePfree(pdbWalPath);
    if (walStreamNode != nullptr) {
        LockWalStreamsList(LW_EXCLUSIVE);
        DListDelete(&(walStreamNode->node));
        m_totalWalStreamsCount--;
        GS_WRITE_BARRIER();
        UnLockWalStreamsList();
        DstorePfreeExt(walStreamNode);
    }
    delete *walFileManager;
    *walFileManager = nullptr;
    delete *stream;
    *stream = nullptr;
    return DSTORE_FAIL;
}

RetStatus WalStreamManager::InitWalStreamBgWriter(WalStream *stream)
{
    BgWalWriter *bgWriter = nullptr;
    g_maxWaitDataTime.tv_nsec = g_storageInstance->GetGuc()->walFlushTimeout * MICRO_TO_NANO_FACTOR;
    /* construct BgWalWriter and bind it to WalStream */
    StorageReleasePanic(stream == nullptr, MODULE_WAL, ErrMsg("InitWalStreamBgWriter failed, stream is nullptr."));
    bgWriter = DstoreNew(m_memoryContext) BgWalWriter(m_memoryContext, stream, stream->GetPdbId());
    if (STORAGE_VAR_NULL(bgWriter)) {
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(bgWriter->Init())) {
        bgWriter->Stop();
        delete bgWriter;
        return DSTORE_FAIL;
    }
    stream->BindBgWalWriter(bgWriter);
    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("pdb (%u) walId %lu bind BgWalWriter Success", stream->GetPdbId(), stream->GetWalId()));
    return DSTORE_SUCC;
}

RetStatus WalStreamManager::StopWalStreamWriteWal(WalId walId)
{
    WalStream *walStream = GetWalStream(walId);
    if (walStream == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("StopWalStreamWriteWal failed for walstream:%lu not found.", walId));
        return DSTORE_FAIL;
    }
    walStream->DestroyBgWalWriter();
    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("pdb (%u) walId %lu Destroy BgWalWriter Success", walStream->GetPdbId(), walStream->GetWalId()));
    walStream->WaitAllWritenWalFlushed();
    return DSTORE_SUCC;
}

RetStatus WalStreamManager::UpdateWalStreamStateInControlFile(WalId walId, const WalStreamState &state)
{
    if (STORAGE_FUNC_FAIL(m_controlFile->UpdateWalStreamState(walId, static_cast<uint8>(state)))) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg(
            "[PDB:%u Wal:%lu]Mark wal stream state to state:%s failed",
            m_pdbId, walId, WalStream::StreamStateToStr(state)));
        return DSTORE_FAIL;
    } else {
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg(
            "[PDB:%u Wal:%lu]Mark wal stream state to state:%s success",
            m_pdbId, walId, WalStream::StreamStateToStr(state)));
        return DSTORE_SUCC;
    }
}

RetStatus WalStreamManager::StopWalStreamBgWriters()
{
    UNUSE_PARAM StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    if (unlikely(pdb == nullptr || !pdb->IsInit())) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Pdb %u is not initialized.", m_pdbId));
        return DSTORE_FAIL;
    }

    dlist_iter iter;
    LockWalStreamsList(LW_SHARED);
    GS_READ_BARRIER();
    dlist_foreach(iter, &m_walStreamsListHead) {
        WalStreamNode *walStreamNode = dlist_container(WalStreamNode, node, iter.cur);
        if (STORAGE_FUNC_FAIL(walStreamNode->walStream->StopBgWriter())) {
            ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg(
                "Could not stop WalStream (walId %lu) background writer",
                walStreamNode->walStream->GetWalId()));
            UnLockWalStreamsList();
            return DSTORE_FAIL;
        }
    }
    UnLockWalStreamsList();
    return DSTORE_SUCC;
}

void WalStreamManager::PrintAllWalStreams(ErrLevel errLevel)
{
    dlist_iter iter;
    ErrLog(errLevel, MODULE_WAL, ErrMsg("Start PrintAllWalStreams"));
    LockWalStreamsList(LW_SHARED);
    dlist_foreach(iter, &m_walStreamsListHead) {
        WalStreamNode *walStreamNode = dlist_container(WalStreamNode, node, iter.cur);
        WalStream *walStream = walStreamNode->walStream;
        if (walStream == nullptr) {
            ErrLog(errLevel, MODULE_WAL, ErrMsg("[PrintAllWalStreams] WalStream pointer is null"));
            continue;
        }
        ErrLog(errLevel, MODULE_WAL,
            ErrMsg("[PrintAllWalStreams] WalStream %lu, Pdb %d, WalStreamState %u, StreamUsage %u, WalRecoveryStage %u",
            walStream->GetWalId(), walStream->GetPdbId(), static_cast<uint8_t>(walStream->GetWalStreamState()),
            static_cast<uint8_t>(walStream->GetStreamUsage()),
            static_cast<uint8_t>(walStream->GetWalRecovery()->GetWalRecoveryStage())));
    }
    UnLockWalStreamsList();
    ErrLog(errLevel, MODULE_WAL, ErrMsg("End PrintAllWalStreams"));
}

void WalStreamManager::DestroyWalStreams()
{
    WalStreamNode *node = nullptr;
    uint32 retryCnt = 0;
    constexpr uint32 logTrigger = 1000;
    while (unlikely((node = DoDestroyWalStreams()) != nullptr)) {
        if (retryCnt % logTrigger == 0) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                   ErrMsg("DestroyWalStreams is blocked while deleting pdbId %u walId %lu, retry %u times", m_pdbId,
                          node->walStream ? node->walStream->GetWalId() : INVALID_WAL_ID, retryCnt));
        }
        retryCnt++;
        GaussUsleep(10);
    }
    if (unlikely(m_totalWalStreamsCount != 0)) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("after Destroying Wal Streams, m_totalWalStreamsCount is %u", m_totalWalStreamsCount));
    }
    if (unlikely(m_writingWalStream != nullptr)) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("after Destroying Wal Streams, m_writingWalStream is with pdbId %u walId %lu", m_pdbId,
                      m_writingWalStream->GetWalId()));
    }
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("pdb (%u) Destroy all wal streams successfully.", m_pdbId));
}

WalStreamNode* WalStreamManager::DoDestroyWalStreams()
{
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("pdb (%u) start destroy wal streams.",
        m_pdbId));
    UNUSE_PARAM StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_WAL,
                        ErrMsg("DoDestroyWalStreams get pdb failed, pdbId(%u).", m_pdbId));
    WalStreamNode *walStreamNode = NULL;
    dlist_node *cur_node = NULL;

    if (thrd) {
        LockWalStreamsList(LW_EXCLUSIVE);
    }
    
    while (!DListIsEmpty(&m_walStreamsListHead)) {
        cur_node = DListHeadNode(&m_walStreamsListHead);
        walStreamNode = dlist_container(WalStreamNode, node, cur_node);
        if (GsAtomicReadU32(&walStreamNode->refCnt) > 0) {
            break;
        }
        DListPopHeadNode(&m_walStreamsListHead);
        m_totalWalStreamsCount--;
        if (walStreamNode->walStream == nullptr) {
            DstorePfreeExt(walStreamNode);
            walStreamNode = nullptr;
            continue;
        }
        if (walStreamNode->walStream == m_writingWalStream) {
            m_writingWalStream = nullptr;
        }
        WalId walId = walStreamNode->walStream->GetWalId();
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg(
            "pdb (%u) try delete wal stream %lu.", m_pdbId, walId));
        delete walStreamNode->walStream;
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg(
            "pdb (%u) delete wal stream %lu.", m_pdbId, walId));
        DstorePfreeExt(walStreamNode);
        walStreamNode = nullptr;
    }
    GS_WRITE_BARRIER();
    if (thrd) {
        UnLockWalStreamsList();
    }
    return walStreamNode;
}

void WalStreamManager::SetAllWalStreamsUsage(WalStreamUsage usage, WalRecoveryStage stage)
{
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("pdb (%u) start set all wal streams usage.",
        m_pdbId));
    dlist_iter iter;
    LockWalStreamsList(LW_EXCLUSIVE);
    dlist_foreach(iter, &m_walStreamsListHead) {
        WalStreamNode *walStreamNode = dlist_container(WalStreamNode, node, iter.cur);
        WalStream *walStream = walStreamNode->walStream;
        if (walStream == nullptr) {
            continue;
        }
        walStream->SetStreamUsage(usage);
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("pdb (%u) walId %lu update usage to %d.",
            m_pdbId, walStream->GetWalId(), static_cast<int>(usage)));
        WalRecovery *walRecovery = walStream->GetWalRecovery();
        if (STORAGE_VAR_NULL(walRecovery)) {
            continue;
        }
        walRecovery->SetWalRecoveryStage(stage);
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("pdb (%u) walId %lu update recovery stage to %d.",
            m_pdbId, walStream->GetWalId(), static_cast<int>(stage)));
    }
    UnLockWalStreamsList();
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("pdb (%u) set all wal streams usage successfully.",
        m_pdbId));
}

void WalStreamManager::DeleteWalStream(WalId walId)
{
    dlist_mutable_iter iter;
    bool changeWritingWalStream = false;
    /* no wait for lock */
    bool isLocked = false;
    LockWalStreamsList(LW_EXCLUSIVE, false, &isLocked);
    if (!isLocked) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
               ErrMsg("PDB %u DeleteWalStream cannot lock walstreams list.", m_pdbId));
        return;
    }
    if (unlikely(m_writingWalStream != nullptr && walId == m_writingWalStream->GetWalId())) {
        changeWritingWalStream = true;
        m_writingWalStream = nullptr;
    }
    dlist_foreach_modify(iter, &m_walStreamsListHead) {
        WalStreamNode *walStreamNode = dlist_container(WalStreamNode, node, iter.cur);
        if (walStreamNode->walStream == nullptr) {
            if (GsAtomicReadU32(&walStreamNode->refCnt) > 0) {
                /* This node is accessing outside the lock. */
                continue;
            }
            DListDelete(iter.cur);
            m_totalWalStreamsCount--;
            DstorePfreeExt(walStreamNode);
            continue;
        }
        if (walStreamNode->walStream->GetWalId() == walId) {
            if (GsAtomicReadU32(&walStreamNode->refCnt) > 0) {
                /* This node is accessing outside the lock. */
                /* As we no need to wait, we also no need to retry */
                UnLockWalStreamsList();
                ErrLog(DSTORE_LOG, MODULE_WAL,
                       ErrMsg("PDB %u DeleteWalStream delete wal stream %lu failed because it's being reffered. ",
                              m_pdbId, walId));
                if (unlikely(changeWritingWalStream)) {
                    UpdateWritingWalStream();
                }
                return;
            }
            DListDelete(iter.cur);
            m_totalWalStreamsCount--;
            delete walStreamNode->walStream;
            walStreamNode->walStream = nullptr;
            DstorePfreeExt(walStreamNode);
            break;
        }
    }
    UnLockWalStreamsList();
    if (unlikely(changeWritingWalStream)) {
        UpdateWritingWalStream();
    }
    GS_WRITE_BARRIER();
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("PDB %u DeleteWalStream delete wal stream %lu successfully", m_pdbId, walId));
    return;
}

void WalStreamManager::DeleteDroppedWalStream()
{
    WalStreamNode *walStreamNode = nullptr;
    WalStream *walStream = nullptr;
    bool changeWritingWalStream = false;
    WalId writtingWalId = INVALID_WAL_ID;
    bool isLocked = false;
    LockWalStreamsList(LW_EXCLUSIVE, false, &isLocked);
    if (unlikely(!isLocked)) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
               ErrMsg("PDB %u DeleteDroppedWalStream cannot lock walstreams list.", m_pdbId));
        return;
    }

    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("PDB %u DeleteDroppedWalStream begin to delete walstreams list.", m_pdbId));

    GS_READ_BARRIER();
    if (m_writingWalStream != nullptr) {
        writtingWalId = m_writingWalStream->GetWalId();
    }

    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &m_walStreamsListHead)
    {
        walStreamNode = dlist_container(WalStreamNode, node, iter.cur);
        if (unlikely(walStreamNode->walStream == nullptr)) {
            if (GsAtomicReadU32(&walStreamNode->refCnt) > 0) {
                /* This node is accessing outside the lock, but it should not be null walstream */
                ErrLog(DSTORE_WARNING, MODULE_WAL,
                       ErrMsg("PDB %u DeleteDroppedWalStream delete walstream node without walstream.", m_pdbId));
            }
            DListDelete(iter.cur);
            m_totalWalStreamsCount--;
            DstorePfreeExt(walStreamNode);
            continue;
        }
        walStream = walStreamNode->walStream;
        WalId walId = walStream->GetWalId();
        bool isStopRecycle = false;
        /* cases that cannot be deleted now */
        if (GsAtomicReadU32(&walStreamNode->refCnt) > 0 || !WalStream::IsWalStreamRecoveryDropping(walStream) ||
            !walStream->IsRecoveryRecycleFinish(&isStopRecycle) || isStopRecycle) {
            continue;
        }
        DListDelete(iter.cur);
        m_totalWalStreamsCount--;
        delete walStreamNode->walStream;
        walStreamNode->walStream = nullptr;
        DstorePfreeExt(walStreamNode);
        if (unlikely(walId == writtingWalId)) {
            changeWritingWalStream = true;
            m_writingWalStream = nullptr;
            ErrLog(
                DSTORE_LOG, MODULE_WAL,
                ErrMsg("PDB %u DeleteDroppedWalStream delete writting wal stream %lu successfully", m_pdbId, walId));
        } else {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                    ErrMsg("PDB %u DeleteDroppedWalStream delete wal stream %lu successfully", m_pdbId, walId));
        }
    }
    UnLockWalStreamsList();
    if (unlikely(changeWritingWalStream)) {
        UpdateWritingWalStream();
    }
    GS_WRITE_BARRIER();

    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("PDB %u DeleteDroppedWalStream finish deleting walstreams list.", m_pdbId));
    return;
}

void WalStreamManager::GetStandbyWalStreamTransInfo(StandbyWalStreamTransInfo *&walStreamTransInfos,
    uint32 &walStreamCount)
{
    AutoMemCxtSwitch autoSwitch(m_memoryContext);
    WalId *ownedWalStreamIds = nullptr;
    walStreamCount = GetOwnedStreamIds(&ownedWalStreamIds, WalStream::IsWalStreamUsing);
    if (walStreamCount == 0) {
        walStreamTransInfos = nullptr;
        return;
    }
    StorageAssert(ownedWalStreamIds != nullptr);
    walStreamTransInfos =
        static_cast<StandbyWalStreamTransInfo *>(DstorePalloc(sizeof(StandbyWalStreamTransInfo) * walStreamCount));
    StorageReleasePanic(STORAGE_VAR_NULL(walStreamTransInfos), MODULE_WAL, ErrMsg("Failed to allocate memory!"));
    for (uint32 i = 0; i < walStreamCount; i++) {
        WalId walId = ownedWalStreamIds[i];
        WalStream *walStream = GetWalStream(walId);
        StorageAssert(!STORAGE_VAR_NULL(walStream));
        uint64 redoFinishPlsn = walStream->GetStandbyPdbRedoFinishedPlsn();
        uint64 maxFlushedPlsn = walStream->GetStandbyMaxFlushedPlsn();
        walStreamTransInfos[i].walId = walId;
        walStreamTransInfos[i].maxFlushedPlsn = maxFlushedPlsn;
        walStreamTransInfos[i].maxReplayedPlsn = redoFinishPlsn;
    }
}

RetStatus WalStreamManager::StartCollectWalReadIoStat(WalId walId, bool *found)
{
    WalStream *walStream = GetWalStream(walId);
    if (walStream == nullptr) {
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("StartCollectWalReadIoStat get invalid walStream, walId:%lu.", walId));
        *found = false;
        return DSTORE_FAIL;
    }
    *found = true;
    if (walStream->GetStreamUsage() != WalStreamUsage::WAL_STREAM_USAGE_ONLY_READ) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("StartCollectWalReadIoStat failed due to walstream is not for read, walId:%lu.", walId));
        return DSTORE_FAIL;
    }
    walStream->StartCollectWalReadIoStat();
    return DSTORE_SUCC;
}

RetStatus WalStreamManager::StartCollectWalWriteIoStat(WalId walId, bool *found)
{
    WalStream *walStream = GetWalStream(walId);
    if (walStream == nullptr) {
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("StartCollectWalWriteIoStat get invalid walStream, walId:%lu.", walId));
        *found = false;
        return DSTORE_FAIL;
    }
    *found = true;
    if (walStream->GetStreamUsage() != WalStreamUsage::WAL_STREAM_USAGE_WRITE_WAL) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("StartCollectWalWriteIoStat failed due to walstream is not for write, walId:%lu.", walId));
        return DSTORE_FAIL;
    }
    walStream->StartCollectWalWriteIoStat();
    return DSTORE_SUCC;
}

RetStatus WalStreamManager::StopCollectWalReadIoStat(WalId walId, WalReadIoStat *walReadIoStat, bool *found)
{
    WalStream *walStream = GetWalStream(walId);
    if (walStream == nullptr) {
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("StopCollectWalReadIoStat get invalid walStream, walId:%lu.", walId));
        *found = false;
        return DSTORE_FAIL;
    }
    *found = true;
    if (!walStream->IsCollectWalReadIoStat()) {
        ErrLog(
            DSTORE_LOG, MODULE_WAL,
            ErrMsg("StopCollectWalReadIoStat failed due to WalStream not collecting WalReadIoStat, walId:%lu.", walId));
        return DSTORE_FAIL;
    }
    walStream->StopCollectWalReadIoStat();
    if (walReadIoStat == nullptr) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("StopCollectWalReadIoStat get invalid walReadIoStat, walId:%lu.", walId));
        return DSTORE_FAIL;
    }
    walReadIoStat->Copy(walStream->GetWalReadIoStat());
    return DSTORE_SUCC;
}

RetStatus WalStreamManager::StopCollectWalWriteIoStat(WalId walId, WalWriteIoStat *walWriteIoStat, bool *found)
{
    WalStream *walStream = GetWalStream(walId);
    if (walStream == nullptr) {
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("StopCollectWalWriteIoStat get invalid walStream, walId:%lu.", walId));
        *found = false;
        return DSTORE_FAIL;
    }
    *found = true;
    if (!walStream->IsCollectWalWriteIoStat()) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("StopCollectWalWriteIoStat failed due to WalStream not collecting WalWriteIoStat, walId:%lu.",
                      walId));
        return DSTORE_FAIL;
    }
    walStream->StopCollectWalWriteIoStat();
    if (walWriteIoStat == nullptr) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("StopCollectWalWriteIoStat get invalid walWriteIoStat, walId:%lu.", walId));
        return DSTORE_FAIL;
    }
    walWriteIoStat->Copy(walStream->GetWalWriteIoStat());
    return DSTORE_SUCC;
}

void WalStreamManager::PauseWalFileRecycle()
{
    dlist_mutable_iter iter = {};
    WalStreamNode *walStreamNode = nullptr;
    WalStream *walStream = nullptr;
    while ((walStreamNode = GetNextWalStream(&iter, WalStream::IsWalStreamForWrite)) != nullptr) {
        walStream = walStreamNode->walStream;
        walStream->GetWalFileManager()->SetPauseWalFileRecycleFlag(true);
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Pause wal recycle, walId: %lu", walStream->GetWalId()));
    }
}

void WalStreamManager::ResumeWalFileRecycle()
{
    dlist_mutable_iter iter = {};
    WalStreamNode *walStreamNode = nullptr;
    WalStream *walStream = nullptr;
    while ((walStreamNode = GetNextWalStream(&iter, WalStream::IsWalStreamForWrite)) != nullptr) {
        walStream = walStreamNode->walStream;
        walStream->GetWalFileManager()->SetPauseWalFileRecycleFlag(false);
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Resume wal recycle, walId: %lu", walStream->GetWalId()));
    }
}
}  // namespace DSTORE

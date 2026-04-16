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

#ifndef DSTORE_BUFFER_ERROR_CODE_MAP_H
#define DSTORE_BUFFER_ERROR_CODE_MAP_H

#include "errorcode/dstore_buf_error_code.h"

namespace DSTORE {

static ErrorDetails g_buffer_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(BUFFER_WARNING_NO_AVAILABLE_BUFFER_CAN_USE)] =
        {"BUFFER_WARNING_NO_AVAILABLE_BUFFER_CAN_USE", "After retry serval times, no available buffer can be used."},
    [ERROR_GET_CODE(BUFFER_WARNING_FAIL_TO_INVALIDATE_BASE_BUFFER)] =
        {"BUFFER_WARNING_FAIL_TO_INVALIDATE_BASE_BUFFER", "Failed to invalidate a base buffer."},
    [ERROR_GET_CODE(BUFFER_WARNING_FAIL_TO_INVALIDATE_CR_BUFFER)] =
        {"BUFFER_WARNING_FAIL_TO_INVALIDATE_CR_BUFFER", "Failed to invalidate a CR buffer."},
    [ERROR_GET_CODE(BUFFER_WARNING_FAIL_TO_INVALIDATE_MEMCHUNK)] =
        {"BUFFER_WARNING_FAIL_TO_INVALIDATE_MEMCHUNK", "Failed to invalidate a memory chunk."},
    [ERROR_GET_CODE(BUFFER_WARNING_FAIL_TO_RESIZE)] =
        {"BUFFER_WARNING_FAIL_TO_RESIZE", "Failed to resize the bufferpool."},
    [ERROR_GET_CODE(BUFFER_WARNING_FAIL_TO_SHRINK)] =
    {   "BUFFER_WARNING_FAIL_TO_SHRINK", "Failed to shrink the bufferpool."},
    [ERROR_GET_CODE(BUFFER_WARNING_INVALIDATION_WORKER_TIMEOUT)] =
        {"BUFFER_WARNING_INVALIDATION_WORKER_TIMEOUT", "Invalidation worker has hit timeout for bufferpool shrink."},
    [ERROR_GET_CODE(BGPAGEWRITER_WARNING_PAGEWRITER_NOT_READY)] =
        {"BGPAGEWRITER_WARNING_PAGEWRITER_NOT_READY",
         "BgPageWriter is not ready, the current page cannot be pushed into the BgPageWriter's queue."},
    [ERROR_GET_CODE(BGPAGEWRITER_WARNING_DIRTY_PAGE_QUEUE_FULL)] =
        {"BGPAGEWRITER_WARNING_DIRTY_PAGE_QUEUE_FULL",
         "DirtyPageQueue is full, the current page cannot be pushed into the queue."},
    [ERROR_GET_CODE(BGPAGEWRITER_ERROR_PAGE_NOT_IN_QUEUE)] =
        {"BGPAGEWRITER_ERROR_PAGE_NOT_IN_QUEUE",
         "DirtyPageQueue does not contain the current page we are trying to remove from the queue."},
    [ERROR_GET_CODE(BGPAGEWRITER_WARNING_PAGE_ALREADY_IN_QUEUE)] =
        {"BGPAGEWRITER_WARNING_PAGE_ALREADY_IN_QUEUE",
         "DirtyPageQueue already contains the current page we are trying to push into the queue."},
    [ERROR_GET_CODE(BGPAGEWRITER_ERROR_PAGEWRITER_ALREADY_CREATE)] =
        {"BGPAGEWRITER_ERROR_PAGEWRITER_ALREADY_CREATE", "BgPageWriter for wal id %u has created."},
    [ERROR_GET_CODE(BGPAGEWRITER_ERROR_NO_AVAILABLE_SLOT)] =
        {"BGPAGEWRITER_ERROR_NO_AVAILABLE_SLOT", "Don't have available slot to create new BgPageWriter"},
    [ERROR_GET_CODE(BGPAGEWRITER_ERROR_ABANDON_FLUSH)] =
        {"BGPAGEWRITER_ERROR_ABANDON_FLUSH", "Abandon the page flush"},
    [ERROR_GET_CODE(BUFFER_INFO_CONSTRUCT_CR_NOT_DATA_PAGE)] =
        {"BUFFER_INFO_CONSTRUCT_CR_NOT_DATA_PAGE", "Page type is not a data page when constructing cr page."},
    [ERROR_GET_CODE(BUFFER_ERROR_OUT_OF_MEMORY)] =
        {"BUFFER_ERROR_OUT_OF_MEMORY", "OOM"},
    [ERROR_GET_CODE(BUFFER_INFO_TRY_LOCK_PAGE_CONTENT_FAIL)] =
        {"BUFFER_INFO_TRY_LOCK_PAGE_CONTENT_FAIL", "Try acquire page content lock fail."},
    [ERROR_GET_CODE(BUFFER_INFO_PD_OWNER_REDIRECT)] =
        {"BUFFER_INFO_PD_OWNER_REDIRECT",
         "The designated PD entry has been relocated or the requesting node got an outdated info"},
    [ERROR_GET_CODE(BUFFER_INFO_PD_ENTRY_RELOCATING)] =
        {"BUFFER_INFO_PD_ENTRY_RELOCATING", "The designated PD entry is under relocation"},
    [ERROR_GET_CODE(BUFFER_INFO_PD_ENTRY_INUSE)] =
        {"BUFFER_INFO_PD_ENTRY_INUSE", "The designated PD entry is still being used."},
    [ERROR_GET_CODE(BUFFER_INFO_OBSOLETE_PD)] =
        {"BUFFER_INFO_OBSOLETE_PD", "The designated PD entry is obsolete."},
    [ERROR_GET_CODE(BUFFER_ERROR_INSERT_ANTI_CACHE_FAIL)] =
        {"BUFFER_ERROR_INSERT_ANTI_CACHE_FAIL", "Failed to insert anti-entry."},
    [ERROR_GET_CODE(BUFFER_ERROR_RPC_RESPONSE_TYPE_INVALID)] =
        {"BUFFER_ERROR_RPC_RESPONSE_TYPE_INVALID", "The buffer RPC response message type is invalid"},
    [ERROR_GET_CODE(BUFFER_ERROR_PD_BUCKET_RELOCATION_FAILURE)] =
        {"BUFFER_ERROR_PD_BUCKET_RELOCATION_FAILURE", "Bucket is in the wrong state"},
    [ERROR_GET_CODE(BUFFER_ERROR_RPC_LOCAL_NODE_DOWN)] =
        {"BUFFER_ERROR_RPC_LOCAL_NODE_DOWN", "Send RPC request message fail because local node is down."},
    [ERROR_GET_CODE(BUFFER_ERROR_RPC_REMOTE_NODE_DOWN)] =
        {"BUFFER_ERROR_RPC_REMOTE_NODE_DOWN", "Send RPC request message fail because remote node is down."},
    [ERROR_GET_CODE(BUFFER_ERROR_PD_OWNER_NODE_DOWN)] =
        {"BUFFER_ERROR_PD_OWNER_NODE_DOWN", "Page Directory owner node is down."},
    [ERROR_GET_CODE(BUFFER_ERROR_PD_OWNER_UPDATE)] =
        {"BUFFER_ERROR_PD_OWNER_UPDATE", "Page Directory owner node ought to be updated for memory checkpoint."},
    [ERROR_GET_CODE(BUFFER_ERROR_PAGE_OWNER_NODE_DOWN)] =
        {"BUFFER_ERROR_PAGE_OWNER_NODE_DOWN", "Page owner node is down."},
    [ERROR_GET_CODE(BUFFER_ERROR_ACQUIRE_PD_BUCKET_LATCH_FAIL)] =
        {"BUFFER_ERROR_ACQUIRE_PD_BUCKET_LATCH_FAIL", "Try lock PD bucket latch fail because of lock conflict"},
    [ERROR_GET_CODE(BUFFER_ERROR_ACQUIRE_GLOBAL_PAGE_LATCH_FAIL)] =
        {"BUFFER_ERROR_ACQUIRE_GLOBAL_PAGE_LATCH_FAIL", "Try lock global page latch fail because of lock conflict"},
    [ERROR_GET_CODE(BUFFER_ERROR_ACQUIRE_GLOBAL_PAGE_LATCH_YIELD)] =
        {"BUFFER_ERROR_ACQUIRE_GLOBAL_PAGE_LATCH_YIELD", "Lock global page latch yield"},
    [ERROR_GET_CODE(BUFFER_ERROR_MEMBER_VIEW_CHANGE)] =
        {"BUFFER_ERROR_MEMBER_VIEW_CHANGE", "Member view is changed"},
    [ERROR_GET_CODE(BUFFER_ERROR_CREATE_PD_ENTRY_FAIL)] =
        {"BUFFER_ERROR_CREATE_PD_ENTRY_FAIL", "PD entries are not successfully created."},
    [ERROR_GET_CODE(BUFFER_ERROR_PAGE_REPLAY_IN_PROGRESS)] =
        {"BUFFER_ERROR_PAGE_REPLAY_IN_PROGRESS", "The page is WAL replay in progress."},
    [ERROR_GET_CODE(BUFFER_ERROR_PAGE_VERSION_CHECK_FAIL)] =
        {"BUFFER_ERROR_PAGE_VERSION_CHECK_FAIL", "The storage fault check fail."},
    [ERROR_GET_CODE(BUFFER_ERROR_CONSTRUCT_CR_AT_PAGE_OWNER_FAILED)] =
        {"BUFFER_ERROR_CONSTRUCT_CR_AT_PAGE_OWNER_FAILED", "Failed to construct CR page at page owner."},
    [ERROR_GET_CODE(BUFFER_ERROR_INVALIDATE_FAILED)] =
        {"BUFFER_ERROR_INVALIDATE_FAILED", "Failed to invalidate buffer."},
    [ERROR_GET_CODE(BUFFER_INFO_PD_OWNER_RIP)] =
        {"BUFFER_INFO_PD_OWNER_RIP", "The PD bucket status is recovery in progress"},
    [ERROR_GET_CODE(BUFFER_INFO_OWNER_PAGE_REDIRECT)] =
        {"BUFFER_INFO_OWNER_PAGE_REDIRECT", "PO Node don't have owner page"},
    [ERROR_GET_CODE(BUFFER_INFO_RECEIVE_OUTDATED_MESSAGE)] =
        {"BUFFER_INFO_RECEIVE_OUTDATED_MESSAGE", "Receive a message from a crashed node"},
    [ERROR_GET_CODE(TDE_ERROR_ENCRYPT_FAILED)] =
        {"TDE_ERROR_ENCRYPT_FAILED", "Tde encrypt failed"},
    [ERROR_GET_CODE(TDE_ERROR_DECRYPT_FAILED)] =
        {"TDE_ERROR_DECRYPT_FAILED", "Tde decrypt failed"},
    [ERROR_GET_CODE(BUFFER_ERROR_PAGE_VERIFY_FAILED)] =
        {"BUFFER_ERROR_PAGE_VERIFY_FAILED", "Page verification failed on read path, possible page corruption."},
};

static_assert(sizeof(g_buffer_error_code_map) != BUFFER_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif

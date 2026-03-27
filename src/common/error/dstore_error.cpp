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
 * IDENTIFICATION
 *        src/common/error/dstore_error.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/error/dstore_error.h"
#include <cstdarg>
#include "securec.h"
#include "errorcode/dstore_buf_error_code_map.h"
#include "errorcode/dstore_catalog_error_code_map.h"
#include "errorcode/dstore_common_error_code_map.h"
#include "errorcode/dstore_control_error_code_map.h"
#include "errorcode/dstore_framework_error_code_map.h"
#include "errorcode/dstore_heap_error_code_map.h"
#include "errorcode/dstore_index_error_code_map.h"
#include "errorcode/dstore_lock_error_code_map.h"
#include "errorcode/dstore_page_error_code_map.h"
#include "errorcode/dstore_port_error_code_map.h"
#include "errorcode/dstore_wal_error_code_map.h"
#include "errorcode/dstore_tablespace_error_code_map.h"
#include "errorcode/dstore_transaction_error_code_map.h"
#include "errorcode/dstore_tuple_error_code_map.h"
#include "errorcode/dstore_undo_error_code_map.h"
#include "common/log/dstore_log.h"
#include "errorcode/dstore_rpc_error_code_map.h"
#include "errorcode/dstore_systable_error_code_map.h"
#include "errorcode/dstore_recovery_error_code_map.h"
#include "errorcode/dstore_logical_replication_error_code_map.h"
#include "errorcode/dstore_pdb_error_code_map.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_modules.h"

namespace DSTORE {

static ErrorDetails *g_errorCodeMap[] = {
    [MODULE_ALL] = nullptr,
    [MODULE_BUFFER] = g_buffer_error_code_map,
    [MODULE_BUFMGR] = nullptr,
    [MODULE_CATALOG] = g_catalog_error_code_map,
    [MODULE_COMMON] = g_common_error_code_map,
    [MODULE_HEAP] = g_heap_error_code_map,
    [MODULE_CONTROL] = g_control_error_code_map,
    [MODULE_FRAMEWORK] = g_framework_error_code_map,
    [MODULE_INDEX] = g_index_error_code_map,
    [MODULE_LOCK] = g_lock_error_code_map,
    [MODULE_PAGE] = g_page_error_code_map,
    [MODULE_PORT] = g_port_error_code_map,
    [MODULE_WAL] = g_wal_error_code_map,
    [MODULE_BGPAGEWRITER] = nullptr,
    [MODULE_TABLESPACE] = g_tablespace_error_code_map,
    [MODULE_SEGMENT] = nullptr,
    [MODULE_TRANSACTION] = g_transaction_error_code_map,
    [MODULE_TUPLE] = g_tuple_error_code_map,
    [MODULE_UNDO] = g_undo_error_code_map,
    [MODULE_RPC] = g_rpc_error_code_map,
    [MODULE_SYSTABLE] = g_systable_error_code_map,
    [MODULE_MEMNODE] = nullptr,
    [MODULE_PDBREPLICA] = nullptr,
    [MODULE_RECOVERY] = g_recovery_error_code_map,
    [MODULE_LOGICAL_REPLICATION] = g_logical_replication_error_code_map, 
    [MODULE_PDB] = g_pdb_error_code_map,
    [MODULE_MAX] = nullptr
};

static int g_errorNumberOfCodesMap[] = {
    [MODULE_ALL] = 0,
    [MODULE_BUFFER] = sizeof(g_buffer_error_code_map) / sizeof(ErrorDetails),
    [MODULE_BUFMGR] = 0,
    [MODULE_CATALOG] = sizeof(g_catalog_error_code_map) / sizeof(ErrorDetails),
    [MODULE_COMMON] = sizeof(g_common_error_code_map) / sizeof(ErrorDetails),
    [MODULE_HEAP] = sizeof(g_heap_error_code_map) / sizeof(ErrorDetails),
    [MODULE_CONTROL] = sizeof(g_control_error_code_map) / sizeof(ErrorDetails),
    [MODULE_FRAMEWORK] = sizeof(g_framework_error_code_map) / sizeof(ErrorDetails),
    [MODULE_INDEX] = sizeof(g_index_error_code_map) / sizeof(ErrorDetails),
    [MODULE_LOCK] = sizeof(g_lock_error_code_map) / sizeof(ErrorDetails),
    [MODULE_PAGE] = sizeof(g_page_error_code_map) / sizeof(ErrorDetails),
    [MODULE_PORT] = sizeof(g_port_error_code_map) / sizeof(ErrorDetails),
    [MODULE_WAL] = sizeof(g_wal_error_code_map) / sizeof(ErrorDetails),
    [MODULE_BGPAGEWRITER] = 0,
    [MODULE_TABLESPACE] = sizeof(g_tablespace_error_code_map) / sizeof(ErrorDetails),
    [MODULE_SEGMENT] = 0,
    [MODULE_TRANSACTION] = sizeof(g_transaction_error_code_map) / sizeof(ErrorDetails),
    [MODULE_TUPLE] = sizeof(g_tuple_error_code_map) / sizeof(ErrorDetails),
    [MODULE_UNDO] = sizeof(g_undo_error_code_map) / sizeof(ErrorDetails),
    [MODULE_RPC] = sizeof(g_rpc_error_code_map) / sizeof(ErrorDetails),
    [MODULE_SYSTABLE] = sizeof(g_systable_error_code_map) / sizeof(ErrorDetails),
    [MODULE_MEMNODE] = 0,
    [MODULE_PDBREPLICA] = 0,
    [MODULE_RECOVERY] = sizeof(g_recovery_error_code_map) / sizeof(ErrorDetails),
    [MODULE_LOGICAL_REPLICATION] = sizeof(g_logical_replication_error_code_map) / sizeof(ErrorDetails),
    [MODULE_PDB] = sizeof(g_pdb_error_code_map) / sizeof(ErrorDetails),
    [MODULE_MAX] = 0
};

Error::Error()
    : m_error_code_map(g_errorCodeMap), m_error_code_map_size(sizeof(g_errorCodeMap) / sizeof(ErrorDetails *))
{}

Error::Error(ErrorDetails **errorCodeMap, int errorCodeMapSize)
    : m_error_code_map(errorCodeMap), m_error_code_map_size(errorCodeMapSize)
{}

void Error::ClearError()
{
    m_error_code = STORAGE_OK;
    m_line_number = 0;

    errno_t rc = memset_s(m_error_msg, MAX_ERRMSG_LEN + 1, 0, MAX_ERRMSG_LEN + 1);
    storage_securec_check(rc, "\0", "\0");
    rc = memset_s(m_token_string, MAX_TOKENSTRING_LEN + 1, 0, MAX_TOKENSTRING_LEN + 1);
    storage_securec_check(rc, "\0", "\0");
    rc = memset_s(m_file_name, MAX_FILENAME_LEN + 1, 0, MAX_FILENAME_LEN + 1);
    storage_securec_check(rc, "\0", "\0");
    rc = memset_s(m_function_name, MAX_FUNCTIONNAME_LEN + 1, 0, MAX_FUNCTIONNAME_LEN + 1);
    storage_securec_check(rc, "\0", "\0");
}

void Error::SetErrorCommon(const char *fileName, int lineNumber, const char *functionName, ErrorCode errorCode,
    va_list args)
{
    m_error_code = errorCode;
    m_line_number = lineNumber;

    errno_t rc = strncpy_s(m_file_name, MAX_FILENAME_LEN + 1, fileName, MAX_FILENAME_LEN);
    storage_securec_check(rc, "\0", "\0");
    rc = strncpy_s(m_function_name, MAX_FUNCTIONNAME_LEN + 1, functionName, MAX_FUNCTIONNAME_LEN);
    storage_securec_check(rc, "\0", "\0");

    /* Make sure that the given error code does not result in an out-of-bound index for m_error_code_map */
    int errorModule = static_cast<int>(ERROR_GET_MODULE(static_cast<unsigned long long>(m_error_code)));
    if (errorModule >= m_error_code_map_size) {
        /* Out-of-bound: It was an undefined error */
        m_error_code = COMMON_ERROR_UNDEFINED_ERROR;
        errorModule = ERROR_GET_MODULE(static_cast<unsigned long long>(m_error_code));
    }

    const char *errorMsg =
        m_error_code_map[errorModule][ERROR_GET_CODE(static_cast<unsigned long long>(m_error_code))].message;
    rc = vsnprintf_s(m_error_msg, MAX_ERRMSG_LEN + 1, MAX_ERRMSG_LEN, errorMsg, args);
    /* Check the buffer exception, but not the truncation exception. */
    if (unlikely(rc == -1 && errno == EINVAL)) {
        StorageAssert(0);
    }

#ifdef UT
    ErrLevel level;
    switch (ERROR_GET_SEVERITY(m_error_code)) {
        case ERROR_SEVERITY_INFO:
            level = DSTORE_INFO;
            break;
        case ERROR_SEVERITY_WARNING:
            level = DSTORE_WARNING;
            break;
        case ERROR_SEVERITY_ERROR:
            level = DSTORE_ERROR;
            break;
        case ERROR_SEVERITY_SEVERE:
        case ERROR_SEVERITY_FATAL:
            level = DSTORE_PANIC;
            break;
        default:
            break;
    }
    ErrLog(level, ModuleId(errorModule), ErrMsg("%s", m_error_msg));
#endif
}

void Error::SetError(const char *fileName, int lineNumber, const char *functionName, ErrorCode errorCode, ...)
{
    va_list args;
    va_start(args, errorCode);
    SetErrorCommon(fileName, lineNumber, functionName, errorCode, args);
    va_end(args);
    m_errorNodeId = g_storageInstance->GetGuc()->selfNodeId;
}

void Error::CopyError(Error *error)
{
    m_error_code = error->m_error_code;
    m_line_number = error->m_line_number;

    errno_t rc = strncpy_s(m_file_name, MAX_FILENAME_LEN + 1, error->m_file_name, MAX_FILENAME_LEN);
    storage_securec_check(rc, "\0", "\0");
    rc = strncpy_s(m_function_name, MAX_FUNCTIONNAME_LEN + 1, error->m_function_name, MAX_FUNCTIONNAME_LEN);
    storage_securec_check(rc, "\0", "\0");

    /* Make sure that the given error code does not result in an out-of-bound index for m_error_code_map */
    int errorModule = static_cast<int>(ERROR_GET_MODULE(static_cast<unsigned long long>(m_error_code)));
    if (errorModule >= m_error_code_map_size) {
        /* Out-of-bound: It was an undefined error */
        m_error_code = COMMON_ERROR_UNDEFINED_ERROR;
        errorModule = ERROR_GET_MODULE(static_cast<unsigned long long>(m_error_code));
    }
    rc = snprintf_s(m_error_msg, MAX_ERRMSG_LEN + 1, MAX_ERRMSG_LEN, error->m_error_msg);
    /* Check the buffer exception, but not the truncation exception. */
    if (unlikely(rc == -1 && errno == EINVAL)) {
        StorageAssert(0);
    }
}

/*
* SetErrorCodeOnly() is used in the performance sensitive codepaths for
* errors like LOCK_INFO_NOT_AVAIL where calling formatting functions such
* as vsnprintf may affect performance.
*/
void Error::SetErrorCodeOnly(ErrorCode errorCode)
{
    m_error_code = errorCode;
}

void Error::SetErrorWithNodeId(const char *fileName, int lineNumber, const char *functionName, NodeId nodeId,
    ErrorCode errorCode, ...)
{
    va_list args;
    va_start(args, errorCode);
    SetErrorCommon(fileName, lineNumber, functionName, errorCode, args);
    va_end(args);
    m_errorNodeId = nodeId;
}

ErrorCode Error::GetErrorCode() const
{
    return m_error_code;
}

const char *Error::GetMessage() const
{
    return m_error_msg;
}

void Error::GetLocation(const char *&fileName, int &lineNumber) const
{
    fileName = m_file_name;
    lineNumber = m_line_number;
}

void Error::GetFunctionName(const char *&functionName) const
{
    functionName = m_function_name;
}

NodeId Error::GetErrorNodeId() const
{
    return m_errorNodeId;
}

const char *Error::GetErrorName() const
{
    int errorModule = static_cast<int>(ERROR_GET_MODULE(m_error_code));
    int errorCode = static_cast<int>(ERROR_GET_CODE(m_error_code));
    return m_error_code_map[errorModule][errorCode].name;
}

char *Error::GetErrorInfo() const
{
    StringInfoData string;
    if (!string.init()) {
        return nullptr;
    }

    int errorModule = static_cast<int>(ERROR_GET_MODULE(m_error_code));
    int errorCode = static_cast<int>(ERROR_GET_CODE(m_error_code));
    const char *moduleName = GetValidModuleNameForTool(static_cast<ModuleId>(errorModule));
    int numOfCodesMapSize = sizeof(g_errorNumberOfCodesMap) / sizeof(int);
    static_assert(sizeof(g_errorNumberOfCodesMap) / sizeof(int) == sizeof(g_errorCodeMap) / sizeof(ErrorDetails *),
        "g_errorCodeMap should be the same size as g_errorNumberOfCodesMap.");
    if (errorModule < 0 || errorModule > (numOfCodesMapSize - 1) || g_errorNumberOfCodesMap[errorModule] == 0 ||
            errorCode < 0 || errorCode > (g_errorNumberOfCodesMap[errorModule] - 1)) {
        DstorePfree(string.data);
        return nullptr;
    }
    ErrorDetails errorDetails = m_error_code_map[errorModule][errorCode];
    string.append("Module: %s\n", moduleName);
    string.append("Name: %s\n", errorDetails.name);
    string.append("Message: %s\n", errorDetails.message);

    return string.data;
}

} /* namespace DSTORE */

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

#include "sysbench_server.h"
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <string>
#include "framework/dstore_instance_interface.h"
#include "framework/dstore_thread.h"
#include "framework/dstore_thread_interface.h"
#include "framework/dstore_vfs_interface.h"
#include "framework/dstore_config_interface.h"
#include "framework/dstore_session_interface.h"
#include "common/log/dstore_log.h"
#include "common/memory/dstore_mctx.h"
#include "framework/dstore_instance.h"
#include "config/dstore_vfs_config.h"
#include "pdb/dstore_pdb_interface.h"
#include "cjson/cJSON.h"
#include "table_handler.h"

using namespace DSTORE;

/* ----------------------------------------------------------------
 * Module-level globals
 * ---------------------------------------------------------------- */
static StorageGUC   g_sysbenchGuc;
static bool         g_isGucLoaded = false;

static const std::string SYSBENCH_GUC_PATH("guc.json");

/* ----------------------------------------------------------------
 * RemoveDirRecursive
 * ---------------------------------------------------------------- */
static void RemoveDirRecursive(const char *path)
{
    DIR *dir = opendir(path);
    if (dir == nullptr) { return; }

    struct dirent *entry;
    char entryPath[VFS_FILE_PATH_MAX_LEN];

    while ((entry = readdir(dir)) != nullptr) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        __attribute__((__unused__)) int rc =
            snprintf(entryPath, sizeof(entryPath), "%s/%s", path, entry->d_name);

        struct stat st;
        if (lstat(entryPath, &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
                RemoveDirRecursive(entryPath);
                rmdir(entryPath);
            } else {
                remove(entryPath);
            }
        }
    }
    closedir(dir);
    rmdir(path);
}

void LoadGucConfig()
{
    std::ifstream configFile(SYSBENCH_GUC_PATH);
    if (!configFile.is_open()) {
        std::cout << "Could not open config file: " << SYSBENCH_GUC_PATH << std::endl;
        exit(1);
    }
    g_isGucLoaded = true;
    std::string configMsg((std::istreambuf_iterator<char>(configFile)), std::istreambuf_iterator<char>());
    std::cout << "--------------------"
              << "GUC Config in " << SYSBENCH_GUC_PATH << "--------------------" << std::endl;

    
    cJSON *configJson = cJSON_Parse(configMsg.c_str());
    g_sysbenchGuc.selfNodeId = cJSON_GetObjectItem(configJson, "selfNodeId")->valueint;
    std::cout << "selfNodeId: " << g_sysbenchGuc.selfNodeId << std::endl;

    g_sysbenchGuc.buffer = cJSON_GetObjectItem(configJson, "buffer")->valueint;
    std::cout << "buffer: " << g_sysbenchGuc.buffer << std::endl;

    g_sysbenchGuc.bufferLruPartition = cJSON_GetObjectItem(configJson, "bufferLruPartition")->valueint;
    std::cout << "bufferLruPartition: " << g_sysbenchGuc.bufferLruPartition << std::endl;

    g_sysbenchGuc.checkpointTimeout = cJSON_GetObjectItem(configJson, "checkpointTimeout")->valueint;
    std::cout << "checkpointTimeout: " << g_sysbenchGuc.checkpointTimeout << std::endl;

    g_sysbenchGuc.defaultIsolationLevel = cJSON_GetObjectItem(configJson, "defaultIsolationLevel")->valueint;
    std::cout << "defaultIsolationLevel: " << g_sysbenchGuc.defaultIsolationLevel << std::endl;

    g_sysbenchGuc.maintenanceWorkMem = cJSON_GetObjectItem(configJson, "maintenanceWorkMem")->valueint;
    std::cout << "maintenanceWorkMem: " << g_sysbenchGuc.maintenanceWorkMem << std::endl;

    char *tmpStrValue = cJSON_GetObjectItem(configJson, "dataDir")->valuestring;
    g_sysbenchGuc.dataDir = (strlen(tmpStrValue) == 0 ? nullptr : tmpStrValue);
    std::cout << "dataDir: " << (g_sysbenchGuc.dataDir == nullptr ? "nullptr" : g_sysbenchGuc.dataDir) << std::endl;

    g_sysbenchGuc.ncores = cJSON_GetObjectItem(configJson, "ncores")->valueint;
    std::cout << "ncores: " << g_sysbenchGuc.ncores << std::endl;

    g_sysbenchGuc.logMinMessages = cJSON_GetObjectItem(configJson, "logMinMessages")->valueint;
    std::cout << "logMinMessages: " << g_sysbenchGuc.logMinMessages << std::endl;

    g_sysbenchGuc.foldPeriod = cJSON_GetObjectItem(configJson, "foldPeriod")->valueint;
    std::cout << "foldPeriod: " << g_sysbenchGuc.foldPeriod << std::endl;

    g_sysbenchGuc.foldThreshold = cJSON_GetObjectItem(configJson, "foldThreshold")->valueint;
    std::cout << "foldThreshold: " << g_sysbenchGuc.foldThreshold << std::endl;

    g_sysbenchGuc.foldLevel = cJSON_GetObjectItem(configJson, "foldLevel")->valueint;
    std::cout << "foldLevel: " << g_sysbenchGuc.foldLevel << std::endl;

    g_sysbenchGuc.csnAssignmentIncrement = cJSON_GetObjectItem(configJson, "csnAssignmentIncrement")->valueint;
    std::cout << "csnAssignmentIncrement: " << g_sysbenchGuc.csnAssignmentIncrement << std::endl;

    tmpStrValue = cJSON_GetObjectItem(configJson, "moduleLoggingConfigure")->valuestring;
    g_sysbenchGuc.moduleLoggingConfigure = (strlen(tmpStrValue) == 0 ? nullptr : tmpStrValue);
    std::cout << "moduleLoggingConfigure: "
              << (g_sysbenchGuc.moduleLoggingConfigure == nullptr ? "nullptr" : g_sysbenchGuc.moduleLoggingConfigure) << std::endl;

    g_sysbenchGuc.lockHashTableSize = cJSON_GetObjectItem(configJson, "lockHashTableSize")->valueint;
    std::cout << "lockHashTableSize: " << g_sysbenchGuc.lockHashTableSize << std::endl;

    g_sysbenchGuc.lockTablePartitionNum = cJSON_GetObjectItem(configJson, "lockTablePartitionNum")->valueint;
    std::cout << "lockTablePartitionNum: " << g_sysbenchGuc.lockTablePartitionNum << std::endl;

    g_sysbenchGuc.enableLazyLock = cJSON_GetObjectItem(configJson, "enableLazyLock")->valueint;
    std::cout << "enableLazyLock: " << ((g_sysbenchGuc.enableLazyLock == 1) ? "true" : "false") << std::endl;

    tmpStrValue = cJSON_GetObjectItem(configJson, "vfsTenantIsolationConfigPath")->valuestring;
    g_sysbenchGuc.vfsTenantIsolationConfigPath = (strlen(tmpStrValue) == 0 ? nullptr : tmpStrValue);
    std::cout << "vfsTenantIsolationConfigPath: "
              << (g_sysbenchGuc.vfsTenantIsolationConfigPath == nullptr ? "nullptr" : g_sysbenchGuc.vfsTenantIsolationConfigPath)
              << std::endl;

    g_sysbenchGuc.updateCsnMinInterval = cJSON_GetObjectItem(configJson, "updateCsnMinInterval")->valueint;
    std::cout << "updateCsnMinInterval: " << g_sysbenchGuc.updateCsnMinInterval << std::endl;

    g_sysbenchGuc.numObjSpaceMgrWorkers = cJSON_GetObjectItem(configJson, "numObjSpaceMgrWorkers")->valueint;
    std::cout << "numObjSpaceMgrWorkers: " << g_sysbenchGuc.numObjSpaceMgrWorkers << std::endl;

    g_sysbenchGuc.minFreePagePercentageThreshold1 =
        cJSON_GetObjectItem(configJson, "minFreePagePercentageThreshold1")->valueint;
    std::cout << "minFreePagePercentageThreshold1: " << g_sysbenchGuc.minFreePagePercentageThreshold1 << std::endl;

    g_sysbenchGuc.minFreePagePercentageThreshold2 =
        cJSON_GetObjectItem(configJson, "minFreePagePercentageThreshold2")->valueint;
    std::cout << "minFreePagePercentageThreshold2: " << g_sysbenchGuc.minFreePagePercentageThreshold2 << std::endl;

    g_sysbenchGuc.probOfExtensionThreshold = cJSON_GetObjectItem(configJson, "probOfExtensionThreshold")->valueint;
    std::cout << "probOfExtensionThreshold: " << g_sysbenchGuc.probOfExtensionThreshold << std::endl;

    g_sysbenchGuc.recoveryWorkerNum = cJSON_GetObjectItem(configJson, "recoveryWorkerNum")->valueint;
    std::cout << "recoveryWorkerNum: " << g_sysbenchGuc.recoveryWorkerNum << std::endl;

    g_sysbenchGuc.synchronousCommit = cJSON_GetObjectItem(configJson, "synchronousCommit")->valueint;
    std::cout << "synchronousCommit: " << ((g_sysbenchGuc.synchronousCommit == 1) ? "true" : "false") << std::endl;

    g_sysbenchGuc.walStreamCount = cJSON_GetObjectItem(configJson, "walStreamCount")->valueint;
    std::cout << "walStreamCount: " << g_sysbenchGuc.walStreamCount << std::endl;

    g_sysbenchGuc.walFileNumber = cJSON_GetObjectItem(configJson, "walFileNumber")->valueint;
    std::cout << "walFileNumber: " << g_sysbenchGuc.walFileNumber << std::endl;

    tmpStrValue = cJSON_GetObjectItem(configJson, "walFileSize")->valuestring;
    g_sysbenchGuc.walFileSize = std::stoll(tmpStrValue);
    std::cout << "walFileSize: " << g_sysbenchGuc.walFileSize << std::endl;

    g_sysbenchGuc.walBuffers = cJSON_GetObjectItem(configJson, "walBuffers")->valueint;
    std::cout << "walBuffers: " << g_sysbenchGuc.walBuffers << std::endl;

    tmpStrValue = cJSON_GetObjectItem(configJson, "walReadBufferSize")->valuestring;
    g_sysbenchGuc.walReadBufferSize = std::stoll(tmpStrValue);
    std::cout << "walReadBufferSize: " << g_sysbenchGuc.walReadBufferSize << std::endl;

    tmpStrValue = cJSON_GetObjectItem(configJson, "walRedoBufferSize")->valuestring;
    g_sysbenchGuc.walRedoBufferSize = std::stoll(tmpStrValue);
    std::cout << "walRedoBufferSize: " << g_sysbenchGuc.walRedoBufferSize << std::endl;

    g_sysbenchGuc.walwriterCpuBind = cJSON_GetObjectItem(configJson, "walwriterCpuBind")->valueint;
    std::cout << "walwriterCpuBind: " << g_sysbenchGuc.walwriterCpuBind << std::endl;

    g_sysbenchGuc.redoBindCpuAttr = cJSON_GetObjectItem(configJson, "redoBindCpuAttr")->valuestring;
    std::cout << "redoBindCpuAttr: " << g_sysbenchGuc.redoBindCpuAttr << std::endl;

    g_sysbenchGuc.numaNodeNum = cJSON_GetObjectItem(configJson, "numaNodeNum")->valueint;
    std::cout << "numaNodeNum: " << static_cast<int>(g_sysbenchGuc.numaNodeNum) << std::endl;

    g_sysbenchGuc.disableBtreePageRecycle = cJSON_GetObjectItem(configJson, "disableBtreePageRecycle")->valueint;
    std::cout << "disableBtreePageRecycle: " << ((g_sysbenchGuc.disableBtreePageRecycle == 1) ? "true" : "false") << std::endl;

    g_sysbenchGuc.deadlockTimeInterval = cJSON_GetObjectItem(configJson, "deadlockTimeInterval")->valueint;
    std::cout << "deadlockTimeInterval: " << g_sysbenchGuc.deadlockTimeInterval << std::endl;

    g_sysbenchGuc.recycleFsmTimeInterval = cJSON_GetObjectItem(configJson, "recycleFsmTimeInterval")->valueint;
    std::cout << "recycleFsmTimeInterval: " << g_sysbenchGuc.recycleFsmTimeInterval << std::endl;

    g_sysbenchGuc.probOfUpdateFsmTimestamp = cJSON_GetObjectItem(configJson, "probOfUpdateFsmTimestamp")->valueint;
    std::cout << "probOfUpdateFsmTimestamp: " << g_sysbenchGuc.probOfUpdateFsmTimestamp << std::endl;

    g_sysbenchGuc.probOfRecycleFsm = cJSON_GetObjectItem(configJson, "probOfRecycleFsm")->valueint;
    std::cout << ".probOfRecycleFsm: " << g_sysbenchGuc.probOfRecycleFsm << std::endl;

    g_sysbenchGuc.probOfRecycleBtree = cJSON_GetObjectItem(configJson, "probOfRecycleBtree")->valueint;
    std::cout << ".probOfRecycleBtree: " << g_sysbenchGuc.probOfRecycleBtree << std::endl;

    g_sysbenchGuc.distLockMaxRingSize = cJSON_GetObjectItem(configJson, "distLockMaxRingSize")->valueint;
    std::cout << "distLockMaxRingSize: " << g_sysbenchGuc.distLockMaxRingSize << std::endl;

    g_sysbenchGuc.csnMode = static_cast<DSTORE::CsnMode>(cJSON_GetObjectItem(configJson, "csnMode")->valueint);
    std::cout << "csnMode: " << static_cast<int>(g_sysbenchGuc.csnMode) << std::endl;

    g_sysbenchGuc.ctrlPlanePort = cJSON_GetObjectItem(configJson, "ctrlPlanePort")->valueint;
    std::cout << "ctrlPlanePort: " << g_sysbenchGuc.ctrlPlanePort << std::endl;

    g_sysbenchGuc.rdmaGidIndex = cJSON_GetObjectItem(configJson, "rdmaGidIndex")->valueint;
    std::cout << "rdmaGidIndex: " << static_cast<int>(g_sysbenchGuc.rdmaGidIndex) << std::endl;

    g_sysbenchGuc.rdmaIbPort = cJSON_GetObjectItem(configJson, "rdmaIbPort")->valueint;
    std::cout << "rdmaIbPort: " << static_cast<int>(g_sysbenchGuc.rdmaIbPort) << std::endl;

    g_sysbenchGuc.pdReadAuthResetPeriod = cJSON_GetObjectItem(configJson, "pdReadAuthResetPeriod")->valueint;
    std::cout << "pdReadAuthResetPeriod: " << g_sysbenchGuc.pdReadAuthResetPeriod << std::endl;

    g_sysbenchGuc.csnThreadBindCpu = cJSON_GetObjectItem(configJson, "csnThreadBindCpu")->valueint;
    std::cout << "csnThreadBindCpu: " << g_sysbenchGuc.csnThreadBindCpu << std::endl;

    tmpStrValue = cJSON_GetObjectItem(configJson, "commConfigStr")->valuestring;
    g_sysbenchGuc.commConfigStr = (strlen(tmpStrValue) == 0 ? nullptr : tmpStrValue);
    std::cout << "commConfigStr: " << (g_sysbenchGuc.commConfigStr == nullptr ? "nullptr" : g_sysbenchGuc.commConfigStr) << std::endl;

    g_sysbenchGuc.commThreadMin = cJSON_GetObjectItem(configJson, "commThreadMin")->valueint;
    std::cout << "commThreadMin: " << g_sysbenchGuc.commThreadMin << std::endl;
    g_sysbenchGuc.commThreadMax = cJSON_GetObjectItem(configJson, "commThreadMax")->valueint;
    std::cout << "commThreadMax: " << g_sysbenchGuc.commThreadMax << std::endl;
    g_sysbenchGuc.clusterId = cJSON_GetObjectItem(configJson, "clusterId")->valueint;
    std::cout << "clusterId: " << g_sysbenchGuc.clusterId << std::endl;

    tmpStrValue = cJSON_GetObjectItem(configJson, "memberView")->valuestring;
    g_sysbenchGuc.memberView = (strlen(tmpStrValue) == 0 ? nullptr : tmpStrValue);
    std::cout << "memberView: " << (g_sysbenchGuc.memberView == nullptr ? "nullptr" : g_sysbenchGuc.memberView) << std::endl;

    tmpStrValue = cJSON_GetObjectItem(configJson, "commProtocolTypeStr")->valuestring;
    g_sysbenchGuc.commProtocolTypeStr = (strlen(tmpStrValue) == 0 ? nullptr : tmpStrValue);
    std::cout << "commProtocolTypeStr: "
              << (g_sysbenchGuc.commProtocolTypeStr == nullptr ? "nullptr" : g_sysbenchGuc.commProtocolTypeStr) << std::endl;

    g_sysbenchGuc.commProtocolType = cJSON_GetObjectItem(configJson, "commProtocolType")->valueint;
    std::cout << "commProtocolType: " << g_sysbenchGuc.commProtocolType << std::endl;

    g_sysbenchGuc.globalClockAdjustWaitTimeUs = cJSON_GetObjectItem(configJson, "globalClockAdjustWaitTimeUs")->valueint;
    std::cout << "globalClockAdjustWaitTimeUs: " << g_sysbenchGuc.globalClockAdjustWaitTimeUs << std::endl;

    g_sysbenchGuc.globalClockSyncIntervalMs = cJSON_GetObjectItem(configJson, "globalClockSyncIntervalMs")->valueint;
    std::cout << "globalClockSyncIntervalMs: " << g_sysbenchGuc.globalClockSyncIntervalMs << std::endl;

    g_sysbenchGuc.gclockOverlapWaitTimeOptimization =
        cJSON_GetObjectItem(configJson, "gclockOverlapWaitTimeOptimization")->valueint;
    std::cout << "gclockOverlapWaitTimeOptimization: "
              << ((g_sysbenchGuc.gclockOverlapWaitTimeOptimization == 1) ? "true" : "false") << std::endl;

    g_sysbenchGuc.enableQuickStartUp = cJSON_GetObjectItem(configJson, "enableQuickStartUp")->valueint;
    std::cout << "enableQuickStartUp: " << ((g_sysbenchGuc.enableQuickStartUp == 1) ? "true" : "false") << std::endl;

    g_sysbenchGuc.defaultHeartbeatTimeoutInterval =
        cJSON_GetObjectItem(configJson, "defaultHeartbeatTimeoutInterval")->valueint;
    std::cout << "defaultHeartbeatTimeoutInterval: " << g_sysbenchGuc.defaultHeartbeatTimeoutInterval << std::endl;

    g_sysbenchGuc.defaultWalSizeThreshold = cJSON_GetObjectItem(configJson, "defaultWalSizeThreshold")->valueint;
    std::cout << "defaultWalSizeThreshold: " << g_sysbenchGuc.defaultWalSizeThreshold << std::endl;

    g_sysbenchGuc.bgDiskWriterSlaveNum = cJSON_GetObjectItem(configJson, "bgDiskWriterSlaveNum")->valueint;
    std::cout << "bgDiskWriterSlaveNum: " << static_cast<int>(g_sysbenchGuc.bgDiskWriterSlaveNum) << std::endl;

    g_sysbenchGuc.bgPageWriterSleepMilliSecond = cJSON_GetObjectItem(configJson, "bgPageWriterSleepMilliSecond")->valueint;
    std::cout << "bgPageWriterSleepMilliSecond: " << static_cast<int>(g_sysbenchGuc.bgPageWriterSleepMilliSecond) << std::endl;

    g_sysbenchGuc.walThrottlingSize = cJSON_GetObjectItem(configJson, "walThrottlingSize")->valueint;
    std::cout << "walThrottlingSize: " << static_cast<int>(g_sysbenchGuc.walThrottlingSize) << std::endl;

    g_sysbenchGuc.maxIoCapacityKb = cJSON_GetObjectItem(configJson, "maxIoCapacityKb")->valueint;
    std::cout << "maxIoCapacityKb: " << static_cast<int>(g_sysbenchGuc.maxIoCapacityKb) << std::endl;

    tmpStrValue = cJSON_GetObjectItem(configJson, "bgWalWriterMinBytes")->valuestring;
    g_sysbenchGuc.bgWalWriterMinBytes = std::stoll(tmpStrValue);
    std::cout << "bgWalWriterMinBytes: " << g_sysbenchGuc.bgWalWriterMinBytes << std::endl;

    tmpStrValue = cJSON_GetObjectItem(configJson, "walEachWriteLenghthLimit")->valuestring;
    g_sysbenchGuc.walEachWriteLenghthLimit = std::stoll(tmpStrValue);
    std::cout << "walEachWriteLenghthLimit: " << g_sysbenchGuc.walEachWriteLenghthLimit << std::endl;

    g_sysbenchGuc.tenantConfig = new TenantConfig;
    assert(g_sysbenchGuc.tenantConfig != nullptr);
    assert(memset_s(g_sysbenchGuc.tenantConfig, sizeof(TenantConfig), 0, sizeof(TenantConfig)) == EOK);
    tmpStrValue = cJSON_GetObjectItem(configJson, "startConfigPath")->valuestring;
    if (strlen(tmpStrValue) > 0) {
        RetStatus ret = TenantConfigInterface::GetTenantConfig(tmpStrValue, g_sysbenchGuc.tenantConfig);
        StorageReleasePanic(STORAGE_FUNC_FAIL(ret), DSTORE::MODULE_FRAMEWORK, ErrMsg("GetTenantConfig fail."));
    }
    configFile.close();
    std::cout << "tmpStrValue = " << tmpStrValue << std::endl;
    std::cout << "config.storageConfig.clientLibPath = " << g_sysbenchGuc.tenantConfig->storageConfig.clientLibPath << std::endl;
    std::cout << std::endl;
}

/* ================================================================
 * SysbenchStorageInstance::Init
 * ================================================================ */
void DSTORE::SysbenchStorageInstance::Init(const char *dataDirBase)
{
    SetDefaultPdbId(PDB_TEMPLATE1_ID);
    LoadGucConfig();

    char dataDir[VFS_FILE_PATH_MAX_LEN];
    __attribute__((__unused__)) int rc =
        sprintf_s(dataDir, VFS_FILE_PATH_MAX_LEN, "%s/sysbenchdir", dataDirBase);
    storage_securec_check_ss(rc);

    char dstoreDir[VFS_FILE_PATH_MAX_LEN];
    rc = sprintf_s(dstoreDir, VFS_FILE_PATH_MAX_LEN, "%s/%s/", dataDir, DSTORE::BASE_DIR);
    storage_securec_check_ss(rc);

    char pdbMetaPath[VFS_FILE_PATH_MAX_LEN];
    rc = sprintf_s(pdbMetaPath, VFS_FILE_PATH_MAX_LEN, "%s/metadata/", dataDir);
    storage_securec_check_ss(rc);

    char walPath[VFS_FILE_PATH_MAX_LEN];
    rc = sprintf_s(walPath, VFS_FILE_PATH_MAX_LEN, "%s/dstore_wal/", dataDir);
    storage_securec_check_ss(rc);

    RemoveDirRecursive(dataDir);
    if (mkdir(dataDir, 0777) != 0) {
        std::cout << "mkdir " << dataDir << " failed: " << strerror(errno) << std::endl;
        exit(1);
    }
    if (chdir(dataDir) != 0) {
        std::cout << "chdir " << dataDir << " failed: " << strerror(errno) << std::endl;
        exit(1);
    }
    if (mkdir(dstoreDir, 0777) != 0) {
        std::cout << "mkdir " << dstoreDir << " failed: " << strerror(errno) << std::endl;
        exit(1);
    }

    bool flag = VfsInterface::ModuleInitialize();
    StorageReleasePanic(!flag, DSTORE::MODULE_FRAMEWORK, ErrMsg("ModuleInitialize fail."));
    VfsInterface::SetupTenantIsoland(g_sysbenchGuc.tenantConfig, dstoreDir);
    RetStatus retStatus = VfsInterface::CreateTenantDefaultVfs(g_sysbenchGuc.tenantConfig);
    StorageReleasePanic(STORAGE_FUNC_FAIL(retStatus), DSTORE::MODULE_FRAMEWORK,
                        ErrMsg("CreateTenantDefaultVfs fail."));

    if (mkdir(pdbMetaPath, 0777) != 0) {
        std::cout << "mkdir " << pdbMetaPath << " failed: " << strerror(errno) << std::endl;
        exit(1);
    }
    if (mkdir(walPath, 0777) != 0) {
        std::cout << "mkdir " << walPath << " failed: " << strerror(errno) << std::endl;
        exit(1);
    }

    StorageGUC guc = g_sysbenchGuc;
    if (guc.dataDir == nullptr) {
        guc.dataDir = dataDir;
    }
    guc.recoveryWorkerNum = 1;

    std::string logFile = std::string(dataDir) + "/sysbench.log";
    InitLogAdapterInstance(guc.logMinMessages, logFile.c_str(),
                           guc.foldPeriod, guc.foldThreshold, guc.foldLevel);

    g_instance = StorageInstanceInterface::Create(DSTORE::StorageInstanceType::SINGLE);
    g_instance->InitWorkingVersionNum(&SYSBENCH_GRAND_VERSION_NUM);

    SetDefaultPdbId(PDB_TEMPLATE1_ID);
    ThreadContextInterface *thrd = ThreadContextInterface::Create();
    StorageReleasePanic(STORAGE_VAR_NULL(thrd), DSTORE::MODULE_FRAMEWORK,
                        ErrMsg("Failed to create thread context for sysbench init."));
    (void)thrd->InitializeBasic();
    StorageSession *sc = CreateStorageSession(1ULL);
    thrd->AttachSessionToThread(sc);
    (void)g_instance->Bootstrap(&guc);
    (void)thrd->InitStorageContext(g_defaultPdbId);
    g_instance->AddVisibleThread(thrd, g_defaultPdbId);
    CreateTemplateTablespace(DSTORE::g_defaultPdbId);
    CreateUndoMapSegment(DSTORE::g_defaultPdbId);
    (void)thrd->InitTransactionRuntime(g_defaultPdbId, nullptr, nullptr);
    DSTORE::InitVfsClientHandles();
}

/* ----------------------------------------------------------------
 * SysbenchStorageInstance::InitFinished
 * ---------------------------------------------------------------- */
void DSTORE::SysbenchStorageInstance::InitFinished()
{
    StoragePdbInterface::FlushAllDirtyPages(g_defaultPdbId);
    StorageSession *sc = thrd->GetSession();
    g_instance->UnregisterThread();
    g_instance->BootstrapDestroy();
    g_instance->BootstrapResDestroy();
    StorageInstanceInterface::DestoryInstance();
    g_instance = nullptr;
    CleanUpSession(sc);
    StopLogAdapterInstance();
}

/* ----------------------------------------------------------------
 * SysbenchStorageInstance::Start
 * ---------------------------------------------------------------- */
void DSTORE::SysbenchStorageInstance::Start(Oid allocMaxRelOid, const char *dataDir)
{
    SetDefaultPdbId(PDB_TEMPLATE1_ID);
    if (!g_isGucLoaded) {
        LoadGucConfig();
    }
    StorageGUC guc = g_sysbenchGuc;
    if (guc.dataDir == nullptr) {
        guc.dataDir = const_cast<char *>(dataDir);
    }
    chdir(guc.dataDir);
    guc.selfNodeId = 1;

    std::string logFile = std::string(dataDir) + "/node_1.log";
    InitLogAdapterInstance(guc.logMinMessages, logFile.c_str(),
                           guc.foldPeriod, guc.foldThreshold, guc.foldLevel);

    g_instance = StorageInstanceInterface::Create(DSTORE::StorageInstanceType::SINGLE);
    g_instance->InitWorkingVersionNum(&SYSBENCH_GRAND_VERSION_NUM);

    ThreadContextInterface *thrd = ThreadContextInterface::Create();
    StorageReleasePanic(STORAGE_VAR_NULL(thrd), DSTORE::MODULE_FRAMEWORK,
                        ErrMsg("Failed to create thread context for sysbench start."));
    (void)thrd->InitializeBasic();
    StorageSession *sc = CreateStorageSession(1ULL);
    thrd->AttachSessionToThread(sc);
    (void)g_instance->StartupInstance(&guc);
    (void)thrd->InitStorageContext(g_defaultPdbId);
    (void)thrd->InitTransactionRuntime(g_defaultPdbId, nullptr, nullptr);

    simulator = new StorageTableContext(g_instance, allocMaxRelOid);
}

/* ----------------------------------------------------------------
 * SysbenchStorageInstance::Stop
 * ---------------------------------------------------------------- */
void DSTORE::SysbenchStorageInstance::Stop()
{
    if (g_instance != nullptr) {
        g_instance->Destroy();
        g_instance = nullptr;
    }
    if (simulator != nullptr) {
        simulator->Destory();
        delete simulator;
        simulator = nullptr;
    }
}

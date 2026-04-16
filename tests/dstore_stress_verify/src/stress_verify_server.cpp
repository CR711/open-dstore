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

#include "stress_verify_server.h"
#include "stress_verify_common.h"

#include <dirent.h>
#include "dfx/dstore_page_verify.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <fstream>

#include "framework/dstore_instance_interface.h"
#include "framework/dstore_thread_interface.h"
#include "framework/dstore_vfs_interface.h"
#include "framework/dstore_config_interface.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_session_interface.h"
#include "common/log/dstore_log.h"
#include "common/memory/dstore_mctx.h"
#include "config/dstore_vfs_config.h"
#include "pdb/dstore_pdb_interface.h"
#include "cjson/cJSON.h"
#include "securec.h"
#include "table_handler.h"

using namespace DSTORE;

static const std::string GUC_CONFIG_PATH("guc.json");
static std::string g_stressBaseDir;  /* original cwd before chdir into dataDir */
static StorageGUC g_stressGuc;
static bool g_stressIsReadGuc = false;

static int StressRemoveDir(const char *dirPath)
{
    char *curDir = (char *)".";
    char *upperDir = (char *)"..";
    DIR *dirHandle = nullptr;
    dirent *dirContext = nullptr;
    struct stat dirStat;
    char subDirPath[VFS_FILE_PATH_MAX_LEN];
    if (access(dirPath, F_OK) != 0) {
        return 0;
    }

    if (stat(dirPath, &dirStat) < 0) {
        assert(0);
    }
    if (S_ISDIR(dirStat.st_mode)) {
        dirHandle = opendir(dirPath);
        if (dirHandle == nullptr) {
            return -1;
        }
        while ((dirContext = readdir(dirHandle)) != nullptr) {
            if ((strcmp(dirContext->d_name, curDir) == 0) || strcmp(dirContext->d_name, upperDir) == 0) {
                continue;
            }
            errno_t rc = sprintf_s(subDirPath, VFS_FILE_PATH_MAX_LEN, "%s/%s", dirPath, dirContext->d_name);
            storage_securec_check_ss(rc);
            StressRemoveDir(subDirPath);
        }
        closedir(dirHandle);
        rmdir(dirPath);
    } else {
        remove(dirPath);
    }
    return 0;
}

/*
 * Helper: duplicate a non-empty JSON string value, returning nullptr for empty/null.
 * Caller owns the returned memory (must free with free()).
 */
static char *DupJsonStr(cJSON *json, const char *key)
{
    const char *val = STRESS_VERIFY::GetJsonStr(json, key, "");
    if (val[0] == '\0') {
        return nullptr;
    }
    char *dup = strdup(val);
    if (dup == nullptr) {
        printf("ERROR: strdup failed (OOM) for key '%s'\n", key);
        /* 返回 nullptr，调用方的 GUC 字段允许为 nullptr */
    }
    return dup;
}

static void StressLoadGucConfig()
{
    /* Try absolute path first (cwd may have changed to dataDir) */
    std::string gucPath = GUC_CONFIG_PATH;
    if (!g_stressBaseDir.empty()) {
        gucPath = g_stressBaseDir + "/" + GUC_CONFIG_PATH;
    }
    std::ifstream configFile(gucPath);
    if (!configFile.is_open()) {
        /* Fallback: try relative path in current directory */
        configFile.open(GUC_CONFIG_PATH);
    }
    if (!configFile.is_open()) {
        std::cout << "Could not open config file: " << gucPath << std::endl;
        exit(1);
    }
    g_stressIsReadGuc = true;
    std::string configMsg((std::istreambuf_iterator<char>(configFile)), std::istreambuf_iterator<char>());
    configFile.close();
    std::cout << "--------------------"
              << "GUC Config in " << GUC_CONFIG_PATH << "--------------------" << std::endl;

    cJSON *configJson = cJSON_Parse(configMsg.c_str());
    if (configJson == nullptr) {
        std::cout << "ERROR: Failed to parse GUC config JSON" << std::endl;
        exit(1);
    }

    using STRESS_VERIFY::GetJsonInt;
    using STRESS_VERIFY::GetJsonStr;

    g_stressGuc.selfNodeId = GetJsonInt(configJson, "selfNodeId", 0);
    g_stressGuc.buffer = GetJsonInt(configJson, "buffer", 0);
    g_stressGuc.bufferLruPartition = GetJsonInt(configJson, "bufferLruPartition", 0);
    g_stressGuc.checkpointTimeout = GetJsonInt(configJson, "checkpointTimeout", 0);
    g_stressGuc.defaultIsolationLevel = GetJsonInt(configJson, "defaultIsolationLevel", 0);
    g_stressGuc.maintenanceWorkMem = GetJsonInt(configJson, "maintenanceWorkMem", 0);

    g_stressGuc.dataDir = DupJsonStr(configJson, "dataDir");

    g_stressGuc.ncores = GetJsonInt(configJson, "ncores", 0);
    g_stressGuc.logMinMessages = GetJsonInt(configJson, "logMinMessages", 0);
    g_stressGuc.foldPeriod = GetJsonInt(configJson, "foldPeriod", 0);
    g_stressGuc.foldThreshold = GetJsonInt(configJson, "foldThreshold", 0);
    g_stressGuc.foldLevel = GetJsonInt(configJson, "foldLevel", 0);
    g_stressGuc.csnAssignmentIncrement = GetJsonInt(configJson, "csnAssignmentIncrement", 0);

    g_stressGuc.moduleLoggingConfigure = DupJsonStr(configJson, "moduleLoggingConfigure");

    g_stressGuc.lockHashTableSize = GetJsonInt(configJson, "lockHashTableSize", 0);
    g_stressGuc.lockTablePartitionNum = GetJsonInt(configJson, "lockTablePartitionNum", 0);
    g_stressGuc.enableLazyLock = GetJsonInt(configJson, "enableLazyLock", 0);

    g_stressGuc.vfsTenantIsolationConfigPath = DupJsonStr(configJson, "vfsTenantIsolationConfigPath");

    g_stressGuc.updateCsnMinInterval = GetJsonInt(configJson, "updateCsnMinInterval", 0);
    g_stressGuc.numObjSpaceMgrWorkers = GetJsonInt(configJson, "numObjSpaceMgrWorkers", 0);
    g_stressGuc.minFreePagePercentageThreshold1 =
        GetJsonInt(configJson, "minFreePagePercentageThreshold1", 0);
    g_stressGuc.minFreePagePercentageThreshold2 =
        GetJsonInt(configJson, "minFreePagePercentageThreshold2", 0);
    g_stressGuc.probOfExtensionThreshold = GetJsonInt(configJson, "probOfExtensionThreshold", 0);
    g_stressGuc.recoveryWorkerNum = GetJsonInt(configJson, "recoveryWorkerNum", 0);
    g_stressGuc.synchronousCommit = GetJsonInt(configJson, "synchronousCommit", 0);

    g_stressGuc.walStreamCount = GetJsonInt(configJson, "walStreamCount", 0);
    g_stressGuc.walFileNumber = GetJsonInt(configJson, "walFileNumber", 0);

    const char *walFileSizeStr = GetJsonStr(configJson, "walFileSize", "0");
    g_stressGuc.walFileSize = std::stoll(walFileSizeStr);

    g_stressGuc.walBuffers = GetJsonInt(configJson, "walBuffers", 0);

    const char *walReadBufStr = GetJsonStr(configJson, "walReadBufferSize", "0");
    g_stressGuc.walReadBufferSize = std::stoll(walReadBufStr);

    const char *walRedoBufStr = GetJsonStr(configJson, "walRedoBufferSize", "0");
    g_stressGuc.walRedoBufferSize = std::stoll(walRedoBufStr);

    g_stressGuc.walwriterCpuBind = GetJsonInt(configJson, "walwriterCpuBind", 0);
    g_stressGuc.redoBindCpuAttr = DupJsonStr(configJson, "redoBindCpuAttr");
    g_stressGuc.numaNodeNum = GetJsonInt(configJson, "numaNodeNum", 0);
    g_stressGuc.disableBtreePageRecycle = GetJsonInt(configJson, "disableBtreePageRecycle", 0);
    g_stressGuc.deadlockTimeInterval = GetJsonInt(configJson, "deadlockTimeInterval", 0);
    g_stressGuc.recycleFsmTimeInterval = GetJsonInt(configJson, "recycleFsmTimeInterval", 0);
    g_stressGuc.probOfUpdateFsmTimestamp = GetJsonInt(configJson, "probOfUpdateFsmTimestamp", 0);
    g_stressGuc.probOfRecycleFsm = GetJsonInt(configJson, "probOfRecycleFsm", 0);
    g_stressGuc.probOfRecycleBtree = GetJsonInt(configJson, "probOfRecycleBtree", 0);
    g_stressGuc.distLockMaxRingSize = GetJsonInt(configJson, "distLockMaxRingSize", 0);
    g_stressGuc.csnMode = static_cast<DSTORE::CsnMode>(GetJsonInt(configJson, "csnMode", 0));
    g_stressGuc.ctrlPlanePort = GetJsonInt(configJson, "ctrlPlanePort", 0);
    g_stressGuc.rdmaGidIndex = GetJsonInt(configJson, "rdmaGidIndex", 0);
    g_stressGuc.rdmaIbPort = GetJsonInt(configJson, "rdmaIbPort", 0);
    g_stressGuc.pdReadAuthResetPeriod = GetJsonInt(configJson, "pdReadAuthResetPeriod", 0);
    g_stressGuc.csnThreadBindCpu = GetJsonInt(configJson, "csnThreadBindCpu", 0);

    g_stressGuc.commConfigStr = DupJsonStr(configJson, "commConfigStr");

    g_stressGuc.commThreadMin = GetJsonInt(configJson, "commThreadMin", 0);
    g_stressGuc.commThreadMax = GetJsonInt(configJson, "commThreadMax", 0);
    g_stressGuc.clusterId = GetJsonInt(configJson, "clusterId", 0);

    g_stressGuc.memberView = DupJsonStr(configJson, "memberView");

    g_stressGuc.commProtocolTypeStr = DupJsonStr(configJson, "commProtocolTypeStr");

    g_stressGuc.commProtocolType = GetJsonInt(configJson, "commProtocolType", 0);
    g_stressGuc.globalClockAdjustWaitTimeUs = GetJsonInt(configJson, "globalClockAdjustWaitTimeUs", 0);
    g_stressGuc.globalClockSyncIntervalMs = GetJsonInt(configJson, "globalClockSyncIntervalMs", 0);
    g_stressGuc.gclockOverlapWaitTimeOptimization =
        GetJsonInt(configJson, "gclockOverlapWaitTimeOptimization", 0);
    g_stressGuc.enableQuickStartUp = GetJsonInt(configJson, "enableQuickStartUp", 0);
    g_stressGuc.defaultHeartbeatTimeoutInterval =
        GetJsonInt(configJson, "defaultHeartbeatTimeoutInterval", 0);
    g_stressGuc.defaultWalSizeThreshold = GetJsonInt(configJson, "defaultWalSizeThreshold", 0);
    g_stressGuc.bgDiskWriterSlaveNum = GetJsonInt(configJson, "bgDiskWriterSlaveNum", 0);
    g_stressGuc.bgPageWriterSleepMilliSecond = GetJsonInt(configJson, "bgPageWriterSleepMilliSecond", 0);
    g_stressGuc.walThrottlingSize = GetJsonInt(configJson, "walThrottlingSize", 0);
    g_stressGuc.maxIoCapacityKb = GetJsonInt(configJson, "maxIoCapacityKb", 0);

    const char *bgWalMinStr = GetJsonStr(configJson, "bgWalWriterMinBytes", "0");
    g_stressGuc.bgWalWriterMinBytes = std::stoll(bgWalMinStr);

    const char *walEachWriteStr = GetJsonStr(configJson, "walEachWriteLenghthLimit", "0");
    g_stressGuc.walEachWriteLenghthLimit = std::stoll(walEachWriteStr);

    g_stressGuc.tenantConfig = new TenantConfig;
    assert(g_stressGuc.tenantConfig != nullptr);
    assert(memset_s(g_stressGuc.tenantConfig, sizeof(TenantConfig), 0, sizeof(TenantConfig)) == EOK);
    const char *startCfgPath = GetJsonStr(configJson, "startConfigPath", "");
    if (strlen(startCfgPath) > 0) {
        RetStatus ret = TenantConfigInterface::GetTenantConfig(startCfgPath, g_stressGuc.tenantConfig);
        StorageReleasePanic(STORAGE_FUNC_FAIL(ret), DSTORE::MODULE_FRAMEWORK, ErrMsg("GetTenantConfig fail."));
    }

    cJSON_Delete(configJson);
}

namespace DSTORE {
namespace STRESS_VERIFY {

void StressVerifyStorageInstance::Init(const char *dataDirBase)
{
    StressLoadGucConfig();

    char dataDir[VFS_FILE_PATH_MAX_LEN] = {0};
    __attribute__((__unused__)) int rc =
        sprintf_s(dataDir, VFS_FILE_PATH_MAX_LEN, "%s/stress_verify_dir/", dataDirBase);
    storage_securec_check_ss(rc);

    char dstoreDir[VFS_FILE_PATH_MAX_LEN] = {0};
    rc = sprintf_s(dstoreDir, VFS_FILE_PATH_MAX_LEN, "%s/%s/", dataDir, DSTORE::BASE_DIR);
    storage_securec_check_ss(rc);

    char pdbMetaDataPath[VFS_FILE_PATH_MAX_LEN] = {0};
    rc = sprintf_s(pdbMetaDataPath, VFS_FILE_PATH_MAX_LEN, "%s/%s/", dataDir, "metadata");
    storage_securec_check_ss(rc);

    char walPath[VFS_FILE_PATH_MAX_LEN] = {0};
    rc = sprintf_s(walPath, VFS_FILE_PATH_MAX_LEN, "%s/%s/", dataDir, "dstore_wal");
    storage_securec_check_ss(rc);

    StressRemoveDir(dataDir);
    __attribute__((__unused__)) int ret = mkdir(dataDir, 0777);
    assert(ret != -1);
    chdir(dataDir);
    ret = mkdir(dstoreDir, 0777);
    assert(ret != -1);

    bool flag = VfsInterface::ModuleInitialize();
    StorageReleasePanic(!flag, DSTORE::MODULE_FRAMEWORK, ErrMsg("ModuleInitialize fail."));
    VfsInterface::SetupTenantIsoland(g_stressGuc.tenantConfig, dstoreDir);
    RetStatus retStatus = VfsInterface::CreateTenantDefaultVfs(g_stressGuc.tenantConfig);
    StorageReleasePanic(STORAGE_FUNC_FAIL(retStatus), DSTORE::MODULE_FRAMEWORK,
                        ErrMsg("CreateTenantDefaultVfs fail."));

    ret = mkdir(pdbMetaDataPath, 0777);
    assert(ret != -1);
    ret = mkdir(walPath, 0777);
    assert(ret != -1);

    StorageGUC guc = g_stressGuc;
    if (guc.dataDir == nullptr) {
        guc.dataDir = dataDir;
    }
    guc.recoveryWorkerNum = 1;

    std::string logFileName = dataDir;
    logFileName = logFileName + "/stress_verify.log";
    InitLogAdapterInstance(guc.logMinMessages, logFileName.c_str(), guc.foldPeriod, guc.foldThreshold,
                           guc.foldLevel);

    g_instance = StorageInstanceInterface::Create(DSTORE::StorageInstanceType::SINGLE);
    g_instance->InitWorkingVersionNum(&STRESS_VERIFY_GRAND_VERSION_NUM);

    SetDefaultPdbId(PDB_TEMPLATE1_ID);
    ThreadContextInterface *thrd = ThreadContextInterface::Create();
    if (STORAGE_VAR_NULL(thrd)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
               ErrMsg("Failed to create thread context for stress verify bootstrap."));
        return;
    }
    (void)thrd->InitializeBasic();
    StorageSession *sc = CreateStorageSession(1ULL);
    thrd->AttachSessionToThread(sc);
    (void)g_instance->Bootstrap(&guc);
    (void)thrd->InitStorageContext(g_defaultPdbId);
    g_instance->AddVisibleThread(thrd, g_defaultPdbId);
    CreateTemplateTablespace(DSTORE::g_defaultPdbId);
    CreateUndoMapSegment(DSTORE::g_defaultPdbId);

    (void)thrd->InitTransactionRuntime(g_defaultPdbId, nullptr, nullptr);
    InitVfsClientHandles();
}

void StressVerifyStorageInstance::InitFinished()
{
    StoragePdbInterface::FlushAllDirtyPages(g_defaultPdbId);
    g_instance->UnregisterThread();
    g_instance->BootstrapDestroy();
    g_instance->BootstrapResDestroy();
    StorageInstanceInterface::DestoryInstance();
    g_instance = nullptr;
    StopLogAdapterInstance();
}

void StressVerifyStorageInstance::Start(Oid allocMaxRelOid, const char *dataDir)
{
    SetDefaultPdbId(PDB_TEMPLATE1_ID);

    if (!g_stressIsReadGuc) {
        StressLoadGucConfig();
    }
    StorageGUC guc = g_stressGuc;
    if (guc.dataDir == nullptr) {
        guc.dataDir = const_cast<char *>(dataDir);
    }
    chdir(guc.dataDir);

    std::string logFileName = guc.dataDir;
    logFileName = logFileName + "/stress_verify.log";
    InitLogAdapterInstance(guc.logMinMessages, logFileName.c_str(), guc.foldPeriod, guc.foldThreshold,
                           guc.foldLevel);

    g_instance = StorageInstanceInterface::Create(DSTORE::StorageInstanceType::SINGLE);
    g_instance->InitWorkingVersionNum(&STRESS_VERIFY_GRAND_VERSION_NUM);

    ThreadContextInterface *thrd = ThreadContextInterface::Create();
    if (STORAGE_VAR_NULL(thrd)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
               ErrMsg("Failed to create thread context for stress verify start."));
        return;
    }
    (void)thrd->InitializeBasic();
    StorageSession *sc = CreateStorageSession(1ULL);
    thrd->AttachSessionToThread(sc);
    (void)g_instance->StartupInstance(&guc);
    (void)thrd->InitStorageContext(g_defaultPdbId);

    (void)thrd->InitTransactionRuntime(g_defaultPdbId, nullptr, nullptr);
    simulator = new StorageTableContext(g_instance, allocMaxRelOid);
}

void StressVerifyStorageInstance::Stop()
{
    StoragePdbInterface::FlushAllDirtyPages(g_defaultPdbId);
    if (simulator != nullptr) {
        simulator->Destory();
        delete simulator;
        simulator = nullptr;
    }
    g_instance->StopAcceptNewConnection();
    g_instance->ShutdownInstance();
    StorageInstanceInterface::DestoryInstance();
    g_instance = nullptr;
    StopLogAdapterInstance();
    if (g_stressGuc.tenantConfig != nullptr) {
        delete g_stressGuc.tenantConfig;
        g_stressGuc.tenantConfig = nullptr;
    }
}

void StressVerifyStorageInstance::ApplyDfxConfig(uint8_t verifyLevel, uint64_t verifyModules)
{
    /* Map stress_verify level enum to DFX VerifyLevel enum */
    DSTORE::VerifyLevel dfxLevel = DSTORE::VerifyLevel::NONE;
    switch (verifyLevel) {
        case 1: dfxLevel = DSTORE::VerifyLevel::LIGHT; break;
        case 2: dfxLevel = DSTORE::VerifyLevel::MEDIUM; break;
        case 3: dfxLevel = DSTORE::VerifyLevel::HEAVY; break;
        default: dfxLevel = DSTORE::VerifyLevel::NONE; break;
    }
    DSTORE::SetDfxVerifyLevel(dfxLevel);
    DSTORE::SetDfxVerifyModules(verifyModules);
    std::cout << "[DFX] Verify level set to " << static_cast<int>(dfxLevel)
              << ", modules=0x" << std::hex << verifyModules << std::dec << std::endl;
}

void StressVerifyStorageInstance::SetBaseDir(const char *dir)
{
    if (dir != nullptr) {
        g_stressBaseDir = dir;
    }
}

} /* namespace STRESS_VERIFY */
} /* namespace DSTORE */

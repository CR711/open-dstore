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

#include <cassert>
#include <cstring>
#include <iostream>
#include <unistd.h>

#include "common/dstore_datatype.h"
#include "config/dstore_vfs_config.h"
#include "securec.h"
#include "stress_verify_client.h"
#include "stress_verify_server.h"
#include "table_handler.h"
#include "dfx/dstore_page_verify.h"
#include "fault_injector.h"

using namespace DSTORE;

int main(int argc, char *argv[])
{
    (void)argc;
    (void)argv;

    /* Step 1: Get current working directory as base for data directory */
    char utTopDir[VFS_FILE_PATH_MAX_LEN] = {0};
    getcwd(utTopDir, VFS_FILE_PATH_MAX_LEN);

    char dataDir[VFS_FILE_PATH_MAX_LEN] = {0};
    __attribute__((__unused__)) int rc =
        sprintf_s(dataDir, VFS_FILE_PATH_MAX_LEN, "%s/stress_verify_dir", utTopDir);
    storage_securec_check_ss(rc);

    /* Save original cwd so guc.json can be found after chdir into dataDir */
    STRESS_VERIFY::StressVerifyStorageInstance::SetBaseDir(utTopDir);

    /* Step 2: Load config to determine command */
    STRESS_VERIFY::StressVerifyStorage storage;
    storage.Init(1);
    STRESS_VERIFY::StressVerifyConfig cfg = storage.GetConfig();

    /* Step 3: Bootstrap or start storage engine */
    uint32_t allocedMaxRelOid = 1;

    if (cfg.command == STRESS_VERIFY::CMD_PREPARE || cfg.command == STRESS_VERIFY::CMD_ALL) {
        /* Prepare needs a fresh initdb: bootstrap, then transition to normal mode */
        STRESS_VERIFY::StressVerifyStorageInstance::Init(utTopDir);
        STRESS_VERIFY::StressVerifyStorageInstance::InitFinished();
        chdir(dataDir);
        DSTORE::InitVfsClientHandles();
        STRESS_VERIFY::StressVerifyStorageInstance::Start(allocedMaxRelOid, dataDir);
        STRESS_VERIFY::StressVerifyStorageInstance::ApplyDfxConfig(
            static_cast<uint8_t>(cfg.verifyLevel), cfg.verifyModules);
    } else {
        /* Run / Cleanup reuse existing data directory */
        chdir(dataDir);
        DSTORE::InitVfsClientHandles();
        STRESS_VERIFY::StressVerifyStorageInstance::Start(1, dataDir);
        STRESS_VERIFY::StressVerifyStorageInstance::ApplyDfxConfig(
            static_cast<uint8_t>(cfg.verifyLevel), cfg.verifyModules);
    }

    /* Step 4: Dispatch to the appropriate phase(s) */
    int exitCode = 0;
    switch (cfg.command) {
        case STRESS_VERIFY::CMD_PREPARE:
            storage.CreateTables(&allocedMaxRelOid);
            storage.LoadData();
            storage.CreateIndexes(&allocedMaxRelOid);
            break;

        case STRESS_VERIFY::CMD_RUN:
            storage.Execute();
            break;

        case STRESS_VERIFY::CMD_CLEANUP:
            storage.DropTables();
            break;

        case STRESS_VERIFY::CMD_ALL:
            storage.CreateTables(&allocedMaxRelOid);
            storage.LoadData();
            storage.CreateIndexes(&allocedMaxRelOid);
            storage.Execute();
            storage.DropTables();
            break;

        default:
            std::cout << "Unknown command" << std::endl;
            exitCode = 3;  /* Configuration error */
            break;
    }

    /* Step 5: Determine exit code based on verification results */
    if (exitCode == 0 && cfg.faultInjectEnabled) {
        /* Fault injection mode: check if errors were detected */
        uint64_t totalErrors = storage.GetStats()->GetTotalVerifyErrors();
        if (totalErrors > 0) {
            std::cout << "\nResult: FAULT DETECTED (verification working correctly)" << std::endl;
            exitCode = 1;  /* Errors found, which is expected in fault injection mode */
        } else if (storage.GetStats()->GetTotalVerifyErrors() == 0 &&
                   cfg.faultInjectTime > 0) {
            std::cout << "\nResult: FAIL (fault injection enabled but no errors detected)" << std::endl;
            exitCode = 2;  /* Fault was injected but not detected - verification issue */
        }
    } else if (exitCode == 0) {
        /* Normal mode: verify no errors */
        uint64_t totalErrors = storage.GetStats()->GetTotalVerifyErrors();
        if (totalErrors > 0) {
            std::cout << "\nResult: FAIL (unexpected verification errors)" << std::endl;
            exitCode = 1;
        } else {
            std::cout << "\nResult: PASS (no verification errors)" << std::endl;
        }
    }

    /* Step 6: Shutdown */
    STRESS_VERIFY::StressVerifyStorageInstance::Stop();

    std::cout << "dstore_stress_verify done. exit_code=" << exitCode << std::endl;
    return exitCode;
}
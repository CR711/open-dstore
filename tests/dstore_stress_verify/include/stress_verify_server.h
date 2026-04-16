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

#ifndef STRESS_VERIFY_SERVER_H
#define STRESS_VERIFY_SERVER_H

#include "framework/dstore_instance_interface.h"
#include "common/dstore_common_utils.h"

namespace DSTORE {
namespace STRESS_VERIFY {

class StressVerifyStorageInstance {
public:
    /* Create data directory, init VFS, bootstrap storage engine */
    static void Init(const char *dataDirBase);
    /* Flush pages and tear down the bootstrap instance */
    static void InitFinished();
    /* Startup storage engine from existing data directory */
    static void Start(Oid allocMaxRelOid, const char *dataDir);
    /* Flush and destroy storage engine */
    static void Stop();
    /* Apply DFX page verification GUC settings */
    static void ApplyDfxConfig(uint8_t verifyLevel, uint64_t verifyModules);
    /* Save original working directory for locating config files after chdir */
    static void SetBaseDir(const char *dir);
};

} /* namespace STRESS_VERIFY */
} /* namespace DSTORE */

constexpr uint32_t STRESS_VERIFY_GRAND_VERSION_NUM = 97040;

#endif /* STRESS_VERIFY_SERVER_H */
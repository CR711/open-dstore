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
#include "ut_transaction/ut_transaction.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_mgr.h"
#include "transaction/dstore_transaction_interface.h"
#include "errorcode/dstore_transaction_error_code.h"
#include "common/dstore_common_utils.h"
#include "mockcpp/mokc.h"
#include <thread>

static void CheckTransactionState(char c, TBlockState tBlockState, TransState transState)
{
    Transaction *txn = thrd->GetActiveTransaction();
    EXPECT_EQ(TransactionInterface::GetStatusCode(), c);
    EXPECT_EQ(txn->m_currTransState.blockState, tBlockState);
    EXPECT_EQ(txn->m_currTransState.state, transState);
}

TEST_F(UTTransactionTest, TransactionInterfaceTest_level0)
{
    RetStatus status;
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);
    EXPECT_TRUE(TransactionInterface::TrxBlockIsDefault());

    /* Test1-1: simple query transaction commit */
    TransactionInterface::StartTrxCommand();
    CheckTransactionState('I', TBlockState::TBLOCK_STARTED, TransState::TRANS_INPROGRESS);
    EXPECT_TRUE(TransactionInterface::TrxIsInProgress());
    TransactionInterface::CommitTrxCommand();
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test1-2: simple query transaction abort */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::AbortTrx();
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test2-1: transaction block (BEGIN + COMMIT) */
    TransactionInterface::StartTrxCommand();
    CheckTransactionState('I', TBlockState::TBLOCK_STARTED, TransState::TRANS_INPROGRESS);
    TransactionInterface::BeginTrxBlock();
    CheckTransactionState('T', TBlockState::TBLOCK_BEGIN, TransState::TRANS_INPROGRESS);
    TransactionInterface::CommitTrxCommand();
    CheckTransactionState('T', TBlockState::TBLOCK_INPROGRESS, TransState::TRANS_INPROGRESS);
    EXPECT_TRUE(TransactionInterface::IsTrxBlock());

    TransactionInterface::StartTrxCommand();
    CheckTransactionState('T', TBlockState::TBLOCK_INPROGRESS, TransState::TRANS_INPROGRESS);
    TransactionInterface::EndTrxBlock();
    CheckTransactionState('T', TBlockState::TBLOCK_END, TransState::TRANS_INPROGRESS);
    TransactionInterface::CommitTrxCommand();
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test2-2: transaction block (BEGIN + ROLLBACK) */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::BeginTrxBlock();
    TransactionInterface::CommitTrxCommand();

    TransactionInterface::StartTrxCommand();
    TransactionInterface::UserAbortTrxBlock();
    CheckTransactionState('E', TBlockState::TBLOCK_ABORT_PENDING, TransState::TRANS_INPROGRESS);
    TransactionInterface::CommitTrxCommand();
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test3-1: transaction block (BEGIN + sql error + COMMIT) */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::BeginTrxBlock();
    TransactionInterface::CommitTrxCommand();

    TransactionInterface::StartTrxCommand();
    TransactionInterface::AbortTrx();    /* If syntax error or other database error happen, sql engine will call Abort */
    CheckTransactionState('E', TBlockState::TBLOCK_ABORT, TransState::TRANS_INPROGRESS);
    EXPECT_TRUE(TransactionInterface::TrxBlockIsAborted());

    TransactionInterface::StartTrxCommand();
    CheckTransactionState('E', TBlockState::TBLOCK_ABORT, TransState::TRANS_INPROGRESS);
    TransactionInterface::EndTrxBlock();
    CheckTransactionState('E', TBlockState::TBLOCK_ABORT_END, TransState::TRANS_INPROGRESS);
    TransactionInterface::CommitTrxCommand();
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test3-2: transaction block (BEGIN + sql error + ROLLBACK) */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::BeginTrxBlock();
    TransactionInterface::CommitTrxCommand();

    TransactionInterface::StartTrxCommand();
    TransactionInterface::AbortTrx();    /* If syntax error or other database error happen, sql engine will call Abort */

    TransactionInterface::StartTrxCommand();
    TransactionInterface::UserAbortTrxBlock();
    CheckTransactionState('E', TBlockState::TBLOCK_ABORT_END, TransState::TRANS_INPROGRESS);
    TransactionInterface::AbortTrx();
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test3-3: transaction block (BEGIN fail) */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::BeginTrxBlock();
    TransactionInterface::AbortTrx();
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test3-4: transaction block (COMMIT fail) */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::BeginTrxBlock();
    TransactionInterface::CommitTrxCommand();

    TransactionInterface::StartTrxCommand();
    TransactionInterface::EndTrxBlock();
    TransactionInterface::AbortTrx();
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test3-5: transaction block (ROLLBACK fail) */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::BeginTrxBlock();
    TransactionInterface::CommitTrxCommand();

    TransactionInterface::StartTrxCommand();
    TransactionInterface::UserAbortTrxBlock();
    TransactionInterface::AbortTrx();
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test3-5: transaction block (COMMIT) */
    TransactionInterface::StartTrxCommand();
    status = TransactionInterface::EndTrxBlock();
    EXPECT_EQ(status, DSTORE_FAIL);
    EXPECT_EQ(StorageGetErrorCode(), TRANSACTION_WARNING_NO_ACTIVE_TRANSACTION);
    TransactionInterface::CommitTrxCommand();
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test3-6: transaction block (ROLLBACK) */
    TransactionInterface::StartTrxCommand();
    status = TransactionInterface::UserAbortTrxBlock();
    EXPECT_EQ(status, DSTORE_FAIL);
    EXPECT_EQ(StorageGetErrorCode(), TRANSACTION_WARNING_NO_ACTIVE_TRANSACTION);
    TransactionInterface::CommitTrxCommand();
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test3-7: transaction block (BEGIN + BEGIN + COMMIT) */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::BeginTrxBlock();
    TransactionInterface::CommitTrxCommand();

    TransactionInterface::StartTrxCommand();
    status = TransactionInterface::BeginTrxBlock();
    EXPECT_EQ(status, DSTORE_FAIL);
    EXPECT_EQ(StorageGetErrorCode(), TRANSACTION_WARNING_ACTIVE_SQL_TRANSACTION);
    CheckTransactionState('T', TBlockState::TBLOCK_INPROGRESS, TransState::TRANS_INPROGRESS);
    TransactionInterface::CommitTrxCommand();
    CheckTransactionState('T', TBlockState::TBLOCK_INPROGRESS, TransState::TRANS_INPROGRESS);

    TransactionInterface::StartTrxCommand();
    TransactionInterface::EndTrxBlock();
    TransactionInterface::CommitTrxCommand();
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test4-1: jdbc client exits abnormally in a normal transaction block */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::BeginTrxBlock();
    TransactionInterface::CommitTrxCommand();

    TransactionInterface::AbortTrx(true);
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test4-2: jdbc client exits abnormally in a failed transaction block */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::BeginTrxBlock();
    TransactionInterface::CommitTrxCommand();

    TransactionInterface::StartTrxCommand();
    TransactionInterface::AbortTrx();
    CheckTransactionState('E', TBlockState::TBLOCK_ABORT, TransState::TRANS_INPROGRESS);

    TransactionInterface::AbortTrx(true);
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);
    
    /* Test5: impossible call order from sql engine */
    status = TransactionInterface::CommitTrxCommand();
    EXPECT_EQ(status, DSTORE_FAIL);
    EXPECT_EQ(StorageGetErrorCode(), TRANSACTION_WARNING_NO_ACTIVE_TRANSACTION);
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    status = TransactionInterface::AbortTrx();
    EXPECT_EQ(status, DSTORE_FAIL);
    EXPECT_EQ(StorageGetErrorCode(), TRANSACTION_WARNING_NO_ACTIVE_TRANSACTION);
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);
    CheckTransactionState('I', TBlockState::TBLOCK_DEFAULT, TransState::TRANS_DEFAULT);

    /* Test6: test print */
    Transaction *txn = thrd->GetActiveTransaction();
    EXPECT_STREQ(txn->BlockStateAsString(TBlockState::TBLOCK_DEFAULT), "DEFAULT");
    EXPECT_STREQ(txn->BlockStateAsString(TBlockState::TBLOCK_STARTED), "STARTED");
    EXPECT_STREQ(txn->BlockStateAsString(TBlockState::TBLOCK_BEGIN), "BEGIN");
    EXPECT_STREQ(txn->BlockStateAsString(TBlockState::TBLOCK_INPROGRESS), "INPROGRESS");
    EXPECT_STREQ(txn->BlockStateAsString(TBlockState::TBLOCK_END), "END");
    EXPECT_STREQ(txn->BlockStateAsString(TBlockState::TBLOCK_ABORT), "ABORT");
    EXPECT_STREQ(txn->BlockStateAsString(TBlockState::TBLOCK_ABORT_END), "ABORT END");
    EXPECT_STREQ(txn->BlockStateAsString(TBlockState::TBLOCK_ABORT_PENDING), "ABORT PEND");

    EXPECT_STREQ(txn->TransStateAsString(TransState::TRANS_DEFAULT), "DEFAULT");
    EXPECT_STREQ(txn->TransStateAsString(TransState::TRANS_START), "START");
    EXPECT_STREQ(txn->TransStateAsString(TransState::TRANS_INPROGRESS), "INPROGR");
    EXPECT_STREQ(txn->TransStateAsString(TransState::TRANS_COMMIT), "COMMIT");
    EXPECT_STREQ(txn->TransStateAsString(TransState::TRANS_ABORT), "ABORT");

    /* Test7: other interface */
    TransactionInterface::StartTrxCommand();
    CommitSeqNo flashbackCsn;
    TransactionInterface::GetSnapshotCsnForFlashback(flashbackCsn);
    TransactionInterface::SetSnapshotCsnForFlashback(flashbackCsn);
    EXPECT_EQ(flashbackCsn, 1);
    EXPECT_EQ(TransactionInterface::GetCurCid(), 0);
    TransactionInterface::AssignXid();
    uint64 xidU64 = TransactionInterface::GetCurrentXid();
    TransactionInterface::IncreaseCommandCounter();
    TransactionInterface::SetCurCidUsed();
    EXPECT_EQ(TransactionInterface::GetCurCid(), 1);
    TransactionInterface::CommitTrxCommand();
    if (unlikely(TransactionInterface::Dump(xidU64) != nullptr)) {
        EXPECT_STREQ(TransactionInterface::Dump(xidU64), "xid (0, 0)");
    }
}

TEST_F(UTTransactionTest, TransactionGetTacTransactionState_level0)
{
    SetupTableHandler();
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    Xid xid;
    TACTransactionState trxState;
    HeapTuple *tuple;

    /* Step 1: Check TAC unknown transaction state */
    /* Step 1.1: Start a new transaction */
    ASSERT_TRUE(STORAGE_FUNC_SUCC(activeTransaction->Start()));

    /* Xid must be invalid */
    xid = activeTransaction->GetCurrentXid();
    ASSERT_EQ(xid, INVALID_XID);

    /* Step 1.2: Get transaction state associated with xid */
    trxState = static_cast<TACTransactionState>(
        TransactionInterface::GetTacTransactionState(DSTORE::g_defaultPdbId, xid.m_placeHolder));

    /*
     * Step 1.3: Check if the transaction state is unknown as
     * the transaction has not made any changes yet.
     */
    ASSERT_EQ(trxState, TACTransactionState::TAC_TRX_UNKNOWN);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(activeTransaction->Commit()));

    /* Step 2: Check TAC inprogress transaction state */
    /* Step 2.1: Start a new transaction */
    ASSERT_TRUE(STORAGE_FUNC_SUCC(activeTransaction->Start()));

    /* Insert a heap tuple */
    tuple = InsertHeapTuple("row");
    DstorePfreeExt(tuple);

    /* Xid must be valid as the transaction has not completed yet */
    xid = activeTransaction->GetCurrentXid();
    ASSERT_NE(xid, INVALID_XID);

    /* Step 2.2: Get transaction state associated with xid */
    trxState = static_cast<TACTransactionState>(
        TransactionInterface::GetTacTransactionState(DSTORE::g_defaultPdbId, xid.m_placeHolder));

    /*
     * Step 2.3: Check if the transaction state is unknown as
     * the transaction xid has not completed yet.
     */
    ASSERT_EQ(trxState, TACTransactionState::TAC_TRX_IN_PROGRESS);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(activeTransaction->Commit()));

    /* Step 3: Check TAC committed transaction state */
    /* Step 3.1: Start a new transaction */
    ASSERT_TRUE(STORAGE_FUNC_SUCC(activeTransaction->Start()));

    /* Insert a heap tuple */
    tuple = InsertHeapTuple("row");
    DstorePfreeExt(tuple);

    /* Xid must be valid as the transaction has not completed yet */
    xid = activeTransaction->GetCurrentXid();
    ASSERT_NE(xid, INVALID_XID);

    /* Commit the transaction */
    ASSERT_TRUE(STORAGE_FUNC_SUCC(activeTransaction->Commit()));

    /*
     * Step 3.2: Get transaction state associated with xid
     * and check if it is committed.
     */
    trxState = static_cast<TACTransactionState>(
        TransactionInterface::GetTacTransactionState(DSTORE::g_defaultPdbId, xid.m_placeHolder));
    ASSERT_EQ(trxState, TACTransactionState::TAC_TRX_COMMITTED);

    /* Step 4: Check TAC aborted transaction state */
    /* Step 4.1: Start a new transaction */
    ASSERT_TRUE(STORAGE_FUNC_SUCC(activeTransaction->Start()));

    /* Insert a heap tuple */
    tuple = InsertHeapTuple("row");
    DstorePfreeExt(tuple);

    /* Xid must be valid as the transaction has not completed yet */
    xid = activeTransaction->GetCurrentXid();
    ASSERT_NE(xid, INVALID_XID);

    /* Abort the transaction */
    ASSERT_TRUE(STORAGE_FUNC_SUCC(activeTransaction->Abort()));

    /*
     * Step 4.2: Get transaction state associated with xid
     * and check if it is committed.
     */
    trxState = static_cast<TACTransactionState>(
        TransactionInterface::GetTacTransactionState(DSTORE::g_defaultPdbId, xid.m_placeHolder));
    ASSERT_EQ(trxState, TACTransactionState::TAC_TRX_ABORTED);
}

#define TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED "Transacton runtime is not initialized."

TEST_F(UTTransactionTest, TransactionNullptr_level0)
{
    Transaction *savedTrx = thrd->GetTransactionList().m_activeTransaction;
    thrd->GetTransactionList().m_activeTransaction = nullptr;
    RetStatus ret;
    CommitSeqNo csn;
    CommandId cid;
    const char *emptyName = nullptr;
    const char* name = "name";
    ret = TransactionInterface::StartTrxCommand();
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::CommitTrxCommand();
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::CommitTrxCommandWithoutCleanUpResource();
    ASSERT_EQ(ret, DSTORE_FAIL);
    const char *err;
    StorageClearError();
    TransactionInterface::CleanUpResourceAfterCommit();
    err = StorageGetMessage();
    ASSERT_STREQ(err, TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
    ret = TransactionInterface::AbortTrx();
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::CommitRollbackAndRestoreTrxState(true);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::BeginTrxBlock();
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::EndTrxBlock();
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::RollbackLastSQLCmd();
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::UserAbortTrxBlock();
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::SetSnapShot();
    ASSERT_EQ(ret, DSTORE_FAIL);
    StorageClearError();
    TransactionInterface::SetSnapshotCsnForFlashback(csn);
    err = StorageGetMessage();
    ASSERT_STREQ(err, TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
    ret = TransactionInterface::GetSnapshotCsnForFlashback(csn);
    ASSERT_EQ(ret, DSTORE_FAIL);
    uint64 xid = TransactionInterface::GetCurrentXid();
    ASSERT_EQ(xid, INVALID_XID.m_placeHolder);
    EXPECT_FALSE(TransactionInterface::IsTacXidSent());
    StorageClearError();
    TransactionInterface::SetTacXidSent(true);
    err = StorageGetMessage();
    ASSERT_STREQ(err, TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
    StorageClearError();
    TransactionInterface::ResetSnapshotCsn();
    err = StorageGetMessage();
    ASSERT_STREQ(err, TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
    ret = TransactionInterface::SetTransactionSnapshotCid(cid);
    ASSERT_EQ(ret, DSTORE_FAIL);
    cid = TransactionInterface::GetTransactionSnapshotCid();
    ASSERT_EQ(cid, FIRST_CID);
    ret = TransactionInterface::SetTransactionSnapshotCsn(csn);
    ASSERT_EQ(ret, DSTORE_FAIL);
    csn = TransactionInterface::GetTransactionSnapshotCsn();
    ASSERT_EQ(csn, INVALID_CSN);
    cid = TransactionInterface::GetCurCid();
    ASSERT_EQ(cid, FIRST_CID);
    EXPECT_FALSE(TransactionInterface::TrxIsDefault());
    EXPECT_FALSE(TransactionInterface::TrxIsStart());
    TBlockState state = TransactionInterface::GetCurrentTBlockState();
    ASSERT_EQ(state, TBlockState::TBLOCK_DEFAULT);
    char statusCode = TransactionInterface::GetStatusCode();
    ASSERT_EQ(statusCode, 0);
    EXPECT_FALSE(TransactionInterface::IsAutonomousTransaction());
    int level = TransactionInterface::GetAutonomousTransactionLevel();
    ASSERT_EQ(level, 0);
    void* resPtr = TransactionInterface::GetTransactionExtraResPtr();
    ASSERT_EQ(resPtr, nullptr);
    EXPECT_TRUE(TransactionInterface::IsSavepointListEmpty());
    StorageClearError();
    TransactionInterface::SetTransactionGucLevel(0);
    err = StorageGetMessage();
    ASSERT_STREQ(err, TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
    StorageClearError();
    TransactionInterface::DeleteAllCursorSnapshots();
    err = StorageGetMessage();
    ASSERT_STREQ(err, TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
    ret = TransactionInterface::AddCursorSnapshot(emptyName);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::AddCursorSnapshot(name);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::DeleteCursorSnapshot(emptyName);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::DeleteCursorSnapshot(name);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::CreateSavepoint(emptyName);
    ASSERT_EQ(ret, DSTORE_FAIL);
    int16 counter;
    ret = TransactionInterface::ReleaseSavepoint(emptyName, &counter, &counter);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::SaveExtraResPtrToSavepoint(emptyName, &counter);
    ASSERT_EQ(ret, DSTORE_FAIL);
    resPtr = TransactionInterface::GetExtraResPtrFromSavepoint(emptyName);
    ASSERT_EQ(resPtr, nullptr);
    void *savepoint = TransactionInterface::GetExtraResPtrFromCurrentSavepoint();
    ASSERT_EQ(savepoint, nullptr);
    ret = TransactionInterface::RollbackToSavepoint(emptyName, &counter, &counter);
    ASSERT_EQ(ret, DSTORE_FAIL);
    EXPECT_FALSE(TransactionInterface::HasCurrentSavepointName());
    char *savepointName = TransactionInterface::GetCurrentSavepointName();
    ASSERT_EQ(savepointName, nullptr);
    int32 nestLevel = TransactionInterface::GetSavepointNestLevel();
    ASSERT_EQ(nestLevel, 0);
    EXPECT_TRUE(TransactionInterface::IsMvccSnapshot());
    ret = TransactionInterface::AssignXid();
    ASSERT_EQ(ret, DSTORE_FAIL);
    StorageClearError();
    TransactionInterface::IncreaseCommandCounter();
    err = StorageGetMessage();
    ASSERT_STREQ(err, TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
    StorageClearError();
    TransactionInterface::SetCurCidUsed();
    err = StorageGetMessage();
    ASSERT_STREQ(err, TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
    EXPECT_FALSE(TransactionInterface::IsCurCidUsed());
    DropSegPendingList *list = TransactionInterface::GetDropSegPendingList();
    ASSERT_EQ(list, nullptr);
    StorageClearError();
    TransactionInterface::SetDropSegPendingList(list);
    err = StorageGetMessage();
    ASSERT_STREQ(err, TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
    StorageClearError();
    TransactionInterface::SaveCurTransactionState();
    err = StorageGetMessage();
    ASSERT_STREQ(err, TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
    StorageClearError();
    TransactionInterface::RestoreCurTransactionState();
    err = StorageGetMessage();
    ASSERT_STREQ(err, TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
    StorageClearError();
    TransactionInterface::SetCurTxnBlockState(state);
    err = StorageGetMessage();
    ASSERT_STREQ(err, TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED);
    thrd->GetTransactionList().m_activeTransaction = savedTrx;
}

TEST_F(UTTransactionTest, TransactionInterfaceTest2_level0)
{
    RetStatus ret;
    CommitSeqNo csn;
    CommandId cid;
    const char *emptyName = nullptr;
    const char* name = "name";
    TransactionInterface::StartTrxCommand();
    ret = TransactionInterface::CommitTrxCommandWithoutCleanUpResource();
    ASSERT_EQ(ret, DSTORE_SUCC);
    const char *err;
    TransactionInterface::StartTrxCommand();
    TransactionInterface::BeginTrxBlock();
    TransactionInterface::CommitTrxCommand();
    ret = TransactionInterface::RollbackLastSQLCmd();
    ASSERT_EQ(ret, DSTORE_SUCC);
    TransactionInterface::SetTacXidSent(true);
    EXPECT_TRUE(TransactionInterface::IsTacXidSent());
    TransactionInterface::SetTransactionSnapshotCid(100);
    cid = TransactionInterface::GetTransactionSnapshotCid();
    ASSERT_EQ(cid, 100);
    ret = TransactionInterface::SetTransactionSnapshotCsn(100);
    ASSERT_EQ(ret, DSTORE_SUCC);
    csn = TransactionInterface::GetTransactionSnapshotCsn();
    ASSERT_NE(csn, INVALID_CSN);
    TransactionInterface::StartTrxCommand();
    TBlockState state = TransactionInterface::GetCurrentTBlockState();
    ASSERT_NE(state, TBlockState::TBLOCK_DEFAULT);
    EXPECT_FALSE(TransactionInterface::IsAutonomousTransaction());
    int level = TransactionInterface::GetAutonomousTransactionLevel();
    ASSERT_NE(level, 0);
    TransactionInterface::SetTransactionExtraResPtr(&level);
    void* resPtr = TransactionInterface::GetTransactionExtraResPtr();
    ASSERT_NE(resPtr, nullptr);
    TransactionInterface::CreateSavepoint(name);
    EXPECT_FALSE(TransactionInterface::IsSavepointListEmpty());
    ret = TransactionInterface::AddCursorSnapshot(emptyName);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::AddCursorSnapshot(name);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ret = TransactionInterface::DeleteCursorSnapshot(emptyName);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TransactionInterface::DeleteCursorSnapshot(name);
    ASSERT_EQ(ret, DSTORE_SUCC);
    int16 counter;
    ret = TransactionInterface::ReleaseSavepoint(name, &counter, &counter);
    ASSERT_EQ(ret, DSTORE_SUCC);
    TransactionInterface::CreateSavepoint(name);
    ret = TransactionInterface::SaveExtraResPtrToSavepoint(name, &counter);
    ASSERT_EQ(ret, DSTORE_SUCC);
    resPtr = TransactionInterface::GetExtraResPtrFromSavepoint(name);
    ASSERT_NE(resPtr, nullptr);
    void *savepoint = TransactionInterface::GetExtraResPtrFromCurrentSavepoint();
    ASSERT_NE(savepoint, nullptr);
    EXPECT_TRUE(TransactionInterface::HasCurrentSavepointName());
    char *savepointName = TransactionInterface::GetCurrentSavepointName();
    ASSERT_NE(savepointName, nullptr);
    int32 nestLevel = TransactionInterface::GetSavepointNestLevel();
    ASSERT_NE(nestLevel, 0);
    EXPECT_TRUE(TransactionInterface::IsMvccSnapshot());
    EXPECT_TRUE(TransactionInterface::IsCurCidUsed());
    DropSegPendingList pendingList;
    TransactionInterface::SetDropSegPendingList(&pendingList);
    DropSegPendingList *list = TransactionInterface::GetDropSegPendingList();
    ASSERT_NE(list, nullptr);
}
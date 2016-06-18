/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.elasql.storage.tx.concurrency;

import java.util.HashSet;
import java.util.Set;

import org.elasql.sql.RecordKey;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;

public class ConservativeOrderedCcMgr extends ConcurrencyMgr {
	protected static ConservativeOrderedLockTable lockTbl = new ConservativeOrderedLockTable();

	// TODO: We can record all objects locked by this transaction here

	public ConservativeOrderedCcMgr(long txNumber) {
		txNum = txNumber;
	}

	public void prepareSp(String[] readTables, String[] writeTables) {
		if (readTables != null)
			for (String rt : readTables)
				lockTbl.requestLock(rt, txNum);

		if (writeTables != null)
			for (String wt : writeTables)
				lockTbl.requestLock(wt, txNum);
	}

	public void executeSp(String[] readTables, String[] writeTables) {
		if (writeTables != null)
			for (String s : writeTables)
				lockTbl.xLock(s, txNum);

		if (readTables != null)
			for (String s : readTables)
				lockTbl.sLock(s, txNum);
	}

	public void prepareSp(RecordKey[] readKeys, RecordKey[] writeKeys) {
		// If a transaction requests to take x lock on a object,
		// it should not take s lock on the same object.
		Set<RecordKey> writeSet = new HashSet<RecordKey>();
		for (RecordKey wk : writeKeys)
			writeSet.add(wk);
		
		if (readKeys != null)
			for (RecordKey rt : readKeys)
				if (!writeSet.contains(rt))
					lockTbl.requestLock(rt, txNum);

		if (writeKeys != null)
			for (RecordKey wt : writeKeys)
				lockTbl.requestLock(wt, txNum);
	}

	public void executeSp(RecordKey[] readKeys, RecordKey[] writeKeys) {
		/*
		 * TODO: should take intension lock on tables? If the structure of
		 * record file may change, the ix lock on table level is needed.
		 */
		Set<RecordKey> writeSet = new HashSet<RecordKey>();
		for (RecordKey wk : writeKeys)
			writeSet.add(wk);
		
		if (readKeys != null)
			for (RecordKey rt : readKeys) 
				if (!writeSet.contains(rt))	{
					// lockTbl.isLock(k.getTableName(), txNum);
					lockTbl.sLock(rt, txNum);
				}
		
		if (writeKeys != null)
			for (RecordKey k : writeKeys) {
				// lockTbl.ixLock(k.getTableName(), txNum);
				lockTbl.xLock(k, txNum);
			}
	}

	public void finishSp(RecordKey[] readKeys, RecordKey[] writeKeys) {
		if (writeKeys != null)
			for (RecordKey k : writeKeys) {
				// TODO: release table ixlock
				lockTbl.release(k, txNum,
						ConservativeOrderedLockTable.LockType.X_LOCK);
			}

		if (readKeys != null)
			for (RecordKey k : readKeys) {
				// TODO: release table islock
				lockTbl.release(k, txNum,
						ConservativeOrderedLockTable.LockType.S_LOCK);
			}
	}

	public void onTxCommit(Transaction tx) {
		lockTbl.releaseAll(txNum);
	}

	public void onTxRollback(Transaction tx) {
		lockTbl.releaseAll(txNum);
	}

	public void onTxEndStatement(Transaction tx) {
		// do nothing
	}

	public void prepareWriteBack(RecordKey... keys) {
		lockTbl.requestWriteBackLocks(keys, txNum);
	}

	public void executeWriteBack(RecordKey... keys) {
		if (keys != null)
			for (RecordKey k : keys)
				lockTbl.wbLock(k, txNum);
	}

	public void releaseWriteBackLock(RecordKey... keys) {
		if (keys != null)
			for (RecordKey k : keys)
				lockTbl.release(k, txNum,
						ConservativeOrderedLockTable.LockType.WRITE_BACK_LOCK);
	}

	@Override
	public void modifyFile(String fileName) {
		// do nothing
	}

	@Override
	public void readFile(String fileName) {
		// do nothing
	}

	@Override
	public void modifyBlock(BlockId blk) {
		// do nothing
	}

	@Override
	public void readBlock(BlockId blk) {
		// do nothing
	}

	@Override
	public void insertBlock(BlockId blk) {
		// do nothing
	}

	@Override
	public void modifyIndex(String dataFileName) {
		// lockTbl.ixLock(dataFileName, txNum);
	}

	@Override
	public void readIndex(String dataFileName) {
		// lockTbl.isLock(dataFileName, txNum);
	}

	/*
	 * Methods for B-Tree index locking
	 */
	private Set<BlockId> readIndexBlks = new HashSet<BlockId>();
	private Set<BlockId> writtenIndexBlks = new HashSet<BlockId>();

	/**
	 * Sets lock on the leaf block for update.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void modifyLeafBlock(BlockId blk) {
		lockTbl.xLock(blk, txNum);
		writtenIndexBlks.add(blk);
	}

	/**
	 * Sets lock on the leaf block for read.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void readLeafBlock(BlockId blk) {
		lockTbl.sLock(blk, txNum);
		readIndexBlks.add(blk);
	}

	/**
	 * Sets exclusive lock on the directory block when crabbing down for
	 * modification.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabDownDirBlockForModification(BlockId blk) {
		lockTbl.xLock(blk, txNum);
		writtenIndexBlks.add(blk);
	}

	/**
	 * Sets shared lock on the directory block when crabbing down for read.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabDownDirBlockForRead(BlockId blk) {
		lockTbl.sLock(blk, txNum);
		readIndexBlks.add(blk);
	}

	/**
	 * Releases exclusive locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForModification(BlockId blk) {
		lockTbl.release(blk, txNum,
				ConservativeOrderedLockTable.LockType.X_LOCK);
		writtenIndexBlks.remove(blk);
	}

	/**
	 * Releases shared locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForRead(BlockId blk) {
		lockTbl.release(blk, txNum,
				ConservativeOrderedLockTable.LockType.S_LOCK);
		readIndexBlks.remove(blk);
	}

	public void releaseIndexLocks() {
		for (BlockId blk : readIndexBlks)
			lockTbl.release(blk, txNum,
					ConservativeOrderedLockTable.LockType.S_LOCK);
		for (BlockId blk : writtenIndexBlks)
			lockTbl.release(blk, txNum,
					ConservativeOrderedLockTable.LockType.X_LOCK);
		readIndexBlks.clear();
		writtenIndexBlks.clear();
	}

	public void lockRecordFileHeader(BlockId blk) {
		lockTbl.xLock(blk, txNum);
	}

	public void releaseRecordFileHeader(BlockId blk) {
		lockTbl.release(blk, txNum,
				ConservativeOrderedLockTable.LockType.X_LOCK);
	}

	@Override
	public void modifyRecord(RecordId recId) {
		// do nothing

	}

	@Override
	public void readRecord(RecordId recId) {
		// do nothing

	}
}

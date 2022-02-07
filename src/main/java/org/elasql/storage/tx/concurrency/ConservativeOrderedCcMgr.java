/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.storage.tx.concurrency;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.elasql.sql.PrimaryKey;
import org.elasql.storage.tx.concurrency.ConservativeOrderedLockTable.LockType;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;

public class ConservativeOrderedCcMgr extends ConcurrencyMgr {
	protected static ConservativeOrderedLockTable lockTbl = new ConservativeOrderedLockTable();
	
	// For normal operations - using conservative locking 
	private Set<Object> bookedObjs, readObjs, writeObjs;

	// For Indexes - using crabbing locking
	private Set<BlockId> readIndexBlks = new HashSet<BlockId>();
	private Set<BlockId> writtenIndexBlks = new HashSet<BlockId>();

	public ConservativeOrderedCcMgr(long txNumber) {
		txNum = txNumber;
		bookedObjs = new HashSet<Object>();
		readObjs = new HashSet<Object>();
		writeObjs = new HashSet<Object>();
	}
	
	public void bookReadKey(PrimaryKey key) {
		if (key != null) {
			// The key needs to be booked only once. 
			if (!bookedObjs.contains(key))
				lockTbl.requestLock(key, txNum);
			
			bookedObjs.add(key);
			readObjs.add(key);
		}
	}

	/**
	 * Book the read lock of the specified objects.
	 * 
	 * @param keys
	 *            the objects which the transaction intends to read
	 */
	public void bookReadKeys(Collection<PrimaryKey> keys) {
		if (keys != null) {
			for (PrimaryKey key : keys) {
				// The key needs to be booked only once. 
				if (!bookedObjs.contains(key))
					lockTbl.requestLock(key, txNum);
			}
			
			bookedObjs.addAll(keys);
			readObjs.addAll(keys);
		}
	}
	
	public void bookWriteKey(PrimaryKey key) {
		if (key != null) {
			// The key needs to be booked only once. 
			if (!bookedObjs.contains(key))
				lockTbl.requestLock(key, txNum);
			
			bookedObjs.add(key);
			writeObjs.add(key);
		}
	}
	
	/**
	 * Book the write lock of the specified object.
	 * 
	 * @param keys
	 *             the objects which the transaction intends to write
	 */
	public void bookWriteKeys(Collection<PrimaryKey> keys) {
		if (keys != null) {
			for (PrimaryKey key : keys) {
				// The key needs to be booked only once. 
				if (!bookedObjs.contains(key))
					lockTbl.requestLock(key, txNum);
			}
			
			bookedObjs.addAll(keys);
			writeObjs.addAll(keys);
		}
	}
	
	/**
	 * Request (get the locks immediately) the locks which the transaction
	 * has booked. If the locks can not be obtained in the time, it will
	 * make the thread wait until it can obtain all locks it requests.
	 */
	public void requestLocks() {
		bookedObjs.clear();
		
		for (Object obj : writeObjs)
			lockTbl.xLock(obj, txNum);
		
		for (Object obj : readObjs)
			if (!writeObjs.contains(obj))
				lockTbl.sLock(obj, txNum);
	}
	
	@Override
	public void onTxCommit(Transaction tx) {
		releaseIndexLocks();
		releaseLocks();
	}
	
	@Override
	public void onTxRollback(Transaction tx) {
		releaseIndexLocks();
		releaseLocks();
	}

	@Override
	public void onTxEndStatement(Transaction tx) {
		// Next-key lock algorithm is non-deterministic. It may
		// cause deadlocks during the execution. Therefore,
		// we release the locks earlier to prevent deadlocks.
		// However, phantoms due to update may happen.
		// TODO: We need a deterministic algorithm to handle this.
		releaseIndexLocks();
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

	/**
	 * Sets lock on the leaf block for update.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void modifyLeafBlock(BlockId blk) {
		lockTbl.xLockForBlock(blk, txNum);
		writtenIndexBlks.add(blk);
	}

	/**
	 * Sets lock on the leaf block for read.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void readLeafBlock(BlockId blk) {
		lockTbl.sLockForBlock(blk, txNum);
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
		lockTbl.xLockForBlock(blk, txNum);
		writtenIndexBlks.add(blk);
	}

	/**
	 * Sets shared lock on the directory block when crabbing down for read.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabDownDirBlockForRead(BlockId blk) {
		lockTbl.sLockForBlock(blk, txNum);
		readIndexBlks.add(blk);
	}

	/**
	 * Releases exclusive locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForModification(BlockId blk) {
		lockTbl.releaseForBlock(blk, txNum, ConservativeOrderedLockTable.LockType.X_LOCK);
		writtenIndexBlks.remove(blk);
	}

	/**
	 * Releases shared locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForRead(BlockId blk) {
		lockTbl.releaseForBlock(blk, txNum, ConservativeOrderedLockTable.LockType.S_LOCK);
		readIndexBlks.remove(blk);
	}

	public void releaseIndexLocks() {
		for (BlockId blk : readIndexBlks)
			lockTbl.releaseForBlock(blk, txNum, ConservativeOrderedLockTable.LockType.S_LOCK);
		for (BlockId blk : writtenIndexBlks)
			lockTbl.releaseForBlock(blk, txNum, ConservativeOrderedLockTable.LockType.X_LOCK);
		readIndexBlks.clear();
		writtenIndexBlks.clear();
	}

	public void lockRecordFileHeader(BlockId blk) {
		lockTbl.xLockForBlock(blk, txNum);
	}

	public void releaseRecordFileHeader(BlockId blk) {
		lockTbl.releaseForBlock(blk, txNum, ConservativeOrderedLockTable.LockType.X_LOCK);
	}

	@Override
	public void modifyRecord(RecordId recId) {
		// do nothing
	}

	@Override
	public void readRecord(RecordId recId) {
		// do nothing
	}
	
	private void releaseLocks() {
		for (Object obj : writeObjs)
			lockTbl.release(obj, txNum, LockType.X_LOCK);
		
		for (Object obj : readObjs)
			if (!writeObjs.contains(obj))
				lockTbl.release(obj, txNum, LockType.S_LOCK);
		
		readObjs.clear();
		writeObjs.clear();
	}
}

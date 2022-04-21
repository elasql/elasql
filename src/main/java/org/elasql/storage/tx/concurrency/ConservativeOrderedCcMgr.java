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
import java.util.concurrent.locks.ReentrantLock;

import org.elasql.sql.PrimaryKey;
import org.elasql.storage.tx.concurrency.RandomizedLockTable.LockType;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;

public class ConservativeOrderedCcMgr extends ConcurrencyMgr {
	protected static RandomizedLockTable randomizedLockTbl = new RandomizedLockTable();

	// For normal operations - using conservative locking
	private Set<Object> bookedObjs, readObjs, writeObjs;
	private FifoLock myFifoLock;

	// For Indexes - using crabbing locking
	private Set<BlockId> readIndexBlks = new HashSet<BlockId>();
	private Set<BlockId> writtenIndexBlks = new HashSet<BlockId>();

	public ConservativeOrderedCcMgr(long txNumber) {
		txNum = txNumber;
		bookedObjs = new HashSet<Object>();
		readObjs = new HashSet<Object>();
		writeObjs = new HashSet<Object>();
		myFifoLock = new FifoLock(txNum);
	}

	public void bookReadKey(PrimaryKey key) {
		if (key != null) { 
			// The key needs to be booked only once.  
			if (!bookedObjs.contains(key)) 
				randomizedLockTbl.requestLock(key, txNum); 
			 
			bookedObjs.add(key); 
			readObjs.add(key); 
		} 
	}

	/**
	 * Book the read lock of the specified objects.
	 * 
	 * @param keys the objects which the transaction intends to read
	 */
	public void bookReadKeys(Collection<PrimaryKey> keys) {
		if (keys != null) { 
			for (PrimaryKey key : keys) { 
				// The key needs to be booked only once.  
				if (!bookedObjs.contains(key)) 
					randomizedLockTbl.requestLock(key, txNum); 
			} 
			 
			bookedObjs.addAll(keys); 
			readObjs.addAll(keys); 
		} 
	}

	public void bookWriteKey(PrimaryKey key) {
		if (key != null) { 
			// The key needs to be booked only once.  
			if (!bookedObjs.contains(key)) 
				randomizedLockTbl.requestLock(key, txNum); 
			 
			bookedObjs.add(key); 
			writeObjs.add(key); 
		} 
	}

	/**
	 * Book the write lock of the specified object.
	 * 
	 * @param keys the objects which the transaction intends to write
	 */
	public void bookWriteKeys(Collection<PrimaryKey> keys) {
		if (keys != null) { 
			for (PrimaryKey key : keys) { 
				// The key needs to be booked only once.  
				if (!bookedObjs.contains(key)) 
					randomizedLockTbl.requestLock(key, txNum); 
			} 
			 
			bookedObjs.addAll(keys); 
			writeObjs.addAll(keys); 
		} 
	}

//	private void bookKeyIfAbsent(PrimaryKey key) {
//		// The key needs to be booked only once.
//		if (!bookedObjs.contains(key)) {
//			fifoLockTbl.requestLock(key, myFifoLock);
//			bookedObjs.add(key);
//		}
//	}

	/**
	 * Request (get the locks immediately) the locks which the transaction has
	 * booked. If the locks can not be obtained in the time, it will make the thread
	 * wait until it can obtain all locks it requests.
	 */
	public void requestLocks() { 
		bookedObjs.clear(); 
		 
		for (Object obj : writeObjs) 
			randomizedLockTbl.xLock(obj, txNum); 
		 
		for (Object obj : readObjs) 
			if (!writeObjs.contains(obj)) 
				randomizedLockTbl.sLock(obj, txNum); 
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
	 * @param blk the block id
	 */
	public void modifyLeafBlock(BlockId blk) {
		randomizedLockTbl.xLockForBlock(blk, txNum);
		writtenIndexBlks.add(blk);
	}

	/**
	 * Sets lock on the leaf block for read.
	 * 
	 * @param blk the block id
	 */
	public void readLeafBlock(BlockId blk) {
		randomizedLockTbl.sLockForBlock(blk, txNum);
		readIndexBlks.add(blk);
	}

	/**
	 * Sets exclusive lock on the directory block when crabbing down for
	 * modification.
	 * 
	 * @param blk the block id
	 */
	public void crabDownDirBlockForModification(BlockId blk) {
		randomizedLockTbl.xLockForBlock(blk, txNum);
		writtenIndexBlks.add(blk);
	}

	/**
	 * Sets shared lock on the directory block when crabbing down for read.
	 * 
	 * @param blk the block id
	 */
	public void crabDownDirBlockForRead(BlockId blk) {
		randomizedLockTbl.sLockForBlock(blk, txNum);
		readIndexBlks.add(blk);
	}

	/**
	 * Releases exclusive locks on the directory block for crabbing back.
	 * 
	 * @param blk the block id
	 */
	public void crabBackDirBlockForModification(BlockId blk) {
		randomizedLockTbl.releaseForBlock(blk, txNum, RandomizedLockTable.LockType.X_LOCK);
		writtenIndexBlks.remove(blk);
	}

	/**
	 * Releases shared locks on the directory block for crabbing back.
	 * 
	 * @param blk the block id
	 */
	public void crabBackDirBlockForRead(BlockId blk) {
		randomizedLockTbl.releaseForBlock(blk, txNum, RandomizedLockTable.LockType.S_LOCK);
		readIndexBlks.remove(blk);
	}

	public void releaseIndexLocks() {
		for (BlockId blk : readIndexBlks)
			randomizedLockTbl.releaseForBlock(blk, txNum, RandomizedLockTable.LockType.S_LOCK);
		for (BlockId blk : writtenIndexBlks)
			randomizedLockTbl.releaseForBlock(blk, txNum, RandomizedLockTable.LockType.X_LOCK);
		readIndexBlks.clear();
		writtenIndexBlks.clear();
	}

	@Override
	public void lockRecordFileHeader(BlockId blk) {
		randomizedLockTbl.xLockForBlock(blk, txNum);
	}

	/**
	 * Optimization on 2022/2/22 There are two attempts to a critical section if we
	 * use lockRecordFileHeader() to lock a record file header, and use
	 * releaseRecordFileHeader to release a record file header. An optimization here
	 * is to make locking a file header a single critical section.
	 */
	@Override
	public ReentrantLock getLockForFileHeader(BlockId blk) {
		return randomizedLockTbl.getFhpLatch(blk);
	}

	public void releaseRecordFileHeader(BlockId blk) {
		randomizedLockTbl.releaseForBlock(blk, txNum, RandomizedLockTable.LockType.X_LOCK);
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
			randomizedLockTbl.release(obj, txNum, LockType.X_LOCK); 
		 
		for (Object obj : readObjs) 
			if (!writeObjs.contains(obj)) 
				randomizedLockTbl.release(obj, txNum, LockType.S_LOCK); 
		 
		readObjs.clear(); 
		writeObjs.clear(); 
	} 
}

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
package org.elasql.cache.calvin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The class that deal with remote records for parent transaction.
 */
public class CalvinCacheMgr {
	
	private static class KeyRecordPair {
		RecordKey key;
		CachedRecord record;
		
		KeyRecordPair(RecordKey key, CachedRecord record) {
			this.key = key;
			this.record = record;
		}
	}
	
	// For single thread
	private Transaction tx;
	private Map<RecordKey, CachedRecord> cachedRecords;
	private Set<RecordKey> writeKeys;
	
	// For multi-threading
	private BlockingQueue<KeyRecordPair> inbox;

	CalvinCacheMgr(CalvinPostOffice postOffice, Transaction tx) {
		this.tx = tx;
		this.cachedRecords = new HashMap<RecordKey, CachedRecord>();
		this.writeKeys = new HashSet<RecordKey>();
	}
	
	/**
	 * Prepare for receiving the records from remote nodes. This must be called before starting
	 * receiving those records.
	 */
	void createInboxForRemotes() {
		inbox = new LinkedBlockingQueue<KeyRecordPair>();
	}
	
	/**
	 * Tell the post office that this transaction has done with the remote records. It will not
	 * be able to receive remote records after calling this. This will make the post office clean
	 * the remote cache for this transaction.
	 */
	public void notifyTxCommitted() {
		CalvinPostOffice postOffice = (CalvinPostOffice) Elasql.remoteRecReceiver();
		
		// Notify the post office the transaction has committed
		postOffice.notifyTxCommitted(tx.getTransactionNumber());
	}
	
	public CachedRecord readFromLocal(RecordKey key) {
		CachedRecord rec = cachedRecords.get(key);
		if (rec != null)
			return rec;
		
		rec = VanillaCoreCrud.read(key, tx);
		if (rec != null) {
			rec.setSrcTxNum(tx.getTransactionNumber());
			cachedRecords.put(key, rec);
		}
		
		return rec;
	}
	
	public CachedRecord readFromRemote(RecordKey key) {
		CachedRecord rec = cachedRecords.get(key);
		if (rec != null)
			return rec;
		
		if (inbox == null)
			throw new RuntimeException("tx." + tx.getTransactionNumber() + " needs to"
					+ " call prepareForRemotes() before receiving remote records.");
		
		try {
			String name = Thread.currentThread().getName();
			Thread.currentThread().setName(name + " waits for " + key + " from remote.");
			
			// Wait for remote records
			KeyRecordPair pair = inbox.take();
			while (!pair.key.equals(key)) {
				cachedRecords.put(pair.key, pair.record);
				pair = inbox.take();
			}
			rec = pair.record;
			
			Thread.currentThread().setName(name);
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return rec;
	}

	public void update(RecordKey key, CachedRecord rec) {
		rec.setSrcTxNum(tx.getTransactionNumber());
		cachedRecords.put(key, rec);
		writeKeys.add(key);
	}

	public void insert(RecordKey key, Map<String, Constant> fldVals) {
		CachedRecord rec = new CachedRecord(fldVals);
		rec.setSrcTxNum(tx.getTransactionNumber());
		rec.setNewInserted(true);
		cachedRecords.put(key, rec);
		writeKeys.add(key);
	}

	public void delete(RecordKey key) {
		CachedRecord dummyRec = new CachedRecord();
		dummyRec.setSrcTxNum(tx.getTransactionNumber());
		dummyRec.delete();
		cachedRecords.put(key, dummyRec);
		writeKeys.add(key);
	}
	
//	public static void setMigrated(RecordKey key, Transaction tx, List<IndexInfo> allIndexes) {
//		VanillaCoreCrud.setMigrated(key, tx, allIndexes);
//	}
//	
//	public static boolean isMigrated(RecordKey key, Transaction tx, IndexInfo ii) {
//		return VanillaCoreCrud.isMigrated(key, tx, ii);
//	}
	
	public void flush() {
		for (RecordKey key : writeKeys) {
			CachedRecord rec = cachedRecords.get(key);
			
			if (rec.isDeleted())
				VanillaCoreCrud.delete(key, tx);
			else if (rec.isNewInserted())
				VanillaCoreCrud.insert(key, rec, tx);
			else if (rec.isDirty())
				VanillaCoreCrud.update(key, rec, tx);
		}
		
		writeKeys.clear();
	}
	
	public void clearCachedRecords() {
		cachedRecords.clear();
	}
	
	void receiveRemoteRecord(RecordKey key, CachedRecord rec) {
		if (inbox == null)
			throw new RuntimeException("No inbox for " + key + " on Tx." + tx.getTransactionNumber());
		inbox.add(new KeyRecordPair(key, rec));
	}
}

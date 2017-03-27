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
package org.elasql.cache.calvin;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.TransactionLifecycleListener;

/**
 * The class that deal with remote records for parent transaction.
 */
public class CalvinCacheMgr implements TransactionLifecycleListener {
	
	private static class KeyRecordPair {
		RecordKey key;
		CachedRecord record;
		
		KeyRecordPair(RecordKey key, CachedRecord record) {
			this.key = key;
			this.record = record;
		}
	}
	
	// For single thread
	private CalvinRemotePostOffice postOffice;
	private Transaction tx;
	private Map<RecordKey, CachedRecord> cachedRecords;
	
	// For multi-threading
	private BlockingQueue<KeyRecordPair> inbox;

	public CalvinCacheMgr(Transaction tx) {
		this.tx = tx;
		this.postOffice = (CalvinRemotePostOffice) Elasql.remoteRecReceiver();
		this.cachedRecords = new HashMap<RecordKey, CachedRecord>();
		this.inbox = new LinkedBlockingQueue<KeyRecordPair>();
		
		// Register this CacheMgr
		this.postOffice.registerCacheMgr(tx.getTransactionNumber(), this);
	}

	@Override
	public void onTxCommit(Transaction tx) {
		postOffice.unregisterCacheMgr(tx.getTransactionNumber());
	}

	@Override
	public void onTxRollback(Transaction tx) {
		postOffice.unregisterCacheMgr(tx.getTransactionNumber());
	}

	@Override
	public void onTxEndStatement(Transaction tx) {
		// Do nothing
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
		
		try {
			KeyRecordPair pair = inbox.take();
			while (!pair.key.equals(key)) {
				cachedRecords.put(pair.key, pair.record);
				pair = inbox.take();
			}
			rec = pair.record;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return rec;
	}

	public void update(RecordKey key, CachedRecord rec) {
		rec.setSrcTxNum(tx.getTransactionNumber());
		cachedRecords.put(key, rec);
	}

	public void insert(RecordKey key, Map<String, Constant> fldVals) {
		CachedRecord rec = new CachedRecord(fldVals);
		rec.setSrcTxNum(tx.getTransactionNumber());
		rec.setNewInserted(true);
		cachedRecords.put(key, rec);
	}

	public void delete(RecordKey key) {
		CachedRecord dummyRec = new CachedRecord();
		dummyRec.setSrcTxNum(tx.getTransactionNumber());
		dummyRec.delete();
		cachedRecords.put(key, dummyRec);
	}
	
	public void flush() {
		for (Map.Entry<RecordKey, CachedRecord> entry : cachedRecords.entrySet()) {
			RecordKey key = entry.getKey();
			CachedRecord rec = entry.getValue();
			
			if (key.getPartition() == Elasql.serverId()) {
				if (rec.isDeleted())
					VanillaCoreCrud.delete(key, tx);
				else if (rec.isNewInserted())
					VanillaCoreCrud.insert(key, rec, tx);
				else if (rec.isDirty())
					VanillaCoreCrud.update(key, rec, tx);
			}
		}
	}
	
	void receiveRemoteRecord(RecordKey key, CachedRecord rec) {
		inbox.add(new KeyRecordPair(key, rec));
	}
}

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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.LocalRecordMgr;
import org.elasql.sql.RecordKey;
import org.elasql.sql.RecordVersion;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;

public class BasicCacheMgr {
	private static final int INITIAL_CACHE_CAPACITY = 256;
	
	// <RecordKey, srcTxNum> -> Record Value
	private Map<RecordVersion, CachedRecord> cacheRecordMap;

	public BasicCacheMgr() {
		this.cacheRecordMap = new ConcurrentHashMap<RecordVersion, CachedRecord>(
				INITIAL_CACHE_CAPACITY);
		
		// Debug
//		new Thread() {
//			@Override
//			public void run() {
//				long startTime = System.currentTimeMillis();
//				long lastRecordTime = 0;
//				long elapsedTime = System.currentTimeMillis() - startTime;
//				long totalTime = 30000;
//				long recordInterval = 1000; // in millisecond
//				
//				while (elapsedTime < totalTime) {
//					// Record tx counts
//					if (elapsedTime - lastRecordTime >= recordInterval) {
//						lastRecordTime = elapsedTime;
//						System.out.println("Map Size: " + cacheRecordMap.size());
//					}
//					
//					// Sleep for a short time (avoid busy waiting)
//					try {
//						Thread.sleep(100);
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//					
//					// Update elapsed time
//					elapsedTime = System.currentTimeMillis() - startTime;
//				}
//				
//				// Check first 10 records
//				int i = 0;
//				for (RecordVersion rv : cacheRecordMap.keySet()) {
//					System.out.println(rv);
//					
//					i++;
//					if (i > 10)
//						break;
//				}
//				
//			}
//		}.start();
	}

	public void cacheRecord(RecordKey key, CachedRecord rec) {
		RecordVersion rv = new RecordVersion(key, rec.getSrcTxNum());
		cacheRecordMap.put(rv, rec);
	}

	public boolean flushToLocalStorage(RecordKey key, long srcTxNum,
			Transaction tx) {
		RecordVersion rv = new RecordVersion(key, srcTxNum);
		CachedRecord rec;
		rec = cacheRecordMap.get(rv);
		if (rec == null)
			return false;
		else
			flush(key, rec, tx);
		return true;
	}

	private void flush(RecordKey key, CachedRecord rec, Transaction tx) {
		if (rec.isDeleted())
			LocalRecordMgr.delete(key, tx);
		else if (rec.isNewInserted())
			LocalRecordMgr.insert(key, rec, tx);
		else if (rec.isDirty())
			LocalRecordMgr.update(key, rec, tx);
	}

	public void remove(RecordKey key, long srcTxNum) {
		cacheRecordMap.remove(new RecordVersion(key, srcTxNum));

	}

	public CachedRecord readCache(RecordKey key, long srcTxNum) {
		RecordVersion rv = new RecordVersion(key, srcTxNum);
		return cacheRecordMap.get(rv);
	}

	public CachedRecord read(RecordKey key, long srcTxNum, Transaction tx,
			boolean isReadFromSink) {
		RecordVersion rv = new RecordVersion(key, srcTxNum);
		CachedRecord rec;
		rec = cacheRecordMap.get(rv);

		// return if the the cache has this record
		if (rec != null || !isReadFromSink)
			return rec;

		rec = LocalRecordMgr.read(key, tx);

		if (rec != null) {
			rv.srcTxNum = tx.getTransactionNumber();
			rec.setSrcTxNum(tx.getTransactionNumber());
			cacheRecordMap.put(rv, rec);
		}
		return rec;
	}
	
	public void update(RecordKey key, CachedRecord rec,	Transaction tx) {
		RecordVersion rv = new RecordVersion(key, tx.getTransactionNumber());
		rec.setSrcTxNum(tx.getTransactionNumber());
		cacheRecordMap.put(rv, rec);
	}

	public void insert(RecordKey key, Map<String, Constant> fldVals,
			Transaction tx) {
		RecordVersion rv = new RecordVersion(key, tx.getTransactionNumber());
		CachedRecord rec = new CachedRecord(fldVals);
		rec.setSrcTxNum(tx.getTransactionNumber());
		rec.setNewInserted(true);
		cacheRecordMap.put(rv, rec);
	}

	public void delete(RecordKey key, Transaction tx) {
		RecordVersion rv = new RecordVersion(key, tx.getTransactionNumber());
		CachedRecord dummyRec = new CachedRecord();
		dummyRec.setSrcTxNum(tx.getTransactionNumber());
		dummyRec.delete();
		cacheRecordMap.put(rv, dummyRec);
	}
}

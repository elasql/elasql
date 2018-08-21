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
package org.elasql.cache.tpart;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.RemoteRecordReceiver;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.remote.groupcomm.Tuple;
import org.elasql.sql.RecordKey;
import org.elasql.storage.tx.concurrency.tpart.LocalStorageCcMgr;
import org.vanilladb.core.storage.tx.Transaction;

public class TPartCacheMgr implements RemoteRecordReceiver {

	/**
	 * Looks up the sink id for the specified partition.
	 * 
	 * @param partId
	 *            the id of the specified partition
	 * @return the sink id
	 */
	public static long toSinkId(int partId) {
		return (partId + 1) * -1;
	}

	private static LocalStorageCcMgr localCcMgr = new LocalStorageCcMgr();

	private Map<CachedEntryKey, CachedRecord> exchange = new ConcurrentHashMap<CachedEntryKey, CachedRecord>();

	private final Object anchors[] = new Object[1009];

	public TPartCacheMgr() {
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
	}

	private Object prepareAnchor(Object o) {
		int hash = o.hashCode() % anchors.length;
		if (hash < 0) {
			hash += anchors.length;
		}
		return anchors[0];
	}

	public CachedRecord takeFromTx(RecordKey key, long src, long dest) {
		CachedEntryKey k = new CachedEntryKey(key, src, dest);
		synchronized (prepareAnchor(k)) {
			try {
				// wait if the record has not delivered
				while (!exchange.containsKey(k)) {
					prepareAnchor(k).wait();
				}
				return exchange.remove(k);
			} catch (InterruptedException e) {
				throw new RuntimeException();
			}
		}
	}

	public void passToTheNextTx(RecordKey key, CachedRecord rec, long src, long dest, boolean isRemote) {
		CachedEntryKey k = new CachedEntryKey(key, src, dest);
		synchronized (prepareAnchor(k)) {
			exchange.put(k, rec);
			prepareAnchor(k).notifyAll();
		}
	}

	@Override
	public void cacheRemoteRecord(Tuple t) {
		passToTheNextTx(t.key, t.rec, t.srcTxNum, t.destTxNum, true);
	}
	
	public CachedRecord readFromSink(RecordKey key, Transaction tx) {
		localCcMgr.beforeSinkRead(key, tx.getTransactionNumber());
		CachedRecord rec = VanillaCoreCrud.read(key, tx);
		localCcMgr.afterSinkRead(key, tx.getTransactionNumber());
		return rec;
	}
	
	public void writeBack(RecordKey key, CachedRecord rec, Transaction tx) {
		localCcMgr.beforeWriteBack(key, tx.getTransactionNumber());
		writeToVanillaCore(key, rec, tx);
		localCcMgr.afterWriteback(key, tx.getTransactionNumber());
	}
	
	public void registerSinkReading(RecordKey key, long txNum) {
		localCcMgr.requestSinkRead(key, txNum);
	}

	public void registerSinkWriteback(RecordKey key, long txNum) {
		localCcMgr.requestWriteBack(key, txNum);
	}
	
	private void writeToVanillaCore(RecordKey key, CachedRecord rec, Transaction tx) {
		if (rec.isDeleted())
			VanillaCoreCrud.delete(key, tx);
		else if (rec.isNewInserted())
			VanillaCoreCrud.insert(key, rec, tx);
		else if (rec.isDirty())
			VanillaCoreCrud.update(key, rec, tx);
	}
}

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

import org.elasql.cache.CacheMgr;
import org.elasql.cache.CachedRecord;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The class that deal with remote wait
 * 
 */

// TODO remove unnecessary code
public class CalvinCacheMgr implements CacheMgr {
	// private static long MAX_TIME = 30000;
	private BasicCacheMgr cacheMgr;

	private final Object[] anchors = new Object[1009];

	public CalvinCacheMgr() {
		cacheMgr = new BasicCacheMgr();
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
	}

	private Object prepareAnchor(Object o) {
		int hash = o.hashCode() % anchors.length;
		if (hash < 0) {
			hash += anchors.length;
		}
		return anchors[hash];
	}

	public void cacheRemoteRecord(RecordKey key, CachedRecord rec) {
		// System.out.println("cache " + key + " src:" + rec.getSrcTxNum());
		synchronized (prepareAnchor(key)) {
			cacheMgr.cacheRecord(key, rec);
			prepareAnchor(key).notifyAll();
		}
	}

	public void flushToLocalStorage(RecordKey key, long srcTxNum, Transaction tx) {
		cacheMgr.flushToLocalStorage(key, srcTxNum, tx);
	}

	public void remove(RecordKey key, long srcTxNum) {
		cacheMgr.remove(key, srcTxNum);
	}

	public CachedRecord read(RecordKey key, long srcTxNum, Transaction tx,
			boolean srcIsLocal) {
		CachedRecord rec;

		if (!srcIsLocal) { // not received the remote record yet
			try {
				synchronized (prepareAnchor(key)) {
					rec = cacheMgr.read(key, srcTxNum, tx, srcIsLocal);
					while (rec == null) {
						prepareAnchor(key).wait();
						rec = cacheMgr.read(key, srcTxNum, tx, srcIsLocal);
					}

				}
			} catch (InterruptedException e) {
				throw new RuntimeException();
			}
		} else {
			rec = cacheMgr.read(key, srcTxNum, tx, srcIsLocal);
		}

		return rec;
	}

	/*
	 * TODO: Deprecated public void update(RecordKey key, Map<String, Constant>
	 * fldVals, Transaction tx) { // need to update the whole flds of a record
	 * cacheMgr.update(key, fldVals, tx); }
	 */

	public void update(RecordKey key, CachedRecord rec, Transaction tx) {
		// need to update the whole flds of a record
		cacheMgr.update(key, rec, tx);
	}

	public void insert(RecordKey key, Map<String, Constant> fldVals,
			Transaction tx) {
		cacheMgr.insert(key, fldVals, tx);
	}

	public void delete(RecordKey key, Transaction tx) {
		cacheMgr.delete(key, tx);
	}
}

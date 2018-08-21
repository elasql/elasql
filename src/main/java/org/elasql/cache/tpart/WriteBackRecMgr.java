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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * This represents the local sink on this machine. All data accesses to the
 * local storage should pass through this interface. It also caches the records
 * written by the transactions of a T-Graph; then, pass the records to the ones
 * need them in the next T-Graph.
 */
public class WriteBackRecMgr {
	/*
	 * The write back tuples should be inserted followed the sink process id in
	 * decs. order.
	 */
	private Map<RecordKey, List<WriteBackTuple>> writeBackRecMap = new ConcurrentHashMap<RecordKey, List<WriteBackTuple>>();

	private final Object[] anchors = new Object[1009];

	public WriteBackRecMgr() {
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

	class WriteBackTuple {
		final int sinkProcessId;
		boolean hasValue;
		CachedRecord rec;

		WriteBackTuple(int sinkProcessId) {
			this.sinkProcessId = sinkProcessId;
		}
	}

	public void setWriteBackInfo(RecordKey key, int sinkProcessId) {
		synchronized (prepareAnchor(key)) {
			List<WriteBackTuple> tuples = writeBackRecMap.get(key);

			if (tuples == null) {
				tuples = new LinkedList<WriteBackTuple>();
				writeBackRecMap.put(key, tuples);
			}
			// insert the new tuple into the head of the list
			tuples.add(0, new WriteBackTuple(sinkProcessId));
		}
	}

	public CachedRecord read(RecordKey key, int mySinkProcessId, Transaction tx) {
		// read from write back cache first
		CachedRecord rec = getCachedRecord(key, mySinkProcessId);

		// if there is no write back cache, read from local storage
		if (rec == null)
			rec = VanillaCoreCrud.read(key, tx);

		return rec;
	}

	/**
	 * Read record key from previous writeback cached map.
	 * 
	 * @param key
	 * @param mySinkProcessId
	 * @return The desired cached record. Return null if no previous record key
	 *         found.
	 */
	public CachedRecord getCachedRecord(RecordKey key, int mySinkProcessId) {
		synchronized (prepareAnchor(key)) {
			List<WriteBackTuple> tuples = writeBackRecMap.get(key);
			if (tuples == null)
				return null;

			// read the newest created record
			Iterator<WriteBackTuple> iter = tuples.iterator();
			while (iter.hasNext()) {
				WriteBackTuple wbt = iter.next();
				if (wbt.sinkProcessId < mySinkProcessId) {
					while (!wbt.hasValue) {
						try {
							prepareAnchor(key).wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

					}
					return wbt.rec;
				}
			}
		}
		return null;
	}

	public void writeBackRecord(RecordKey key, int sinkProcessId, CachedRecord rec, Transaction tx) {

		synchronized (prepareAnchor(key)) {
			List<WriteBackTuple> tuples = writeBackRecMap.get(key);
			ListIterator<WriteBackTuple> tuplesItr = tuples.listIterator(tuples.size());
			while (tuplesItr.hasPrevious()) {
				WriteBackTuple wbt = tuplesItr.previous();
				// if wbt is in previous sink, make sure the write back is done
				if (wbt.sinkProcessId < sinkProcessId) {
					while (wbt.hasValue == false)
						try {
							prepareAnchor(key).wait();
							tuplesItr = tuples.listIterator(tuples.size());
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
				}
				// if wbt is in this sink size, set the has value = true
				else if (wbt.sinkProcessId == sinkProcessId) {
					wbt.hasValue = true;
					wbt.rec = rec;
					break;
				}
			}
			prepareAnchor(key).notifyAll();
		}

		// flush to local
		// XXX: (Possible optimization) We can do this in the background
		flush(key, rec, tx);

		uncache(key, sinkProcessId);
		// XXX: cache the touched record longer?
	}

	public void uncache(RecordKey key, int sinkProcessId) {
		synchronized (prepareAnchor(key)) {
			List<WriteBackTuple> tuples = writeBackRecMap.get(key);

			Iterator<WriteBackTuple> iter = tuples.iterator();
			while (iter.hasNext()) {
				WriteBackTuple wbt = iter.next();
				if (wbt.sinkProcessId == sinkProcessId) {
					iter.remove();
					break;
				}
			}

			if (tuples.isEmpty())
				writeBackRecMap.remove(key);
		}
	}

	private void flush(RecordKey key, CachedRecord rec, Transaction tx) {
		if (rec.isDeleted())
			VanillaCoreCrud.delete(key, tx);
		else if (rec.isNewInserted())
			VanillaCoreCrud.insert(key, rec, tx);
		else if (rec.isDirty())
			VanillaCoreCrud.update(key, rec, tx);
	}
}

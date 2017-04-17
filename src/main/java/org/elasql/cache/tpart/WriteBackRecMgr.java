package org.elasql.cache.tpart;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.cache.tpart.WriteBackRecMgr.WriteBackTuple;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.storage.tx.Transaction;

public class WriteBackRecMgr {
	private static final long UNCACHE_DELAY = 500;
	/*
	 * The write back tuples should be inserted followed the sink process id in
	 * decs. order.
	 */
	private Map<RecordKey, List<WriteBackTuple>> writeBackRecMap = new ConcurrentHashMap<RecordKey, List<WriteBackTuple>>();

	// the thread pool for uncahced task
	private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);

	private final Object[] anchors = new Object[100000];

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
				//System.out.println("Key " + key + "first pust at" + System.currentTimeMillis());
				writeBackRecMap.put(key, tuples);
			} else {
				//System.out.println("Key " + key + "put " + tuples.size() + "times at" + System.currentTimeMillis()+ "by " + Thread.currentThread().getId());
			}

			// insert the new tuple into the head of the list

			tuples.add(0, new WriteBackTuple(sinkProcessId));
		}
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
							// System.out.println("wait obj: " + key
							// + ", sink id: " + mySinkProcessId);
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
			if (tuples == null)
				System.out.println("Null key in wbr " + key + "At " + System.currentTimeMillis()+ "by " + Thread.currentThread().getId());
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
			// System.out
			// .println("ok obj: " + key + ", sink id: " + sinkProcessId);
			prepareAnchor(key).notifyAll();
		}

		// flush to local
		flush(key, rec, tx);

		uncache(key, sinkProcessId);
		// XXX: cache the touched record longer?
	}

	class UncacheTask implements Runnable {
		RecordKey key;
		int sinkProcessId;

		UncacheTask(RecordKey key, int sinkProcessId) {
			this.key = key;
			this.sinkProcessId = sinkProcessId;
		}

		@Override
		public void run() {
			uncache(key, sinkProcessId);
		}
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

			// System.out.println("tuples size: " + tuples.size());
			//System.out.println("Remove key at" + key + "at " + System.currentTimeMillis() + "by " + Thread.currentThread().getId());
			if(tuples.isEmpty())
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

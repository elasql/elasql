package org.elasql.cache.tpart;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.RemoteRecordReceiver;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;


public class NewTPartCacheMgr implements RemoteRecordReceiver {
	private Map<CachedEntryKey, CachedRecord> cachedRecordMap = new ConcurrentHashMap<CachedEntryKey, CachedRecord>();
	private Set<CachedEntryKey> remoteFlags = Collections.synchronizedSet(new HashSet<CachedEntryKey>());
	// a struture to maintain every txn's writeback information
	// Map<txn_id,List<record_key>>
	// private Map<Long, ArrayList<RecordKey>> writeBackFlags = new
	// HashMap<Long, ArrayList<RecordKey>>();

	private int[] latencies = new int[200];
	private Object[] syncObjs = new Object[200];

	private WriteBackRecMgr writeBackMgr = new WriteBackRecMgr();

	private static long mySinkId;

	private final Object anchors[] = new Object[1009];

	public NewTPartCacheMgr() {
		// TODO set mySinkId as -{partitionId}
		mySinkId = getPartitionTxnId(Elasql.serverId());

		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}

		for (int i = 0; i < latencies.length; ++i) {
			latencies[i] = 0;
		}
	}

	private Object prepareAnchor(Object o) {
		int hash = o.hashCode() % anchors.length;
		if (hash < 0) {
			hash += anchors.length;
		}
		return anchors[0];
	}

	public static long getPartitionTxnId(int partId) {
		return (partId + 1) * -1;
	}

	/**
	 * 
	 * @param key
	 * @param src
	 * @param dest
	 * @return
	 */
	public CachedRecord read(RecordKey key, long src, long dest) {

		CachedEntryKey k = new CachedEntryKey(key, src, dest);
		synchronized (prepareAnchor(k)) {
			// wait if the remote record has not delivered
			try {
				while (!cachedRecordMap.containsKey(k)) {
					// System.out.println("Read <" + key + "," + src + "," +
					// dest
					// + ">");
					// System.out.println("transaction " + dest
					// + " wait for read <" + key + "," + src + "," + dest
					// + ">");
					prepareAnchor(k).wait();
					// System.out.println("Read OK <" + key + "," + src + ","
					// + dest + ">");
				}
			} catch (InterruptedException e) {
				throw new RuntimeException();
			}
			// read from the cache

			return cachedRecordMap.get(k);
		}
	}

	public void update(RecordKey key, Map<String, Constant> fldVals, Transaction tx, Long... dests) {
		CachedRecord rec = new CachedRecord();
		// if (key.getKeyFldSet().contains("ca_id")) {
		// System.out.print("Update <" + key + "," + tx.getTransactionNumber()
		// + "> ->");
		// for (Long l : dests)
		// System.out.print(l + ", ");
		// System.out.println("");
		// }
		rec.setVals(fldVals);
		rec.setSrcTxNum(tx.getTransactionNumber());
		if (dests != null) {

			for (int i = 0; i < dests.length; i++) {
				CachedEntryKey k = new CachedEntryKey(key, tx.getTransactionNumber(), dests[i]);
				synchronized (prepareAnchor(k)) {
					cachedRecordMap.put(k, rec);
					prepareAnchor(k).notifyAll();
				}
			}

		}
	}

	public void insert(RecordKey key, Map<String, Constant> fldVals, Transaction tx, Long... dests) {
		CachedRecord rec = new CachedRecord(fldVals);
		rec.setSrcTxNum(tx.getTransactionNumber());
		rec.setNewInserted(true);
		if (dests != null) {
			for (int i = 0; i < dests.length; i++) {
				CachedEntryKey k = new CachedEntryKey(key, tx.getTransactionNumber(), dests[i]);
				synchronized (prepareAnchor(k)) {
					cachedRecordMap.put(k, rec);
					prepareAnchor(k).notifyAll();
				}
			}
		}
	}

	public void delete(RecordKey key, Transaction tx, Long... dests) {
		CachedRecord dummyRec = new CachedRecord();
		dummyRec.setSrcTxNum(tx.getTransactionNumber());
		dummyRec.delete();
		if (dests != null) {
			for (int i = 0; i < dests.length; i++) {
				CachedEntryKey k = new CachedEntryKey(key, tx.getTransactionNumber(), dests[i]);
				synchronized (prepareAnchor(k)) {
					cachedRecordMap.put(k, dummyRec);
					prepareAnchor(k).notifyAll();
				}
			}

		}
	}

	public void writeBack(RecordKey key, long src, int sinkProcessId, Transaction tx) {
		// read from the cache to get the record

		// if (key.getKeyFldSet().contains("ca_id")) {
		// System.out.println("read: " + key + ", src:" + src + ", target:"
		// + mySinkId);
		// DelayPrinter.shareInstance().print(
		// "fuck: " + key + ", src:" + src + ", target:" + mySinkId
		// + ", hash: " + hash + ", time:" + time);
		// }
		CachedRecord rec = read(key, src, mySinkId);
		// if (key.getKeyFldSet().contains("ca_id")) {
		// System.out.println("read done: " + key + ", src:" + src
		// + ", target:" + mySinkId);
		// DelayPrinter.shareInstance().removeMessage(
		// "fuck: " + key + ", src:" + src + ", target:" + mySinkId
		// + ", hash: " + hash + ", time:" + time);
		// }
		// System.out.println("writeback: " + key + ", src:" + src + ", target:"
		// + mySinkId);
		writeBackMgr.writeBackRecord(key, sinkProcessId, rec, tx);
		// System.out.println("writeback done: " + key + ", src:" + src
		// + ", target:" + mySinkId);
		uncache(key, src, mySinkId);
	}

	public void uncache(RecordKey key, long src, long dest) {
		// System.out.println("Uncache <" + key + "," + src + "," + dest + ">");
		CachedEntryKey k = new CachedEntryKey(key, src, dest);
		synchronized (prepareAnchor(k)) {
			cachedRecordMap.remove(k);
		}
	}

	/**
	 * Reads the record from sink and creates the cached record with specified
	 * key.
	 * 
	 */
	public void createCacheRecordFromSink(RecordKey key, int mySinkProcessId, Transaction tx, long... dests) {

		CachedRecord rec = readFromSink(key, mySinkProcessId, tx);
		// TODO: need to deal with null value
		for (int i = 0; i < dests.length; i++) {
			// System.out.println("Create Cache Record <" + key + ","
			// + mySinkId + "," + dests[i] + ">, record: " + rec);
			CachedEntryKey k = new CachedEntryKey(key, mySinkId, dests[i]);
			synchronized (prepareAnchor(k)) {
				cachedRecordMap.put(k, rec);
				prepareAnchor(k).notifyAll();
			}
		}
	}

	public CachedRecord readFromSink(RecordKey key, int mySinkProcessId, Transaction tx) {
		// read from write back cache first
		// System.out.println("Read from sink: <" + key + "," + mySinkProcessId
		// + "," + tx.getTransactionNumber());
		CachedRecord rec = writeBackMgr.getCachedRecord(key, mySinkProcessId);
		// if there is no write back cache, read from local storage
		if (rec == null)
			rec = VanillaCoreCrud.read(key, tx);

		// System.out.println("Read from sink done: <" + key + ","
		// + mySinkProcessId + "," + tx.getTransactionNumber());
		return rec;
	}

	public void cacheRemoteRecord(RecordKey key, long src, long dest, CachedRecord rec) {
		CachedEntryKey k = new CachedEntryKey(key, src, dest);

		// + dest + ">, hash: " + hash + ", time:" + time);
		// System.out.println("Cache Remote Record <" + key + "," + src + ","
		synchronized (prepareAnchor(k)) {
			cachedRecordMap.put(k, rec);
			remoteFlags.remove(k);
			prepareAnchor(k).notifyAll();
		}
	}

	public void setRemoteFlag(RecordKey key, long src, long dest) {
		CachedEntryKey k = new CachedEntryKey(key, src, dest);
		// the remote pushing may arrive earlier than the flag to be set up
		synchronized (prepareAnchor(k)) {
			if (!cachedRecordMap.containsKey(k))
				remoteFlags.add(k);
		}
	}

	public void setWriteBackInfo(RecordKey key, int sinkProcessId) {
		writeBackMgr.setWriteBackInfo(key, sinkProcessId);
	}

	@Override
	public void cacheRemoteRecord(org.elasql.sql.RecordKey key, org.elasql.cache.CachedRecord rec) {
		// TODO Auto-generated method stub

	}

}

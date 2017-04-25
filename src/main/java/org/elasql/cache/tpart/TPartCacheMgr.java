package org.elasql.cache.tpart;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.RemoteRecordReceiver;
import org.elasql.remote.groupcomm.Tuple;
import org.elasql.sql.RecordKey;
import org.elasql.util.PeriodicalJob;
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

	private static WriteBackRecMgr writeBackMgr = new WriteBackRecMgr();

	private Map<CachedEntryKey, CachedRecord> exchange = new ConcurrentHashMap<CachedEntryKey, CachedRecord>();

	private final Object anchors[] = new Object[1009];

	public TPartCacheMgr() throws IOException {
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}

		/*File dir = new File(".");
		File outputFile = new File(dir, "left_over.txt");
		FileWriter wrFile = new FileWriter(outputFile);
		final BufferedWriter bwrFile = new BufferedWriter(wrFile);
*/
//		new PeriodicalJob(3000, 500000, new Runnable() {
//
//			@Override
//			public void run() {
//				System.out.println("The size of exchange: " + exchange.size());
//				/*long nowTime = System.currentTimeMillis();
//				try {
//					bwrFile.write("++++++++++++++++++++++++++++++++++++++++at"+nowTime+"\n");
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				for (CachedEntryKey k : exchange.keySet()) {
//					if (nowTime - k.getTime() > 10000) {
//						try {
//							bwrFile.write(String.valueOf(k.getRemote()) + k + "\n");
//						} catch (IOException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//					}
//				}*/
//
//			}
//
//		}).start();
	}

	private Object prepareAnchor(Object o) {
		int hash = o.hashCode() % anchors.length;
		if (hash < 0) {
			hash += anchors.length;
		}
		return anchors[0];
	}

	public CachedRecord readFromSink(RecordKey key, int mySinkId, Transaction tx) {
		return writeBackMgr.read(key, mySinkId, tx);
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
		k.setTime();
		k.setRemote(isRemote);
		synchronized (prepareAnchor(k)) {
			exchange.put(k, rec);
			prepareAnchor(k).notifyAll();
		}
	}

	public void release(CachedEntryKey k) {

		synchronized (prepareAnchor(k)) {

			CachedRecord rec = exchange.remove(k);
			if (rec != null)
				System.out.println("writeback done: " + k.getRecordKey() + ", src:" + k.getSource() + ", target:"
						+ k.getDestination());

		}
	}

	@Override
	public void cacheRemoteRecord(Tuple t) {
		passToTheNextTx(t.key, t.rec, t.srcTxNum, t.destTxNum, true);
	}

	public void writeBack(RecordKey key, int sinkProcessId, CachedRecord rec, Transaction tx) {
		writeBackMgr.writeBackRecord(key, sinkProcessId, rec, tx);
	}

	public void setWriteBackInfo(RecordKey key, int sinkProcessId) {
		writeBackMgr.setWriteBackInfo(key, sinkProcessId);
	}

}

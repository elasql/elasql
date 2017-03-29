package org.elasql.cache.calvin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.cache.CachedRecord;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.server.task.Task;

import static org.elasql.cache.calvin.CalvinPostOffice.NUM_DISPATCHERS;

public class RemoteRecordDispatcher extends Task {

	private static enum EventType {
		REGISTER, UNREGISTER, REMOTE_RECORD
	}

	private static interface Event {
		EventType getEventType();
	}

	private static class RegisterRequest implements Event {
		long txNum;
		CalvinCacheMgr cacheMgr;

		RegisterRequest(long txNum, CalvinCacheMgr cacheMgr) {
			this.txNum = txNum;
			this.cacheMgr = cacheMgr;
		}

		public EventType getEventType() {
			return EventType.REGISTER;
		}
	}

	private static class UnregisterRequest implements Event {
		long txNum;

		UnregisterRequest(long txNum) {
			this.txNum = txNum;
		}

		public EventType getEventType() {
			return EventType.UNREGISTER;
		}
	}

	private static class RemoteRecord implements Event {
		RecordKey key;
		CachedRecord record;

		RemoteRecord(RecordKey key, CachedRecord record) {
			this.key = key;
			this.record = record;
		}

		public EventType getEventType() {
			return EventType.REMOTE_RECORD;
		}
	}

	// For thread-to-thread communication
	private BlockingQueue<Event> eventQueue;

	// For dispatcher thread
	private Map<Long, CalvinCacheMgr> channelMap;
	private Map<Long, Set<RemoteRecord>> cachedRecords;
	private long lowerWaterMark; // The transaction number that all
	// transactions with smaller number have committed.
	private Set<Long> committedTxs; // The committed transactions
	// whose number larger than lowerWaterMark
	
	private int dispatcherId;

	RemoteRecordDispatcher(int id) {
		eventQueue = new LinkedBlockingQueue<Event>();
		channelMap = new HashMap<Long, CalvinCacheMgr>();
		cachedRecords = new HashMap<Long, Set<RemoteRecord>>();
		lowerWaterMark = Elasql.START_TX_NUMBER - CalvinPostOffice.NUM_DISPATCHERS + id;
		committedTxs = new HashSet<Long>();
		dispatcherId = id;
		
		if (id == 3)
		new Thread() {
			@Override
			public void run() {
				long startTime = System.currentTimeMillis();
				long lastRecordTime = 0;
				long elapsedTime = System.currentTimeMillis() - startTime;
				long totalTime = 30000;
				long recordInterval = 1000; // in millisecond

				while (elapsedTime < totalTime) {
					// Record tx counts
					if (elapsedTime - lastRecordTime >= recordInterval) {
						lastRecordTime = elapsedTime;
						System.out.println("D" + dispatcherId + "'s EventQueue Size: " + eventQueue.size());
						System.out.println("D" + dispatcherId + "'s ChannelMap Size: " + channelMap.size());
						System.out.println("D" + dispatcherId + "'s CachedRecordMap Size: " + cachedRecords.size());
						System.out.println("D" + dispatcherId + "'s LowerWaterMark: " + lowerWaterMark);
						System.out.println("D" + dispatcherId + "'s CommittedSet Size: " + committedTxs.size());
					}

					// Sleep for a short time (avoid busy waiting)
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					// Update elapsed time
					elapsedTime = System.currentTimeMillis() - startTime;
				}

//				int i = 0;
//				for (Long txNum : cachedRecords.keySet()) {
//					System.out.println("Tx: " + txNum);
//					System.out.println(cachedRecords.get(txNum));
//
//					i++;
//					if (i > 5) {
//						break;
//					}
//				}
			}
		}.start();
	}
	
	@Override
	public void run() {

		while (true) {
			try {
				// Retrieve an event
				Event e = eventQueue.take();

				switch (e.getEventType()) {
				case REGISTER:
					RegisterRequest rq = (RegisterRequest) e;

					// Add the channel
					channelMap.put(rq.txNum, rq.cacheMgr);

					// Check if there is any cached record
					Set<RemoteRecord> cachedRecs = cachedRecords.get(rq.txNum);

					// Transfer the cached records
					if (cachedRecs != null) {
						for (RemoteRecord rec : cachedRecs)
							rq.cacheMgr.receiveRemoteRecord(rec.key, rec.record);
					}

					break;
				case UNREGISTER:
					UnregisterRequest ur = (UnregisterRequest) e;

					// Delete the channel
					channelMap.remove(ur.txNum);
				
					// If the tx number = (lower water mark + NUM_DISPATCHERS), update
					// the lower water mark
					if (ur.txNum == lowerWaterMark + NUM_DISPATCHERS) {
						lowerWaterMark += NUM_DISPATCHERS;
						cachedRecords.remove(lowerWaterMark);

						// Process all committed transactions
						while (committedTxs.remove(lowerWaterMark + NUM_DISPATCHERS)) {
							lowerWaterMark += NUM_DISPATCHERS;
							cachedRecords.remove(lowerWaterMark);
						}
					} else {
						// If it is not, add it to committed tx.
						committedTxs.add(ur.txNum);
					}

					break;
				case REMOTE_RECORD:
					RemoteRecord rr = (RemoteRecord) e;

					// If the tx number is lower than lower water mark,
					// the record should be abandoned.
					long txNum = rr.record.getSrcTxNum();
					if (txNum <= lowerWaterMark)
						continue;

					// Send the record to the corresponding channel
					CalvinCacheMgr cacheMgr = channelMap.get(txNum);

					if (cacheMgr != null) {
						cacheMgr.receiveRemoteRecord(rr.key, rr.record);
					} else {
						// If there is no such channel, cache it.
						Set<RemoteRecord> cache = cachedRecords.get(txNum);

						if (cache == null) {
							cache = new HashSet<RemoteRecord>();
							cachedRecords.put(txNum, cache);
						}

						cache.add(rr);
					}

					break;
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	// ======================
	// APIs for Other Threads
	// ======================

	void cacheRemoteRecord(RecordKey key, CachedRecord rec) {
		eventQueue.add(new RemoteRecord(key, rec));
	}

	void registerCacheMgr(long txNum, CalvinCacheMgr cacheMgr) {
		eventQueue.add(new RegisterRequest(txNum, cacheMgr));
	}

	void ungisterTransaction(long txNum) {
		eventQueue.add(new UnregisterRequest(txNum));
	}
}

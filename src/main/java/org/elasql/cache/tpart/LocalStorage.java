package org.elasql.cache.tpart;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.sql.RecordKey;
import org.elasql.util.PeriodicalJob;
import org.vanilladb.core.storage.tx.Transaction;

public class LocalStorage {
	
	private static final int NUM_ANCHOR = 1009;
	
	/**
	 * == Reading ==
	 * For read-only:
	 *    Check if self < write_queue.head
	 *    Remove self from the read queue
	 * For write:
	 *    Check if self < read_queue.head && < write_queue.head
	 *    
	 * == Writing ==
	 *    Write
	 *    Remove self from the write queue
	 * 
	 * @author SLMT
	 *
	 */
	private class LockRequests {
		LinkedList<Long> sharedLocks = new LinkedList<Long>();
		LinkedList<Long> exclusiveLocks = new LinkedList<Long>();
		
		@Override
		public String toString() {
			return "S Reqs: " + sharedLocks + ", X Reqs: " + exclusiveLocks;
		}
	}
	
	private Map<RecordKey, LockRequests> requestMap = 
			new ConcurrentHashMap<RecordKey, LockRequests>();
	
	// Lock-stripping
	private final Object anchors[] = new Object[NUM_ANCHOR];
	
	public LocalStorage() {
		for (int i = 0; i < anchors.length; i++)
			anchors[i] = new Object();
		new PeriodicalJob(5_000, 300_000, new Runnable() {

			@Override
			public void run() {
				System.out.println("Size: " + requestMap.size());
			}
			
		}).start();
	}
	
	/**
	 * Request the shared lock of a record for reading it in the near future.
	 * A sink process can request the shared lock of the same object multiple times.
	 * Those requests will be handled separately.
	 * 
	 * @param key
	 * @param txNum
	 */
	public void requestSharedLock(RecordKey key, long txNum) {
//		System.out.println(String.format("%d request reads on %s", sinkProcessId, key));
		synchronized (getAnchor(key)) {
			LockRequests requests = getLockRequests(key);
			
			if (!requests.exclusiveLocks.isEmpty() &&
					requests.exclusiveLocks.peekLast() == txNum)
				return;
			
			requests.sharedLocks.add(txNum);
		}
	}
	
	/**
	 * Request the exclusive lock of a record for reading and modifying it 
	 * in the near future. If it had requested the shared lock of the record,
	 * the request would be upgraded for the exclusive lock. 
	 * 
	 * @param key
	 * @param txNum
	 */
	public void requestExclusiveLock(RecordKey key, long txNum) {
//		System.out.println(String.format("%d request writes on %s", sinkProcessId, key));
		synchronized (getAnchor(key)) {
			LockRequests requests = getLockRequests(key);
			
			// Remove the shared request using the same sink process id
			if (!requests.sharedLocks.isEmpty() &&
					requests.sharedLocks.peekLast() == txNum)
				requests.sharedLocks.removeLast();
			
			requests.exclusiveLocks.add(txNum);
		}
	}
	
	/**
	 * Read the requested record. If a sink process only requested a shared lock,
	 * the lock would be release immediately after the process got the record. 
	 * 
	 * @param key
	 * @param txNum
	 * @param tx
	 * @return
	 */
	public CachedRecord read(RecordKey key, Transaction tx) {
		Object anchor = getAnchor(key);
		synchronized (anchor) {
			try {
				LockRequests requests = getLockRequests(key);
				long txNum = tx.getTransactionNumber();
				long xh = peekHead(requests.exclusiveLocks);
				
//				System.out.println(String.format("%d reads %s: %s", txNum, key, requests));
				
				// Compare its process id (SI) with the head (XH) of the exclusive lock queue.
	
				// If SI > XH, wait for the next check
				while (txNum > xh) {
					anchor.wait(10000);
					xh = peekHead(requests.exclusiveLocks);
				}
				
				// If SI = XH, check if SI is also smaller than the head (SH) of shared lock queue
				// If it wasn't, continue waiting and checking SH
				// If it was, read the object (but do nothing to the queue)
				if (txNum == xh) {
					long sh = peekHead(requests.sharedLocks);
					
					while (txNum > sh) {
						anchor.wait();
						sh = peekHead(requests.sharedLocks);
					}
					
					return VanillaCoreCrud.read(key, tx);
				}
				
				// If SI < XH, read the object and remove itself from the shared lock queue
				// Then, notify the other waiting
				// Check if it can remove this request pair
				CachedRecord rec = VanillaCoreCrud.read(key, tx);
				
				// Convert to Integer to avoid calling remove(index)
				if (!requests.sharedLocks.remove((Long) txNum)) {
					throw new RuntimeException(
							"Cannot find " + txNum + " in neithor "
									+ "shared queue or the head of exclusive queue of " + key);
				}
				
				if (requests.sharedLocks.isEmpty() && requests.exclusiveLocks.isEmpty())
					requestMap.remove(key);
				
				anchor.notifyAll();
				
				return rec;
				
				// TODO: Is it possible that SI < XH, but the tx actually requesting x lock ?
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(
						"Interrupted when waitting for sink read " + key);
			}
		}
	}
	
	/**
	 * Write back the record and release the lock that the sink process holds.
	 * 
	 * @param key
	 * @param sinkProcessId
	 * @param rec
	 * @param tx
	 */
	public void writeBack(RecordKey key, CachedRecord rec, Transaction tx) {
		Object anchor = getAnchor(key);
		synchronized (anchor) {
			long txNum = tx.getTransactionNumber();
			LockRequests requests = getLockRequests(key);
			
//			System.out.println(String.format("%d writes %s: %s", txNum, key, requests));
			
			// Write and remove itself from the queue
			// Check if it can remove this request pair
			writeToVanillaCore(key, rec, tx);
			
			// It should be the head of exclusive locks
			if (txNum != requests.exclusiveLocks.peekFirst())
				throw new RuntimeException(
						"" + txNum + " is not the head of exclusive queue of " + key);
			
			requests.exclusiveLocks.pollFirst();
			if (requests.sharedLocks.isEmpty() && requests.exclusiveLocks.isEmpty())
				requestMap.remove(key);
			
			anchor.notifyAll();
		}
	}
	
	private Object getAnchor(Object obj) {
		int code = obj.hashCode();
		code = Math.abs(code); // avoid negative value
		return anchors[code % anchors.length];
	}
	
	private LockRequests getLockRequests(RecordKey key) {
		LockRequests requests = requestMap.get(key);
		if (requests == null) {
			requests = new LockRequests();
			requestMap.put(key, requests);
		}
		return requests;
	}
	
	private void writeToVanillaCore(RecordKey key, CachedRecord rec, Transaction tx) {
		if (rec.isDeleted())
			VanillaCoreCrud.delete(key, tx);
		else if (rec.isNewInserted())
			VanillaCoreCrud.insert(key, rec, tx);
		else if (rec.isDirty())
			VanillaCoreCrud.update(key, rec, tx);
	}
	
	// XXX: This may not be a good solution, but it is a simple solution
	private long peekHead(LinkedList<Long> queue) {
		return (queue.isEmpty())? Long.MAX_VALUE : queue.peekFirst();
	}
}

package org.elasql.cache.tpart;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.sql.RecordKey;
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
		LinkedList<Integer> sharedLocks = new LinkedList<Integer>();
		LinkedList<Integer> exclusiveLocks = new LinkedList<Integer>();
		
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
	}
	
	/**
	 * Request the shared lock of a record for reading it in the near future.
	 * 
	 * @param key
	 * @param sinkProcessId
	 */
	public void requestSharedLock(RecordKey key, int sinkProcessId) {
		synchronized (getAnchor(key)) {
			LockRequests requests = getLockRequests(key);
			
			if (!requests.sharedLocks.isEmpty() &&
					requests.sharedLocks.peekLast() == sinkProcessId)
				return;
			
			if (!requests.exclusiveLocks.isEmpty() &&
					requests.exclusiveLocks.peekLast() == sinkProcessId)
				return;
			
			requests.sharedLocks.add(sinkProcessId);
		}
	}
	
	/**
	 * Request the exclusive lock of a record for reading and modifying it 
	 * in the near future. If it had requested the shared lock of the record,
	 * the request would be upgraded for the exclusive lock. 
	 * 
	 * @param key
	 * @param sinkProcessId
	 */
	public void requestExclusiveLock(RecordKey key, int sinkProcessId) {
		synchronized (getAnchor(key)) {
			LockRequests requests = getLockRequests(key);
			
			// Remove the shared request using the same sink process id
			if (!requests.sharedLocks.isEmpty() &&
					requests.sharedLocks.peekLast() == sinkProcessId)
				requests.sharedLocks.removeLast();
			
			requests.exclusiveLocks.add(sinkProcessId);
		}
	}
	
	/**
	 * Read the requested record. If a sink process only requested a shared lock,
	 * the lock would be release immediately after the process got the record. 
	 * 
	 * @param key
	 * @param sinkProcessId
	 * @param tx
	 * @return
	 */
	public CachedRecord read(RecordKey key, int sinkProcessId, Transaction tx) {
		Object anchor = getAnchor(key);
		synchronized (anchor) {
			try {
				LockRequests requests = getLockRequests(key);
				int xh = peekHead(requests.exclusiveLocks);
				
				// Compare its process id (SI) with the head (XH) of the exclusive lock queue.
	
				// If SI > XH, wait for the next check
				while (sinkProcessId > xh) {
					anchor.wait();
					xh = peekHead(requests.exclusiveLocks);
				}
				
				// If SI = XH, check if SI is also smaller than the head (SH) of shared lock queue
				// If it wasn't, continue waiting and checking SH
				// If it was, read the object (but do nothing to the queue)
				if (sinkProcessId == xh) {
					int sh = peekHead(requests.sharedLocks);
					
					while (sinkProcessId > sh) {
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
				if (!requests.sharedLocks.remove((Integer) sinkProcessId)) {
					throw new RuntimeException(
							"Cannot find " + sinkProcessId + " in neithor "
									+ "shared queue or the head of exclusive queue");
				}
				
				if (requests.sharedLocks.isEmpty() && requests.exclusiveLocks.isEmpty())
					requestMap.remove(key);
				
				anchor.notifyAll();
				
				return rec;
				
				// TODO: Is it possible that SI < XH, but the tx actually requesting x lock ?
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(
						"Interrupted when waitting for sink read");
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
	public void writeBack(RecordKey key, int sinkProcessId, CachedRecord rec, Transaction tx) {
		Object anchor = getAnchor(key);
		synchronized (anchor) {
			LockRequests requests = getLockRequests(key);
			
			// Write and remove itself from the queue
			// Check if it can remove this request pair
			writeToVanillaCore(key, rec, tx);
			
			// It should be the head of exclusive locks
			if (sinkProcessId != requests.exclusiveLocks.peekFirst())
				throw new RuntimeException(
						"" + sinkProcessId + " is not the head of exclusive queue");
			
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
	private int peekHead(LinkedList<Integer> queue) {
		return (queue.isEmpty())? Integer.MAX_VALUE : queue.peekFirst();
	}
}

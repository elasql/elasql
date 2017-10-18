package org.elasql.storage.tx.concurrency.tpart;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.sql.RecordKey;

/**
 * A concurrency control manager that handles the access to the local storage in T-Part,
 *  such as sink readings and write-backs of T-Graphs.
 * 
 * @author Yu-Shan Lin
 */
public class LocalStorageCcMgr {
	
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
	
	public LocalStorageCcMgr() {
		for (int i = 0; i < anchors.length; i++)
			anchors[i] = new Object();
	}
	
	/**
	 * Request the shared lock of a record for reading it in the near future. <br />
	 * <br />
	 * All requests should be issued in the same thread.
	 * 
	 * @param key
	 * @param txNum
	 */
	public void requestSinkRead(RecordKey key, long txNum) {
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
	 * the request would be upgraded for the exclusive lock. <br />
	 * <br />
	 * All requests should be issued in the same thread.
	 * 
	 * @param key
	 * @param txNum
	 */
	public void requestWriteBack(RecordKey key, long txNum) {
		synchronized (getAnchor(key)) {
			LockRequests requests = getLockRequests(key);
			
			// Remove the shared lock request for the same transaction
			if (!requests.sharedLocks.isEmpty() &&
					requests.sharedLocks.peekLast() == txNum)
				requests.sharedLocks.removeLast();
			
			requests.exclusiveLocks.add(txNum);
		}
	}
	
	/**
	 * If a transaction only requested for sink read,
	 * the lock would be release immediately after the process got the record. 
	 * 
	 * @param key
	 * @param txNum
	 * @param tx
	 * @return
	 */
	public void beforeSinkRead(RecordKey key, long txNum) {
		Object anchor = getAnchor(key);
		synchronized (anchor) {
			try {
				LockRequests requests = getLockRequests(key);
				long xh = peekHead(requests.exclusiveLocks);
				
				// Compare its number (txNum) with the head (XH) of the exclusive lock queue.
	
				// If txNum > XH, wait for the next check
				while (txNum > xh) {
					anchor.wait();
					xh = peekHead(requests.exclusiveLocks);
				}
				
				// If txNum = XH, check if txNum is also smaller than the head (SH) of shared lock queue
				// If it wasn't, continue waiting and checking SH
				// If it was, allow to read the object (but do nothing to the queue)
				if (txNum == xh) {
					long sh = peekHead(requests.sharedLocks);
					
					while (txNum > sh) {
						anchor.wait();
						sh = peekHead(requests.sharedLocks);
					}
				}
				
				// If txNum < XH, allow to read the object
				
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(
						"Interrupted when waitting for sink read " + key);
			}
		}
	}
	
	public void beforeWriteBack(RecordKey key, long txNum) {
		Object anchor = getAnchor(key);
		synchronized (anchor) {
			LockRequests requests = getLockRequests(key);
			
			// It should be the head of exclusive locks
			if (txNum != requests.exclusiveLocks.peekFirst())
				throw new RuntimeException(
						"Tx." + txNum + " is not the head of exclusive queue of " + key);
			
			// Do nothing
			// In T-Part, a transaction's write-back must come after a former transaction's
			// sink-read on the same record.
			// That is, we don't need to do anything for a write-back since a former transaction
			// has waited during its sink-read. The write-back transaction must be blocked by
			// the sink-read transaction in TPartCacheMgr.
		}
	}
	
	public void afterSinkRead(RecordKey key, long txNum) {
		Object anchor = getAnchor(key);
		synchronized (anchor) {
			LockRequests requests = getLockRequests(key);
			
			// If it was the head of the exclusive queue, it should return immediately.
			if (txNum == peekHead(requests.exclusiveLocks))
				return;
			
			// Remove itself from shared lock queue
			// Convert to Long object to avoid calling remove(index)
			if (!requests.sharedLocks.remove((Long) txNum)) {
				throw new RuntimeException(
						"Cannot find Tx." + txNum + " in neithor "
								+ "shared queue or the head of exclusive queue of " + key);
			}
			
			if (requests.sharedLocks.isEmpty() && requests.exclusiveLocks.isEmpty())
				requestMap.remove(key);
			
			anchor.notifyAll();
		}
	}
	
	public void afterWriteback(RecordKey key, long txNum) {
		Object anchor = getAnchor(key);
		synchronized (anchor) {
			LockRequests requests = getLockRequests(key);
			
			// It should be the head of exclusive locks
			if (txNum != requests.exclusiveLocks.peekFirst())
				throw new RuntimeException(
						"Tx." + txNum + " is not the head of exclusive queue of " + key);
			
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
	
	// XXX: This may not be a good solution, but it is a simple solution
	private long peekHead(LinkedList<Long> queue) {
		return (queue.isEmpty())? Long.MAX_VALUE : queue.peekFirst();
	}
}

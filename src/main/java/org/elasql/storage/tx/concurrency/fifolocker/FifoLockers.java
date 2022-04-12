package org.elasql.storage.tx.concurrency.fifolocker;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FifoLockers is used to keep which transaction possesses right to access a
 * record, and to keep those transactions which are waiting for the record.
 * 
 * @author Pin-Yu Wang
 *
 */
public class FifoLockers {
	private ConcurrentLinkedDeque<FifoLock> requestQueue = new ConcurrentLinkedDeque<FifoLock>();
	private ConcurrentLinkedDeque<Long> sLockers = new ConcurrentLinkedDeque<Long>();
	private AtomicLong xLocker = new AtomicLong(-1);

	private boolean sLocked() {
		// avoid using sLockers.size() due to the cost of traversing the queue
		return !sLockers.isEmpty();
	}

	private boolean xLocked() {
		return xLocker.get() != -1;
	}

	private boolean hasSLock(long txNum) {
		return sLockers.contains(txNum);
	}

	private boolean hasXLock(long txNum) {
		return xLocker.get() == txNum;
	}

	private boolean isTheOnlySLocker(long txNum) {
		return sLockers.size() == 1 && sLockers.contains(txNum);
	}

	private boolean sLockable(long txNum) {
		return (!xLocked() || hasXLock(txNum));
	}

	private boolean xLockable(long txNum) {
		return (!sLocked() || isTheOnlySLocker(txNum)) && (!xLocked() || hasXLock(txNum));
	}

	public void addToRequestQueue(FifoLock fifoLock) {
		requestQueue.add(fifoLock);
//		System.out.println("head is " + requestQueue.peek().getTxNum() + " tail is " + requestQueue.peekLast().getTxNum());
	}

	public void waitOrPossessSLock(FifoLock myFifoLock) {
		long myTxNum = myFifoLock.getTxNum();
		while (true) {
			synchronized(myFifoLock) {
				FifoLock headFifoLock = requestQueue.peek();
				if (headFifoLock.isMyFifoLock(myFifoLock) && sLockable(myTxNum)) {
					sLockers.add(myTxNum);
					requestQueue.poll();
					notifyNextSLockCandidate();
					myFifoLock.resetLockable();
					break;
				}
				myFifoLock.waitOnSLock();
			}			
		}
	}

	public void waitOrPossessXLock(FifoLock myFifoLock) {
		/*-
		 * The following example shows that a thread might not be notified.
		 * 
		 * Assume Thread A and Thread B come concurrently,
		 * and both of them want to acquires xLock.
		 * 
		 * =========================================================
		 * 			Thread A peaks the request queue (see A's proxy object)
		 * 			Thread A finds xLockable
		 * 			Thread A finds the head of the request queue is itself
		 * 			Thread A updates the xLocker
		 * 			Thread A remove the head of the request queue
		 * 							|
		 * 							v
		 * 			Thread B peaks the request queue (see B's proxy object)
		 * 			Thread B finds the record not xLockable
		 * 							|
		 * 							v
		 * 			Thread A set xLocker to -1
		 * 			Thread A notify the head of the queue
		 * 							|
		 * 							|
		 *			(Boom! No one will wake thread b up. This is going to be a performance killer)
		 * 							|
		 * 							|
		 * 							v
		 * 			Thread B wait on the record
		 * =========================================================
		 * 
		 */
		long myTxNum = myFifoLock.getTxNum();
		while (true) {
			synchronized(myFifoLock) {
				FifoLock headFifoLock = requestQueue.peek();
				if (headFifoLock.isMyFifoLock(myFifoLock) && xLockable(myTxNum)) {
					xLocker.set(myTxNum);
					requestQueue.poll();
					myFifoLock.resetLockable();
					break;
				}
				myFifoLock.waitOnXLock();
			}	
//			myFifoLock.waitOnXLock();	
		}
	}

	public void releaseSLock(long txNum) {
		FifoLock nextFifoLock = requestQueue.peek();
		if (nextFifoLock == null) {
			return;
		}
		
		synchronized(nextFifoLock) {
			sLockers.remove(txNum);
			
			if (sLockable(txNum)) {
				nextFifoLock.setSLockable();
			}
			
			if (xLockable(txNum)) {
				nextFifoLock.setXLockable();
			}
			
			nextFifoLock.notifyLock();
		}
	}

	public void releaseXLock() {
		FifoLock nextFifoLock = requestQueue.peek();
		if (nextFifoLock == null) {
			return;
		}
		
		synchronized(nextFifoLock) {
			xLocker.set(-1);
			nextFifoLock.setSLockable();
			nextFifoLock.setXLockable();
			nextFifoLock.notifyLock();
		}
	}
	
	public void notifyNextSLockCandidate() {
		FifoLock nextFifoLock = requestQueue.peek();
		if (nextFifoLock == null) {
			return;
		}
		
		synchronized(nextFifoLock) {
			nextFifoLock.setSLockable();
			nextFifoLock.notifyLock();
		}
	}
}

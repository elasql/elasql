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

	private void waitIfHeadIsNotMe(FifoLock myFifoLock) {
		while (true) {
			synchronized (myFifoLock) {
				FifoLock headFifoLock = requestQueue.peek();

				if (!headFifoLock.isMyFifoLock(myFifoLock)) {
					myFifoLock.waitOnLock();
				} else {
					break;
				}
			}
		}
	}

	public void waitOrPossessSLock(FifoLock myFifoLock) {
		waitIfHeadIsNotMe(myFifoLock);

		long myTxNum = myFifoLock.getTxNum();
		synchronized (myFifoLock) {
			while (true) {
				if (!sLockable(myTxNum)) {
					Thread.currentThread().setName(Thread.currentThread().getName() + " waits on sLock " + myFifoLock.getKey());
					myFifoLock.waitOnLock();
				} else {
					sLockers.add(myTxNum);
					Thread.currentThread().setName(Thread.currentThread().getName() + " gets sLock " + myFifoLock.getKey());
					break;
				}
			}
		}

		requestQueue.poll();
		notifyNext();
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
		waitIfHeadIsNotMe(myFifoLock);

		long myTxNum = myFifoLock.getTxNum();
		synchronized (myFifoLock) {
			while (true) {
				if (!xLockable(myTxNum)) {
					Thread.currentThread().setName(Thread.currentThread().getName() + " waits on xLock " + myFifoLock.getKey());
					myFifoLock.waitOnLock();
				} else {
					xLocker.set(myTxNum);
					Thread.currentThread().setName(Thread.currentThread().getName() + " gets xLock " + myFifoLock.getKey());
					break;
				}
			}
		}

		requestQueue.poll();
	}

	public void releaseSLock(long txNum, FifoLock myFifoLock) {
		Thread.currentThread().setName(Thread.currentThread().getName() + " releases sLock " + myFifoLock.getKey());
		FifoLock nextFifoLock = requestQueue.peek();
		if (nextFifoLock == null) {
			sLockers.remove(txNum);
			/*
			 * Check again because there is a chance that a transaction is added after the previous peek.
			 */
			nextFifoLock = requestQueue.peek();
			if (nextFifoLock == null) {
				return;
			}
		}

		synchronized (nextFifoLock) {
			sLockers.remove(txNum);
			nextFifoLock.notifyLock();
		}
	}

	public void releaseXLock(FifoLock myFifoLock) {
		Thread.currentThread().setName(Thread.currentThread().getName() + " releases xLock " + myFifoLock.getKey());
		FifoLock nextFifoLock = requestQueue.peek();
		if (nextFifoLock == null) {
			resetLockers();
			
			/*
			 * Check again because there is a chance that a transaction is added after the previous peek.
			 */
			nextFifoLock = requestQueue.peek();
			if (nextFifoLock == null) {
				return;
			}
		}

		synchronized (nextFifoLock) {
			resetLockers();
			nextFifoLock.notifyLock();
		}
	}
	
	private void resetLockers() {
		xLocker.set(-1);
		sLockers.clear();
	}

	private void notifyNext() {
		FifoLock nextFifoLock = requestQueue.peek();
		if (nextFifoLock == null) {
			return;
		}

		synchronized (nextFifoLock) {
			nextFifoLock.notifyLock();
		}
	}
}

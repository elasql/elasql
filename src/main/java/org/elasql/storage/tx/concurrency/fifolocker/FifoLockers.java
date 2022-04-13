package org.elasql.storage.tx.concurrency.fifolocker;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

import org.elasql.sql.PrimaryKey;

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
	private PrimaryKey key;

	public FifoLockers(PrimaryKey key) {
		this.key = key;
	}

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
					myFifoLock.waitOnLock();
				} else {
					sLockers.add(myTxNum);
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
		 * and both of them want to acquire xLock.
		 * 
		 * =========================================================
		 * 			Thread A peeks the request queue (see A's proxy object)
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
		 * 			Thread B waits on the record
		 * =========================================================
		 * 
		 */
		waitIfHeadIsNotMe(myFifoLock);

		long myTxNum = myFifoLock.getTxNum();
		synchronized (myFifoLock) {
			while (true) {
				if (!xLockable(myTxNum)) {
					myFifoLock.waitOnLock();
				} else {
					xLocker.set(myTxNum);
					break;
				}
			}
		}

		requestQueue.poll();
	}

	public void releaseSLock(FifoLock myFifoLock) {
		long myTxNum = myFifoLock.getTxNum();
		
		FifoLock nextFifoLock = requestQueue.peek();
		if (nextFifoLock == null) {
			sLockers.remove(myTxNum);
			/*
			 * Check again because there might be a transaction added after the
			 * previous peek.
			 */
			nextFifoLock = requestQueue.peek();
			if (nextFifoLock != null) {
				synchronized (nextFifoLock) {
					nextFifoLock.notifyLock();
				}
			}
			return;
		}

		synchronized (nextFifoLock) {
			sLockers.remove(myTxNum);
			nextFifoLock.notifyLock();
		}
	}

	public void releaseXLock(FifoLock myFifoLock) {
		long myTxNum = myFifoLock.getTxNum();
		
		FifoLock nextFifoLock = requestQueue.peek();
		if (nextFifoLock == null) {
			xLocker.set(-1);

			/*
			 * Check again because there might be a transaction added after the
			 * previous peek.
			 */
			nextFifoLock = requestQueue.peek();
			if (nextFifoLock == null) {
				return;
			} else {
				synchronized (nextFifoLock) {
					nextFifoLock.notifyLock();
				}
			}
		}

		synchronized (nextFifoLock) {
			xLocker.set(-1);
			nextFifoLock.notifyLock();
		}
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

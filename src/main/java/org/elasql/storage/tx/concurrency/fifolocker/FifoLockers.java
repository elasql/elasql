package org.elasql.storage.tx.concurrency.fifolocker;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FifoLockers is used to keep which transaction possesses right to access a
 * record, and to keep those transactions which are waiting for the record.
 * 
 * @author Pin-Yu Wang
 *
 */
public class FifoLockers {
	private ConcurrentLinkedQueue<FifoLock> requestQueue = new ConcurrentLinkedQueue<FifoLock>();
	private ConcurrentLinkedDeque<Long> sLockers = new ConcurrentLinkedDeque<Long>();
	private AtomicLong xLocker = new AtomicLong(-1);

	private boolean sLocked() {
		return sLockers.size() > 0;
	}

	private boolean xLocked() {
		return xLocker.get() != -1;
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
	}

	public void waitOrPossessSLock(long txNum) {
		while (true) {
			FifoLock fifoLock = requestQueue.peek();

			if (!sLockable(txNum) || !fifoLock.isMyFifoLock(txNum)) {
				synchronized (fifoLock) {
					try {
						fifoLock.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			} else {
				break;
			}
		}

		sLockers.add(txNum);
		
		/*
		 * requestQueue.poll() should be put at the end of this function to make this
		 * function logically atomic.
		 */
		requestQueue.poll();
	}

	public void waitOrPossessXLock(long txNum) {
		while (true) {
			FifoLock fifoLock = requestQueue.peek();

			if (!xLockable(txNum) || !fifoLock.isMyFifoLock(txNum)) {
				synchronized (fifoLock) {
					try {
						fifoLock.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			} else {
				break;
			}
		}

		xLocker.set(txNum);

		/*
		 * requestQueue.poll() should be put at the end of this function to make this
		 * function logically atomic.
		 */
		requestQueue.poll();
	}
	
	public void releaseSLock(long txNum) {
		sLockers.remove(txNum);
		notifyFirst();
	}
	
	public void releaseXLock(long txNum) {
		xLocker.set(-1);
		notifyFirst();
	}

	private void notifyFirst() {
		FifoLock fifoLock = requestQueue.peek();

		if (fifoLock == null) {
			return;
		}

		synchronized (fifoLock) {
			fifoLock.notify();
		}
	}
}

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
			FifoLock headFifoLock = requestQueue.peek();
			
//			if (headFifoLock.getKey().hashCode() != myFifoLock.getKey().hashCode()) {
//				throw new RuntimeException("sLock: key's hashcode is not equal" + " left: " + headFifoLock.getKey() + " right: " + myFifoLock.getKey());
//			}
//			
//			if ((headFifoLock.isMyFifoLock(myFifoLock) && (headFifoLock.getTxNum() != myFifoLock.getTxNum()))
//					|| (!headFifoLock.isMyFifoLock(myFifoLock) && (headFifoLock.getTxNum() == myFifoLock.getTxNum()))) {
//				throw new RuntimeException("sLock Comparison fails among threads");
//			}

			
			if (sLockable(myTxNum) && headFifoLock.isMyFifoLock(myFifoLock)) {
//				if (headFifoLock.getTxNum() != myTxNum) {
//					throw new RuntimeException("what the fuck sLock");
//				}
//				System.out.println(headFifoLock.getTxNum() + " " + headFifoLock.getKey() + " is sLockable");
				break;
			} else {
//				Thread.currentThread().setName(Thread.currentThread().getName() + " wait on sLock " + myFifoLock.getKey());
//				System.out.println(myFifoLock.getTxNum() + " " + myFifoLock.getKey() + " is NOT sLockable. The head is tx " + headFifoLock.getTxNum() + ". The sLocker is empty?" + sLockers.isEmpty() + ". The xlocker is " + xLocker.get());
//				if (!headFifoLock.isMyFifoLock(myFifoLock)) {
//					headFifoLock.notifyLock();
//				}
				myFifoLock.waitOnLock();
//				notifyFirst();
			}
		}

//		Thread.currentThread().setName("" + myTxNum);
		sLockers.add(myTxNum);

		/*
		 * requestQueue.poll() should be put after sLockers.add and should be put before
		 * notifyFirst.
		 */
		requestQueue.poll();
		
		if (requestQueue.peek() == myFifoLock) {
			throw new RuntimeException("sLock poll fails");
		}

		/*
		 * Transactions that get the sLock MUST notify the next transaction in the
		 * queue. The next transaction shouldn't be blocked because sLock is shared
		 * unless xLock is involved.
		 */
		notifyFirst();
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
			FifoLock headFifoLock = requestQueue.peek();
			
//			if (headFifoLock.getKey().hashCode() != myFifoLock.getKey().hashCode()) {
//				throw new RuntimeException("xLock: key's hashcode is not equal" + " left: " + headFifoLock.getKey() + " right: " + myFifoLock.getKey());
//			}
//			
//			if ((headFifoLock.isMyFifoLock(myFifoLock) && (headFifoLock.getTxNum() != myFifoLock.getTxNum()))
//					|| (!headFifoLock.isMyFifoLock(myFifoLock) && (headFifoLock.getTxNum() == myFifoLock.getTxNum()))) {
//				throw new RuntimeException("xLock Comparison fails among threads");
//			}

			if (xLockable(myTxNum) && headFifoLock.isMyFifoLock(myFifoLock)) {
//				if (headFifoLock.getTxNum() != myTxNum) {
//					throw new RuntimeException("what the fuck xLock");
//				}
//				System.out.println(headFifoLock.getTxNum() + " " + headFifoLock.getKey() + " is xLockable");
				break;
			} else {
//				Thread.currentThread().setName(Thread.currentThread().getName() + " wait on xLock " + myFifoLock.getKey());
//				System.out.println(myFifoLock.getTxNum() + " " + myFifoLock.getKey() + " is NOT xLockable. The head is tx " + headFifoLock.getTxNum() + ". The sLocker is empty?" + sLockers.isEmpty() + ". The xlocker is " + xLocker.get());
				
//				if (!headFifoLock.isMyFifoLock(myFifoLock)) {
//					headFifoLock.notifyLock();
//				}
				
				myFifoLock.waitOnLock();
//				
			}
		}

//		Thread.currentThread().setName("" + myTxNum);
		xLocker.set(myTxNum);

		/*
		 * requestQueue.poll() should be put at the end of this function to make this
		 * function logically atomic.
		 */
		requestQueue.poll();
		
		if (requestQueue.peek() == myFifoLock) {
			throw new RuntimeException("xLock poll fails");
		}
	}

	public void releaseSLock(long txNum) {
		sLockers.remove(txNum);
		notifyFirst();
	}

	public void releaseXLock(long txNum) {
		if (txNum != xLocker.get() ) {
			throw new RuntimeException("A transaction releases a lock that doesn't belong to itself");
		}
		xLocker.set(-1);
		notifyFirst();
	}

	private void notifyFirst() {
		FifoLock fifoLock = requestQueue.peek();

		if (fifoLock == null) {
			return;
		}

		// XXX: is is possible that notify will fail?
		// how about notify in a while loop
		fifoLock.notifyLock();
	}
}

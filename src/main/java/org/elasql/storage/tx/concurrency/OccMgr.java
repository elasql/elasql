package org.elasql.storage.tx.concurrency;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.elasql.sql.PrimaryKey;
import org.elasql.storage.tx.OccTransaction;

public class OccMgr {
	private ReentrantLock validationLatch = new ReentrantLock();

	private AtomicLong leastReadPhaseStartTimeAmongActiveTxs = new AtomicLong(0);
	private Set<Long> activeTxsStartTimes = ConcurrentHashMap.newKeySet();
	private AtomicInteger readyToCleanCount = new AtomicInteger(0);
	private ConcurrentLinkedQueue<OccTransaction> recentCompletedTxs = new ConcurrentLinkedQueue<OccTransaction>();

	public void registerActiveTx(OccTransaction occTx) {
		activeTxsStartTimes.add(occTx.getReadPhaseStartTime());
		leastReadPhaseStartTimeAmongActiveTxs.set(Collections.min(activeTxsStartTimes));
		cleanRecentCompletedTxs();
	}

	private void cleanRecentCompletedTxs() {
		for (int i = 0; i < readyToCleanCount.getAndSet(0); i++) {
			recentCompletedTxs.poll();
		}
	}
	
	public long getLeastReadPhaseStartTimeAmongActiveTxs() {
		return leastReadPhaseStartTimeAmongActiveTxs.get();
	}
	
	public Set<Long> getActiveTxsStartTimes() {
		return activeTxsStartTimes;
	}

	public void validationLock() {
		validationLatch.lock();
	}

	public void validationUnLock() {
		validationLatch.unlock();
	}

	public boolean hasValidationLocked() {
		return validationLatch.isHeldByCurrentThread();
	}

	public boolean validate(OccTransaction curOccTx) throws Exception {
		if (!hasValidationLocked()) {
			throw new Exception(String.format("Tx %d enters validation phase without possessing the validation latch",
					curOccTx.getTxNum()));
		}

		long myReadPhaseStartTime = curOccTx.getReadPhaseStartTime();

		int headRemoveCount = 0;
		Iterator<OccTransaction> iterator = recentCompletedTxs.iterator();
		while (iterator.hasNext()) {
			OccTransaction prevOccTx = iterator.next();

			if (prevOccTx.getWritePhaseEndTime() < leastReadPhaseStartTimeAmongActiveTxs.get()) {
				// the previous tx no long affects the recent transactions, remove it
				headRemoveCount += 1;
			}

			// readPhaseStartTime(Tk) < writeEndTime(Ti) < validationPhaseStartTime(Tk)
			if (myReadPhaseStartTime < prevOccTx.getWritePhaseEndTime()) {
				/*
				 * check if the write set of the previous tx has overlapped with the read set of
				 * the current tx. If overlap, then abort
				 */

				Set<PrimaryKey> intersection = new HashSet<PrimaryKey>(prevOccTx.getWriteSet());
				intersection.retainAll(curOccTx.getReadSet());
				if (!intersection.isEmpty()) {
					readyToCleanCount.set(headRemoveCount);
					activeTxsStartTimes.remove(curOccTx.getReadPhaseStartTime());
					return false;
				}
			}
		}

		readyToCleanCount.set(headRemoveCount);
		return true;
	}

	public void dropSelfFromActiveTxs(OccTransaction curOccTx) throws Exception {
		if (!hasValidationLocked()) {
			throw new Exception(String.format("Tx %d enters dropActiveTx without possessing the validation latch",
					curOccTx.getTxNum()));
		}

		recentCompletedTxs.add(curOccTx);
		activeTxsStartTimes.remove(curOccTx.getReadPhaseStartTime());
	}
}

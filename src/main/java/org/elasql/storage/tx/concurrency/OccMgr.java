package org.elasql.storage.tx.concurrency;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantLock;

import org.elasql.sql.PrimaryKey;
import org.elasql.storage.tx.OccTransaction;

public class OccMgr {
	private ReentrantLock validationLatch = new ReentrantLock();
	private ReentrantLock cleanLatch = new ReentrantLock();
	private Set<Long> activeTxsStartTimes = ConcurrentHashMap.newKeySet();
	private ConcurrentLinkedDeque<OccTransaction> recentCompletedTxs = new ConcurrentLinkedDeque<OccTransaction>();

	public void registerActiveTx(OccTransaction occTx) {
		activeTxsStartTimes.add(occTx.getReadPhaseStartTime());
		cleanRecentCompletedTxs();
	}

	private void cleanRecentCompletedTxs() {
		// a transaction will be chosen to clean the recent completed transactions
		if (cleanLatch.tryLock()) {
			try {
				long leastReadPhaseStartTimeAmongActiveTxs = Collections.min(activeTxsStartTimes);

				OccTransaction lastTx = recentCompletedTxs.peekLast();

				while (lastTx != null && lastTx.getWritePhaseEndTime() < leastReadPhaseStartTimeAmongActiveTxs) {
					recentCompletedTxs.pollLast();
					lastTx = recentCompletedTxs.peekLast();
				}
			} finally {
				cleanLatch.unlock();
			}
		}

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

//		if (curOccTx.getTxNum() % 1000 == 0) {
//			System.out.println("recentCompletedTxs size: " + recentCompletedTxs.size());
//		}

		Iterator<OccTransaction> iterator = recentCompletedTxs.iterator();
		while (iterator.hasNext()) {
			OccTransaction prevOccTx = iterator.next();

			// readPhaseStartTime(Tk) < writeEndTime(Ti) < validationPhaseStartTime(Tk)
			if (myReadPhaseStartTime < prevOccTx.getWritePhaseEndTime()) {
				/*
				 * check if the write set of the previous tx has overlapped with the read set of
				 * the current tx. If overlap, then abort
				 */

				if (prevOccTx.getWriteSet().isEmpty()) {
					// early return if the previous transaction is read-only
					continue;
				}

				Set<PrimaryKey> intersection = new HashSet<PrimaryKey>(prevOccTx.getWriteSet());

				intersection.retainAll(curOccTx.getReadSet());
				if (!intersection.isEmpty()) {
					activeTxsStartTimes.remove(curOccTx.getReadPhaseStartTime());
					return false;
				}
			} else {
				return true;
			}
		}

		return true;
	}

	public void dropSelfFromActiveTxs(OccTransaction curOccTx) throws Exception {
		if (!hasValidationLocked()) {
			throw new Exception(String.format("Tx %d enters dropActiveTx without possessing the validation latch",
					curOccTx.getTxNum()));
		}

		recentCompletedTxs.addFirst(curOccTx);
		activeTxsStartTimes.remove(curOccTx.getReadPhaseStartTime());
	}
}

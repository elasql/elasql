package org.elasql.storage.tx.concurrency;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantLock;

import org.elasql.sql.PrimaryKey;
import org.elasql.storage.tx.OccTransaction;
import org.vanilladb.core.server.task.Task;

public class OccMgr extends Task {
	private ReentrantLock validationLatch = new ReentrantLock();
	private Set<Long> activeTxsStartTimes = ConcurrentHashMap.newKeySet();
	private ConcurrentLinkedDeque<OccTransaction> recentCompletedTxs = new ConcurrentLinkedDeque<OccTransaction>();

	public void registerActiveTx(OccTransaction occTx) {
		activeTxsStartTimes.add(occTx.getReadPhaseStartTime());
//		if (occTx.getTxNum() % 5000 == 0) {
//			System.out.println("recent complted Txs: " + recentCompletedTxs.size());
//		}
	}

	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (activeTxsStartTimes.isEmpty()) {
				continue;
			}

			long leastReadPhaseStartTimeAmongActiveTxs = 0;
			try {
				leastReadPhaseStartTimeAmongActiveTxs = Collections.min(activeTxsStartTimes);
			} catch (NoSuchElementException e) {
				// it's ok
				continue;
			}

			OccTransaction lastTx = recentCompletedTxs.peekLast();

			while (lastTx != null && lastTx.getWritePhaseEndTime() < leastReadPhaseStartTimeAmongActiveTxs) {
				recentCompletedTxs.pollLast();
				lastTx = recentCompletedTxs.peekLast();
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

//				if (curOccTx.getTxNum() % 5000 == 0) {
//					System.out.println("prevOccTx: " + prevOccTx.getTxNum() + ", write set: "+ prevOccTx.getWriteSet());
//					System.out.println("curOccTx: " + curOccTx.getTxNum() + ", read set: "+ curOccTx.getReadSet());
//				}

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

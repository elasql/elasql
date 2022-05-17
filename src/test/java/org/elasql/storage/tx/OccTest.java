package org.elasql.storage.tx;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.elasql.sql.PrimaryKey;
import org.elasql.storage.tx.concurrency.OccMgr;
import org.junit.Assert;
import org.junit.Test;
import org.vanilladb.core.sql.IntegerConstant;

public class OccTest {
	private final static long NON_CONFLICT_TX_NUMS = 5_000L;

	public class NonConflictProcedure implements Runnable {
		private OccMgr occMgr;
		private AtomicLong txNumGenerator;

		public NonConflictProcedure(OccMgr occMgr, AtomicLong txNumGenerator) {
			this.occMgr = occMgr;
			this.txNumGenerator = txNumGenerator;
		}

		@Override
		public void run() {
			while (true) {
				long txNum = txNumGenerator.getAndIncrement();
				if (txNum > NON_CONFLICT_TX_NUMS) {
					break;
				}

//				System.out.println(String.format("Tx: %s has been generated", txNum));

				OccTransaction occTx = new OccTransaction(txNum);
				occTx.setReadPhaseStartTime();

				Set<PrimaryKey> readSet = new HashSet<PrimaryKey>();
				Set<PrimaryKey> writeSet = new HashSet<PrimaryKey>();

				readSet.add(new PrimaryKey("tbl", "id", new IntegerConstant(1)));
				occTx.setReadWriteSet(readSet, writeSet);

				occMgr.registerActiveTx(occTx);

				try {
					occMgr.validationLock();
					if (occMgr.validate(occTx)) {
						// do some write
						occTx.setWritePhaseEndTime();
						occMgr.dropSelfFromActiveTxs(occTx);
					} else {
						continue;
					}
				} catch (Exception e) {
					// do nothing
				} finally {
					occMgr.validationUnLock();
				}
			}
		}
	}

	@Test
	public void testConcurrentlyExecuteNonConflictTxs() {
		OccMgr occMgr = new OccMgr();
		AtomicLong txNumGenerator = new AtomicLong();
		int threadNum = 10;

		Thread[] threads = new Thread[threadNum];
		// new 10 threads to run this test
		for (int i = 0; i < threadNum; i++) {
			threads[i] = new Thread(new NonConflictProcedure(occMgr, txNumGenerator));
			threads[i].start();
		}

		for (int i = 0; i < threadNum; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		System.out.println("pass testConcurrentlyExecuteNonConflictTxs");
	}

	@Test
	public void testOccMgr() {
		OccMgr occMgr = new OccMgr();
		ConcurrentLinkedQueue<OccTransaction> firstTxs = new ConcurrentLinkedQueue<OccTransaction>();
		ConcurrentLinkedQueue<OccTransaction> otherTxs = new ConcurrentLinkedQueue<OccTransaction>();
		AtomicBoolean firstTxHasGenerated = new AtomicBoolean(false);
		int threadNum = 100;

		/*
		 * Test registerActiveTx The first transaction will write id_1, and the later
		 * transactions will read id_1. This test will intentionally commit the first
		 * transaction to make all later transactions abort.
		 */

		Thread[] threads = new Thread[threadNum];
		// new 10 threads to run this test
		for (int i = 0; i < threadNum; i++) {
			final long txNum = i;
			threads[i] = new Thread(new Runnable() {
				@Override
				public void run() {
					OccTransaction occTx = new OccTransaction(txNum + 1);
					occTx.setReadPhaseStartTime();

					Set<PrimaryKey> readSet = new HashSet<PrimaryKey>();
					Set<PrimaryKey> writeSet = new HashSet<PrimaryKey>();

					readSet.add(new PrimaryKey("tbl", "id", new IntegerConstant(1)));

					if (!firstTxHasGenerated.getAndSet(true)) {
						writeSet.add(new PrimaryKey("tbl", "id", new IntegerConstant(1)));
						firstTxs.add(occTx);
					} else {
						otherTxs.add(occTx);
					}

					occTx.setReadWriteSet(readSet, writeSet);

					occMgr.registerActiveTx(occTx);
				}
			});
			threads[i].start();
		}

		for (int i = 0; i < threadNum; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/*
		 * Validate the transactions in order.
		 */

		// validate the first transaction
		OccTransaction occTx = firstTxs.poll();
		try {
			occMgr.validationLock();
			Assert.assertTrue(occMgr.validate(occTx));
			occTx.setWritePhaseEndTime();
			occMgr.dropSelfFromActiveTxs(occTx);
		} catch (Exception e) {
			// do nothing
		} finally {
			occMgr.validationUnLock();
		}

		Iterator<OccTransaction> iterator = otherTxs.iterator();
		while (iterator.hasNext()) {
			occTx = iterator.next();

			try {
				occMgr.validationLock();
				Assert.assertFalse(occMgr.validate(occTx));
				Assert.assertFalse(occMgr.getActiveTxsStartTimes().contains(occTx.getReadPhaseStartTime()));
			} catch (Exception e) {
				// do nothing
			} finally {
				occMgr.validationUnLock();
			}
		}

		System.out.println("pass testOccMgr");
	}
}

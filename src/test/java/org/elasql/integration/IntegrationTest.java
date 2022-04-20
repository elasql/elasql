package org.elasql.integration;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.util.TransactionProfiler;

public class IntegrationTest {
	private final static int PSEUDO_CLIENT_ID = 0;
	private final static int ITGR_TEST_PROC_ID = 0;
	private final static int ITGR_TEST_VALIDATION_PROC_ID = 1;
	private final static int MAX_TESTING_TIME_IN_SECOND = 10;

	private static Logger logger = Logger.getLogger(IntegrationTest.class.getName());
	private ConcurrentLinkedQueue<Integer> completedTxs;
	private ConcurrentLinkedQueue<Integer> errorTxs;

	public static final int TABLE_ROW_NUM = 10;
	public static int TX_NUMS = 999;

	@BeforeClass
	public static void init() {
		ServerInit.init(IntegrationTest.class);
		ServerInit.loadTestBed();

		if (logger.isLoggable(Level.INFO)) {
			logger.info("Integration test begins");
		}
	}

	@Test
	public void testRecordCorrectness() {
		completedTxs = ServerInit.getCompletedTxsContainer();
		errorTxs = ServerInit.getErrorTxsContainer();

		scheduleSpCalls();

		waitForTxsCommit();

		if (logger.isLoggable(Level.INFO)) {
			logger.info("PASS integration test");
		}
	}

	void scheduleSpCalls() {
		for (int txId = 1; txId <= TX_NUMS; txId++) {
			if (txId % 10 == 0) {
				// schedule validation stored procedure when tx id is multiples of 10
				scheduleValidationSpCall(txId);
			} else {
				scheduleTestSpCall(txId);
			}
		}
	}

	void scheduleTestSpCall(int txId) {
		scheduleSpCall(ITGR_TEST_PROC_ID, txId);
	}

	void scheduleValidationSpCall(int txId) {
		scheduleSpCall(ITGR_TEST_VALIDATION_PROC_ID, txId);
	}

	void scheduleSpCall(int pid, int txId) {
		Object[] psuedoPars = new Object[10];

		StoredProcedureCall spc = new StoredProcedureCall(PSEUDO_CLIENT_ID, pid, psuedoPars);

		// set TxNum
		spc.setTxNum(txId);

		TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
		profiler.reset();
		profiler.startExecution();

		// set Profiler
		spc.setProfiler(TransactionProfiler.takeOut());

		Elasql.scheduler().schedule(spc);
	}

	void waitForTxsCommit() {
		long startTime = System.nanoTime();
		long elapsedTestTime = TimeUnit.SECONDS.convert(0, TimeUnit.NANOSECONDS);

		while (elapsedTestTime < MAX_TESTING_TIME_IN_SECOND) {
			if (completedTxs.size() == TX_NUMS) {
				// all txs are completed;
				break;
			}

			// should be
			Assert.assertTrue(errorTxs.isEmpty());

			// sleep for a while
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			elapsedTestTime = TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
		}

		Assert.assertTrue(completedTxs.size() == TX_NUMS);
	}
}

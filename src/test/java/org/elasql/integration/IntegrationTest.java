package org.elasql.integration;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.procedure.SpEndListener;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.util.TransactionProfiler;

/**
 * This is the main test file of integration test.
 * (Eclipse) To run the integration test, please follow the instructions.
 * 1. Click run configuration and new launch a JUnit test configuration.
 * 2. Set the test class to org.elasql.integration.IntegrationTest.
 * 3. Set the arguments:
 * 		-Djava.util.logging.config.file=src/test/resources/java/util/logging/logging.properties
 * 		-Dorg.vanilladb.core.config.file=src/test/resources/org/vanilladb/core/vanilladb.properties
 * 		-Dorg.elasql.config.file=src/test/resources/org/elasql/elasql.properties 
 * 
 * 4. Save and run.
 */
public class IntegrationTest implements SpEndListener {
	public final static int ITGR_TEST_PROC_ID = 0;
	public final static int ITGR_TEST_VALIDATION_PROC_ID = 1;

	private final static int PSEUDO_CLIENT_ID = -1;
	private final static int MAX_TESTING_TIME_IN_SECOND = 10;

	private static Logger logger = Logger.getLogger(IntegrationTest.class.getName());
	private ConcurrentLinkedQueue<Integer> completedTxs = new ConcurrentLinkedQueue<Integer>();
	private ConcurrentLinkedQueue<Integer> errorTxs = new ConcurrentLinkedQueue<Integer>();

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
		ServerInit.registerSpEndListener(this);

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

	@Override
	public void onSpCommit(int txNum) {
		completedTxs.add(txNum);
	}

	@Override
	public void onSpRollback(int txNum) {
		errorTxs.add(txNum);
	}
}

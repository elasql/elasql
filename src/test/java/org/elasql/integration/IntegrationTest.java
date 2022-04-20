package org.elasql.integration;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.util.TransactionProfiler;

public class IntegrationTest {
	private static Logger logger = Logger.getLogger(IntegrationTest.class.getName());
	private final static int PSEUDO_CLIENT_ID = 0;
	private final static int ITGR_TEST_PROC_ID = 0;
	private final static int ITGR_TEST_VALIDATION_PROC_ID = 1;
	private ConcurrentLinkedQueue<Integer> completedTxs;

	public static int TX_NUMS = 100;

	@BeforeClass
	public static void init() {
		ServerInit.init(IntegrationTest.class);
		ServerInit.loadTestBed();

		if (logger.isLoggable(Level.INFO)) {
			logger.info("Integration test begins");
		}
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

	void scheduleTestSpCalls() {
		for (int txId = 1; txId < TX_NUMS; txId++) {
			scheduleSpCall(ITGR_TEST_PROC_ID, txId);
		}
	}

	void scheduleValidationSpCall() {
		scheduleSpCall(ITGR_TEST_VALIDATION_PROC_ID, TX_NUMS + 1);
	}

	void waitForTxsCommit() {
		// Tx id starts from 1 to 999999
		while (completedTxs.size() != TX_NUMS - 1) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Amount of comitted transaction: " + completedTxs.size());
			try {

				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	void validateData() {
		scheduleValidationSpCall();

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Assert.assertTrue(completedTxs.size() == TX_NUMS);
	}

	@Test
	public void testRecordCorrectness() {
		completedTxs = ServerInit.getCompletedTxsContainer();

		scheduleTestSpCalls();

		waitForTxsCommit();

		validateData();

		if (logger.isLoggable(Level.INFO)) {
			logger.info("PASS integration test");
		}
	}
}

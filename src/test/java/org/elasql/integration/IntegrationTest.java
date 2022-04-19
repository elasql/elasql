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
	private final static int TX_NUMS = 100000;

	@BeforeClass
	public static void init() {
		ServerInit.init(IntegrationTest.class);
		ServerInit.loadTestBed();

		if (logger.isLoggable(Level.INFO))
			logger.info("Integration test begins");
	}

	@Test
	public void testRecordCorrectness() {
		ConcurrentLinkedQueue<Integer> completedTxs = ServerInit.getCompletedTxsContainer();
		
		Object[] psuedoPars = new Object[10];

		for (int txId = 0; txId < TX_NUMS; txId++) {
			StoredProcedureCall spc = new StoredProcedureCall(PSEUDO_CLIENT_ID, ITGR_TEST_PROC_ID, psuedoPars);

			// set TxNum
			spc.setTxNum(txId);

			TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
			profiler.reset();
			profiler.startExecution();

			// set Profiler
			spc.setProfiler(TransactionProfiler.takeOut());

			Elasql.scheduler().schedule(spc);
		}
		while (completedTxs.size() != TX_NUMS) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Amount of comitted transaction: " + completedTxs.size());
			try {
				
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		if (logger.isLoggable(Level.INFO))
			logger.info("PASS integration test");
	}
}

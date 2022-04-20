package org.elasql.integration;

import java.io.File;
import java.sql.Connection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.integration.procedure.ItgrTestStoredProcFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.server.Elasql;
import org.elasql.storage.metadata.HashPartitionPlan;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.query.planner.Planner;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.tx.Transaction;

public class ServerInit {
	private static Logger logger = Logger.getLogger(ServerInit.class.getName());

	static final int NODE_ID = 0;
	static final String DB_MAIN_DIR = "elasql_testdbs";
	static ConcurrentLinkedQueue<Integer> completedTxs = null;
	static ConcurrentLinkedQueue<Integer> errorTxs = null;

	static String resetDb(Class<?> testClass) {
		String testClassName = testClass.getName();
		String dbName = DB_MAIN_DIR + "/" + testClassName;

		// Creates the main directory if it was not created before
		File dbPath = new File(FileMgr.DB_FILES_DIR, DB_MAIN_DIR);
		if (!dbPath.exists())
			dbPath.mkdir();

		// Deletes the existing database
		deleteDB(dbName);

		return dbName;
	}

	private static void deleteDB(String dbName) {
		File dbPath = new File(FileMgr.DB_FILES_DIR, dbName);
		if (dbPath.exists()) {
			File[] files = dbPath.listFiles();
			for (File file : files) {
				if (!file.delete())
					throw new RuntimeException("cannot delete the file: " + file);
			}

			if (!dbPath.delete())
				throw new RuntimeException("cannot delete the directory: " + dbPath);
		}

		if (logger.isLoggable(Level.INFO)) {
			logger.info("Clean the database");
		}
	}

	private static TPartStoredProcedureFactory getTpartSpFactory() {
		return new ItgrTestStoredProcFactory();
	}

	static ConcurrentLinkedQueue<Integer> getCompletedTxsContainer() {
		return completedTxs;
	}
	
	static ConcurrentLinkedQueue<Integer> getErrorTxsContainer() {
		return errorTxs;
	}

	static void init(Class<?> testClass) {
		String dbName = resetDb(testClass);

		VanillaDb.init(dbName);

		if (logger.isLoggable(Level.INFO)) {
			logger.info("VanillaCore is initialized");
		}

		if (!VanillaDb.isInited()) {
			throw new RuntimeException("Vanillacore hasn't been initialized");
		}

		/*
		 * Enable testMode or we might get errors because lots of component depends on
		 * VanillComm, which won't be initialized during the test.
		 */
		Elasql.testMode = true;

		PartitionPlan partitionPlan = new HashPartitionPlan(1);

		Elasql.initCacheMgr();
		if (logger.isLoggable(Level.INFO)) {
			logger.info("ElaSQL.CacheMgr is initialized");
		}

		Elasql.initPartitionMetaMgr(partitionPlan);
		if (logger.isLoggable(Level.INFO)) {
			logger.info("ElaSQL.PartitionMetaMgr is initialized");
		}

		Elasql.initScheduler(getTpartSpFactory(), null);
		if (logger.isLoggable(Level.INFO)) {
			logger.info("ElaSQL.Scheduler is initialized");
		}

		Elasql.initDdLogMgr();
		if (logger.isLoggable(Level.INFO)) {
			logger.info("ElaSQL.DdLogMgr is initialized");
		}

		completedTxs = new ConcurrentLinkedQueue<Integer>();
		Elasql.setCompletedTxsContainer(completedTxs);
		
		errorTxs = new ConcurrentLinkedQueue<Integer>();
		Elasql.setErrorTxsContainer(errorTxs);

		if (logger.isLoggable(Level.INFO)) {
			logger.info("ElaSQL is initialized");
		}
	}

	static void loadTestBed() {
		if (logger.isLoggable(Level.INFO)) {
			logger.info("Testing data is loading");
		}

		Transaction tx = VanillaDb.txMgr().newTransaction(Connection.TRANSACTION_SERIALIZABLE, false);
		Planner planner = VanillaDb.newPlanner();

		// Create a table
		planner.executeUpdate("CREATE TABLE elasql_test_add (id INT, value INT, overflow INT)", tx);

		// Create an index
		planner.executeUpdate("CREATE INDEX id_idx ON elasql_test_add (id) USING BTREE", tx);

		// Insert a few records
		for (int i = 0; i < IntegrationTest.TABLE_ROW_NUM; i++) {
			String query = String.format("INSERT INTO elasql_test_add (id, value, overflow) VALUES (%s, 0, 0)", i);
			planner.executeUpdate(query, tx);
		}

		tx.commit();

		if (logger.isLoggable(Level.INFO)) {
			logger.info("Testing data has been loaded");
		}
	}
}

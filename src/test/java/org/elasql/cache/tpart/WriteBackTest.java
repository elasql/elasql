package org.elasql.cache.tpart;

import static org.junit.Assert.assertEquals;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.sql.RecordKey;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.tx.Transaction;

public class WriteBackTest {
	private static Logger logger = Logger.getLogger(WriteBackTest.class.getName());
	
	@BeforeClass
	public static void init() {
		// Uses Conservative Concurrency Control
		System.setProperty("org.vanilladb.core.storage.tx.TransactionMgr.SERIALIZABLE_CONCUR_MGR",
				"org.elasql.storage.tx.concurrency.ConservativeOrderedCcMgr");
		
		ServerInit.init(WriteBackTest.class);
		
		createTestingTable();
		
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN WRITEBACK TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH WRITEBACK TEST");
	}
	
	private static void createTestingTable() {
		CatalogMgr md = VanillaDb.catalogMgr();
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		
		// create and populate the course table
		Schema sch = new Schema();
		sch.addField("tid", INTEGER);
		sch.addField("tvalue", INTEGER);
		md.createTable("test", sch, tx);
	}
	
	private static RecordKey createRecordKey(int tid) {
		Map<String, Constant> fldValMap = new HashMap<String, Constant>();
		
		fldValMap.put("tid", new IntegerConstant(tid));
		
		return new RecordKey("test", fldValMap);
	}
	
	private static CachedRecord createCachedRecord(int tid, int tvalue) {
		Map<String, Constant> fldValMap = new HashMap<String, Constant>();
		
		fldValMap.put("tid", new IntegerConstant(tid));
		fldValMap.put("tvalue", new IntegerConstant(tvalue));
		
		return new CachedRecord(fldValMap);
	}
	
	/**
	 * <p>
	 * Sink 1 {Tx.1} <br>
	 * Sink 2 {Tx.2} <br>
	 * Sink 3 {Tx.3} <br>
	 * </p>
	 * <p>
	 * Tx.1: insert record A<br>
	 * Tx.2: read record A<br>
	 * Tx.3: read record A and update record A<br>
	 * </p>
	 * <p>
	 * Execution Flow: Tx.1 -> Tx.3 -> Tx.2<br>
	 * Note that the above flow is just one of possible flow 
	 * in parallel execution.<br>
	 * </p>
	 * <p>
	 * Tx.2 should read the version Tx.1 writes.
	 * </p>
	 */
	@Test
	public void testWriteback() {
		RecordKey commonKey = createRecordKey(1);
		WriteBackRecMgr writeBackMgr = new WriteBackRecMgr();
		
		// Set writeback info (in scheduler)
		writeBackMgr.setWriteBackInfo(commonKey, 1);
		writeBackMgr.setWriteBackInfo(commonKey, 3);
		
		// Tx.1 inserts a record
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false, 1);
		CachedRecord record = createCachedRecord(1, 10001);
		record.setNewInserted(true);
		writeBackMgr.writeBackRecord(commonKey, 1, record, tx);
		
		// Tx.3 reads and modify the record
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false, 3);
		record = writeBackMgr.read(commonKey, 3, tx);
		record.setVal("tvalue", new IntegerConstant(20001));
		writeBackMgr.writeBackRecord(commonKey, 3, record, tx);
		
		// Tx.2 reads the record
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false, 2);
		record = writeBackMgr.read(commonKey, 2, tx);
		
		assertEquals("bad writeback", 10001, record.getVal("tvalue"));
	}
}

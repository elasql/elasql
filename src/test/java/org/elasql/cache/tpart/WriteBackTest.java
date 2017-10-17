package org.elasql.cache.tpart;

import static org.junit.Assert.assertEquals;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
		final RecordKey commonKey = createRecordKey(1);
		final LocalStorageMgr localStorage = new LocalStorageMgr();
		final BlockingQueue<CachedRecord> returnQueue = new LinkedBlockingQueue<CachedRecord>();
		
		// Set sink reads and write-back info
		localStorage.requestSharedLock(commonKey, 2);
		localStorage.requestSharedLock(commonKey, 3);
		localStorage.requestExclusiveLock(commonKey, 1);
		localStorage.requestExclusiveLock(commonKey, 3);
		
		Thread[] threads = new Thread[3];
		
		// Tx.1 inserts a record
		threads[0] = new Thread(new Runnable() {
			@Override
			public void run() {
				Transaction tx = VanillaDb.txMgr().newTransaction(
						Connection.TRANSACTION_SERIALIZABLE, false, 1);
				CachedRecord record = createCachedRecord(1, 10001);
				record.setNewInserted(true);
				localStorage.writeBack(commonKey, record, tx);
			}
		});
		threads[0].start();
		
		
		// Tx.3 reads and modify the record
		threads[2] = new Thread(new Runnable() {
			@Override
			public void run() {
				Transaction tx = VanillaDb.txMgr().newTransaction(
						Connection.TRANSACTION_SERIALIZABLE, false, 3);
				CachedRecord record = localStorage.read(commonKey, tx);
				record.setVal("tvalue", new IntegerConstant(20001));
				localStorage.writeBack(commonKey, record, tx);
			}
		});
		threads[2].start();
		
		
		// Tx.2 reads the record
		threads[1] = new Thread(new Runnable() {
			@Override
			public void run() {
				Transaction tx = VanillaDb.txMgr().newTransaction(
						Connection.TRANSACTION_SERIALIZABLE, false, 2);
				CachedRecord record = localStorage.read(commonKey, tx);
				
				try {
					returnQueue.put(record);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		threads[1].start();
		
		try {
			assertEquals("bad writeback", 10001, returnQueue.take().getVal("tvalue").asJavaVal());
			
			// TODO: But we can not know if there is a exception happened in a thread
			for (Thread t : threads)
				t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}	
	}
}

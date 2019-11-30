package org.elasql.storage;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.index.Index.IDX_BTREE;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.CachedRecordBuilder;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.sql.RecordKey;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

import junit.framework.Assert;

public class VanillaCoreInsertTest {
	private static Logger logger = Logger.getLogger(VanillaCoreInsertTest.class
			.getName());
	
	private static final int STR_LEN = 33;
	
	private static CatalogMgr catMgr;

	@BeforeClass
	public static void init() {
		System.setProperty("org.vanilladb.core.storage.tx.TransactionMgr.SERIALIZABLE_CONCUR_MGR",
				"org.elasql.storage.tx.concurrency.ConservativeOrderedCcMgr");
		
		ServerInit.init(VanillaCoreInsertTest.class);
		RecoveryMgr.enableLogging(false);
		catMgr = VanillaDb.catalogMgr();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN VANILLA CORE INSERT TEST");
	}
	
	@AfterClass
	public static void finish() {
		RecoveryMgr.enableLogging(true);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH VANILLA CORE INSERT TEST");
	}
	
	@Test
	public void testRandomInsert() {
		// Create a data table and an index
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Schema schema = new Schema();
		String tblName = "random_table";
		String idFldName = "random_id";
		String dummyfldName = "random_fld";
		schema.addField(idFldName, INTEGER);
		schema.addField(dummyfldName, INTEGER);
		catMgr.createTable(tblName, schema, tx);
		catMgr.createIndex("random_index", tblName, idFldName, IDX_BTREE, tx);
		tx.commit();
		
		// Insert the records with continuous ids that do not starts form 1
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		for (int id = 10001; id <= 20000; id++) {
			RecordKey key = new RecordKey(tblName, idFldName, new IntegerConstant(id));
			CachedRecordBuilder builder = new CachedRecordBuilder(key);
			builder.addField(dummyfldName, new IntegerConstant(id));
			VanillaCoreCrud.insert(key, builder.build(), tx);
		}
		tx.commit();
		
		// Randomly insert records in front of previous records
		List<Integer> ids = new ArrayList<Integer>();
		for (int id = 1; id <= 10000; id++)
			ids.add(id);
		Collections.shuffle(ids);
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		for (Integer id : ids) {
			RecordKey key = new RecordKey(tblName, idFldName, new IntegerConstant(id));
			CachedRecordBuilder builder = new CachedRecordBuilder(key);
			builder.addField(dummyfldName, new IntegerConstant(id));
			VanillaCoreCrud.insert(key, builder.build(), tx);
		}
		tx.commit();

		// Verify all records are inserted
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		for (int id = 1; id <= 20000; id++) {
			RecordKey key = new RecordKey(tblName, idFldName, new IntegerConstant(id));
			CachedRecord rec = VanillaCoreCrud.read(key, tx);
			if (rec != null) {
				Assert.assertEquals("The RecordId for " + key + " is different", id, rec.getVal(dummyfldName).asJavaVal());
			} else {
				Assert.fail("Cannot find an index record for " + key);
			}
		}
		tx.commit();
	}
	
	@Test
	public void testStringRandomInsert() {
		// Create a data table and an index
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Schema schema = new Schema();
		String tblName = "str_random_table";
		String idFldName = "str_random_id";
		String dummyfldName = "str_random_fld";
		int strLen = 33;
		schema.addField(idFldName, VARCHAR(33));
		schema.addField(dummyfldName, VARCHAR(33));
		catMgr.createTable(tblName, schema, tx);
		catMgr.createIndex("str_random_index", tblName, idFldName, IDX_BTREE, tx);
		tx.commit();
		
		// Insert the records with continuous ids that do not starts form 1
		List<Thread> threads = new ArrayList<Thread>();
		List<Integer> ids = new ArrayList<Integer>();
		for (int id = 10001; id <= 20000; id++)
			ids.add(id);
		Thread thread = new Thread(new InsertJob(ids));
		thread.start();
		threads.add(thread);
		
		// Randomly insert records in front of previous records
		ids = new ArrayList<Integer>();
		for (int id = 1; id <= 10000; id++)
			ids.add(id);
		Collections.shuffle(ids);
		for (int tid = 0; tid < 1000; tid++) {
			List<Integer> subList = ids.subList(tid * 10, (tid + 1) * 10);
			thread = new Thread(new InsertJob(subList));
			thread.start();
			threads.add(thread);
		}
		
		// Wait for threads
		for (Thread t : threads)
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		// Verify all records are inserted
		tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		for (int id = 1; id <= 20000; id++) {
			String keyStr = String.format("%0" + strLen + "d", id);
			Constant idCon = new VarcharConstant(keyStr);
			RecordKey key = new RecordKey(tblName, idFldName, idCon);
			CachedRecord rec = VanillaCoreCrud.read(key, tx);
			if (rec != null) {
				int val = Integer.parseInt((String) rec.getVal(dummyfldName).asJavaVal());
				Assert.assertEquals("The value for " + key + " is different", id, val);
			} else {
				Assert.fail("Cannot find an index record for " + key);
			}
		}
		tx.commit();
	}
	
	static class InsertJob implements Runnable {
		
		List<Integer> ids;
		
		InsertJob(List<Integer> ids) {
			this.ids = ids;
		}

		@Override
		public void run() {
			Transaction tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			for (Integer id : ids) {
				String keyStr = String.format("%0" + STR_LEN + "d", id);
				Constant idCon = new VarcharConstant(keyStr);
				RecordKey key = new RecordKey("str_random_table", "str_random_id", idCon);
				CachedRecordBuilder builder = new CachedRecordBuilder(key);
				builder.addField("str_random_fld", idCon);
				VanillaCoreCrud.insert(key, builder.build(), tx);
			}
			tx.commit();
		}
		
	}
}

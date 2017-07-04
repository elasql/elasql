/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.elasql.procedure.naive;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.naive.NaiveCacheMgr;
import org.elasql.procedure.DdStoredProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.tx.concurrency.ConservativeOrderedCcMgr;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;

public abstract class NaiveStoredProcedure<H extends StoredProcedureParamHelper>
		implements DdStoredProcedure {

	// Protected resource
	protected Transaction tx;
	protected long txNum;
	protected H paramHelper;

	// Record keys
	private List<RecordKey> readKeys = new ArrayList<RecordKey>();
	private List<RecordKey> writeKeys = new ArrayList<RecordKey>();
	
	private NaiveCacheMgr cacheMgr = (NaiveCacheMgr) Elasql.remoteRecReceiver();
	
	public NaiveStoredProcedure(long txNum, H paramHelper) {
		this.txNum = txNum;
		this.paramHelper = paramHelper;

		if (paramHelper == null)
			throw new NullPointerException("paramHelper should not be null");
	}

	/*******************
	 * Abstract methods
	 *******************/

	/**
	 * Prepare the RecordKey for each record to be used in this stored
	 * procedure. Use the {@link #addReadKey(RecordKey)},
	 * {@link #addWriteKey(RecordKey)} method to add keys.
	 */
	protected abstract void prepareKeys();
	
	/**
	 * Perform the transaction logic and record the result of the transaction.
	 */
	protected abstract void performTransactionLogic();

	
	/**********************
	 * Implemented methods
	 **********************/

	public void prepare(Object... pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// create transaction
		boolean isReadOnly = paramHelper.isReadOnly();
		this.tx = Elasql.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, isReadOnly, txNum);
		this.tx.addLifecycleListener(new DdRecoveryMgr(tx
				.getTransactionNumber()));

		// prepare keys
		prepareKeys();
	}

	public void requestConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.bookReadKeys(readKeys);
		ccMgr.bookWriteKeys(writeKeys);
	}

	@Override
	public SpResultSet execute() {
		
		try {
			// Get conservative locks it has asked before
			getConservativeLocks();

			// Execute transaction
			performTransactionLogic();

			// The transaction finishes normally
			tx.commit();

		} catch (Exception e) {
			tx.rollback();
			paramHelper.setCommitted(false);
			e.printStackTrace();
		}

		return paramHelper.createResultSet();
	}
	
	@Override
	public boolean isReadOnly() {
		return paramHelper.isReadOnly();
	}
	
	protected void addReadKey(RecordKey readKey) {
		readKeys.add(readKey);
	}

	protected void addWriteKey(RecordKey writeKey) {
		writeKeys.add(writeKey);
	}
	
	protected CachedRecord read(RecordKey key) {
		return cacheMgr.read(key, tx);
	}
	
	protected void update(RecordKey key, CachedRecord rec) {
		cacheMgr.update(key, rec, tx);
	}
	
	protected void insert(RecordKey key, Map<String, Constant> fldVals) {
		cacheMgr.insert(key, new CachedRecord(fldVals), tx);
	}
	
	protected void delete(RecordKey key) {
		cacheMgr.delete(key, tx);
	}
	
	private void getConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.requestLocks(false);
	}
}

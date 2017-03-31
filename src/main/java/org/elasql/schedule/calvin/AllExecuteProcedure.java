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
package org.elasql.schedule.calvin;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.NotificationPartMetaMgr;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public abstract class AllExecuteProcedure<H extends StoredProcedureParamHelper>
		extends CalvinStoredProcedure<H> {
	private static Logger logger = Logger.getLogger(AllExecuteProcedure.class
			.getName());

	private static final String KEY_FINISH = "finish";
	
	private static final int MASTER_NODE = 0;

	public AllExecuteProcedure(long txNum, H paramHelper) {
		super(txNum, paramHelper);
	}

	/**
	 * Only return true in order to force all nodes to participate.
	 * 
	 * @return true
	 */
	public boolean isParticipated() {
		return true;
	}

	@Override
	protected void prepareKeys() {
		// Do nothing
	}

	@Override
	protected int decideMaster() {
		return MASTER_NODE;
	}

	@Override
	protected void executeSQL(Map<RecordKey, CachedRecord> readings) {
		// Do nothing
	}

	@Override
	protected void masterCollectResults(Map<RecordKey, CachedRecord> readings) {
		// Do nothing
	}

	protected void executeTransactionLogic() {
		executeSql();

		// Notification for finish
		if (isMasterNode()) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Waiting for other servers...");

			// Master: Wait for notification from other nodes
			waitForNotification();
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Other servers completion comfirmed.");
		} else {
			// Salve: Send notification to the master
			sendNotification();
		}
	}

	protected abstract void executeSql();

	private void waitForNotification() {
		// Wait for notification from other nodes
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++)
			if (nodeId != MASTER_NODE) {
				if (logger.isLoggable(Level.FINE))
					logger.fine("Waiting for the notification from node no." + nodeId);
				
				RecordKey notKey = NotificationPartMetaMgr.createRecordKey(nodeId, MASTER_NODE);
				CachedRecord rec = cacheMgr.readFromRemote(notKey);
				Constant con = rec.getVal(KEY_FINISH);
				int value = (int) con.asJavaVal();
				if (value != 1)
					throw new RuntimeException(
							"Notification value error, node no." + nodeId
									+ " sent " + value);

				if (logger.isLoggable(Level.FINE))
					logger.fine("Receive notification from node no." + nodeId);
			}
	}

	private void sendNotification() {
		// Create a key value set
		Map<String, Constant> fldVals = new HashMap<String, Constant>();
		fldVals.put(KEY_FINISH, new IntegerConstant(1));
		
		RecordKey notKey = NotificationPartMetaMgr.createRecordKey(Elasql.serverId(), MASTER_NODE);
		CachedRecord notVal = NotificationPartMetaMgr.createRecord(Elasql.serverId(), MASTER_NODE,
				txNum, fldVals);

		TupleSet ts = new TupleSet(-1);
		// Use node id as source tx number
		ts.addTuple(notKey, txNum, txNum, notVal);
		Elasql.connectionMgr().pushTupleSet(0, ts);

		if (logger.isLoggable(Level.FINE))
			logger.fine("The notification is sent to the master by tx." + txNum);
	}
}

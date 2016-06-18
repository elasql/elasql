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
import org.elasql.cache.calvin.CalvinCacheMgr;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public abstract class AllExecuteProcedure<H extends StoredProcedureParamHelper>
		extends CalvinStoredProcedure<H> {
	private static Logger logger = Logger.getLogger(AllExecuteProcedure.class
			.getName());

	private static final String NOTIFICATION_FILED_NAME = "finish";

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
		// The first node be the master node
		return 0;
	}

	@Override
	protected void onLocalReadCollected(
			Map<RecordKey, CachedRecord> localReadings) {
		// Do nothing
	}

	@Override
	protected void onRemoteReadCollected(
			Map<RecordKey, CachedRecord> remoteReadings) {
		// Do nothing
	}

	@Override
	protected void writeRecords(Map<RecordKey, CachedRecord> readings) {
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
		CalvinCacheMgr cm = (CalvinCacheMgr) Elasql.cacheMgr();

		// Wait for notification from other nodes
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++)
			if (nodeId != Elasql.serverId()) {
				RecordKey notKey = getFinishNotificationKey(nodeId);
				CachedRecord rec = cm.read(notKey, txNum, tx, false);
				Constant con = rec.getVal(NOTIFICATION_FILED_NAME);
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
		RecordKey notKey = getFinishNotificationKey(Elasql.serverId());
		CachedRecord notVal = getFinishNotificationValue(txNum);

		TupleSet ts = new TupleSet(-1);
		// Use node id as source tx number
		ts.addTuple(notKey, txNum, txNum, notVal);
		Elasql.connectionMgr().pushTupleSet(0, ts);

		if (logger.isLoggable(Level.FINE))
			logger.fine("The notification is sent to the master by tx." + txNum);
	}

	private RecordKey getFinishNotificationKey(int nodeId) {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put(NOTIFICATION_FILED_NAME, new IntegerConstant(nodeId));
		return new RecordKey("notification", keyEntryMap);
	}

	private CachedRecord getFinishNotificationValue(long txNum) {
		// Create key value sets
		Map<String, Constant> fldVals = new HashMap<String, Constant>();
		fldVals.put(NOTIFICATION_FILED_NAME, new IntegerConstant(1));

		// Create a record
		CachedRecord rec = new CachedRecord(fldVals);
		rec.setSrcTxNum(txNum);
		return rec;
	}
}

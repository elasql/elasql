/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.procedure.calvin;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.calvin.ExecutionPlan;
import org.elasql.schedule.calvin.ExecutionPlan.ParticipantRole;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.schedule.calvin.StandardAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.NotificationPartitionPlan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public abstract class AllExecuteProcedure<H extends StoredProcedureParamHelper>
		extends CalvinStoredProcedure<H> {
	private static Logger logger = Logger.getLogger(AllExecuteProcedure.class
			.getName());

	private static final String KEY_FINISH = "finish";
	
	private static final int MASTER_NODE = 0;

	private int localNodeId = Elasql.serverId();
	private int numOfParts;

	public AllExecuteProcedure(long txNum, H paramHelper) {
		super(txNum, paramHelper);
		
		numOfParts = Elasql.partitionMetaMgr().getCurrentNumOfParts();
	}
	
	protected ExecutionPlan analyzeParameters(Object[] pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// analyze read-write set
		ReadWriteSetAnalyzer analyzer;
		if (Elasql.migrationMgr().isInMigration())
			analyzer = Elasql.migrationMgr().newAnalyzer();
		else
			analyzer = new StandardAnalyzer();
		prepareKeys(analyzer);
		
		// generate execution plan
		return alterExecutionPlan(analyzer.generatePlan());
	}
	
	public boolean willResponseToClients() {
		// The master node is the only one that will response to the clients.
		return localNodeId == MASTER_NODE;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		// default: do nothing
	}
	
	private ExecutionPlan alterExecutionPlan(ExecutionPlan plan) {
		if (localNodeId == MASTER_NODE) {
			for (int nodeId = 0; nodeId < numOfParts; nodeId++)
				plan.addRemoteReadKey(NotificationPartitionPlan.createRecordKey(nodeId, MASTER_NODE));
		}
		plan.setParticipantRole(ParticipantRole.ACTIVE);
		plan.setForceReadWriteTx();
		
		return plan;
	}

	protected void executeTransactionLogic() {
		executeSql(null);

		// Notification for finish
		if (localNodeId == MASTER_NODE) {
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

	private void waitForNotification() {
		// Wait for notification from other nodes
		for (int nodeId = 0; nodeId < numOfParts; nodeId++)
			if (nodeId != MASTER_NODE) {
				if (logger.isLoggable(Level.FINE))
					logger.fine("Waiting for the notification from node no." + nodeId);
				
				RecordKey notKey = NotificationPartitionPlan.createRecordKey(nodeId, MASTER_NODE);
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
		
		RecordKey notKey = NotificationPartitionPlan.createRecordKey(Elasql.serverId(), MASTER_NODE);
		CachedRecord notVal = NotificationPartitionPlan.createRecord(Elasql.serverId(), MASTER_NODE,
				txNum, fldVals);

		TupleSet ts = new TupleSet(-1);
		// Use node id as source tx number
		ts.addTuple(notKey, txNum, txNum, notVal);
		Elasql.connectionMgr().pushTupleSet(0, ts);

		if (logger.isLoggable(Level.FINE))
			logger.fine("The notification is sent to the master by tx." + txNum);
	}
}

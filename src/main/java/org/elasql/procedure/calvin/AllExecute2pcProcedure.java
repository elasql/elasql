package org.elasql.procedure.calvin;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.calvin.ExecutionPlan;
import org.elasql.schedule.calvin.ExecutionPlan.ParticipantRole;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.schedule.calvin.StandardAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.NotificationPartitionPlan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.ManuallyAbortException;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

/**
 * A stored procedure that executes its logic on all the machines and performs
 * a two phase commit to ensure consistency at the end.<br />
 * <br />
 * A deterministic database system usually does not need to perform two phase
 * commit to ensure consistency. However, if it is possible for a procedure
 * to perform non-deterministic actions that might cause aborts, two phase
 * commit will be needed.
 * 
 * @author SLMT
 *
 * @param <H>
 */
public abstract class AllExecute2pcProcedure<H extends StoredProcedureParamHelper>
		extends CalvinStoredProcedure<H> {
	private static Logger logger = Logger.getLogger(AllExecuteProcedure.class.getName());

	private static final String FIELD_DECISION = "decision";
	private static final String FIELD_MESSAGE = "message";
	private static final IntegerConstant COMMIT = new IntegerConstant(0);
	private static final IntegerConstant ABORT = new IntegerConstant(1);

	private static final int MASTER_NODE = 0;

	private int localNodeId = Elasql.serverId();
	private int numOfParts;
	private RuntimeException abortCause;
	private String abortMessage = "";

	public AllExecute2pcProcedure(long txNum, H paramHelper) {
		super(txNum, paramHelper);

		numOfParts = Elasql.partitionMetaMgr().getCurrentNumOfParts();
	}

	@Override
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

	@Override
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
		} else {
			plan.addRemoteReadKey(NotificationPartitionPlan.createRecordKey(MASTER_NODE, Elasql.serverId()));
		}
		plan.setParticipantRole(ParticipantRole.ACTIVE);
		plan.setForceReadWriteTx();

		return plan;
	}

	@Override
	protected void executeTransactionLogic() {
		try {
			executeSql(null);
		} catch (RuntimeException e) {
			abortCause = e;
			if (e.getMessage() != null && !e.getMessage().isEmpty())
				abortMessage = e.getMessage();
		}

		boolean finalDecision = performTwoPhaseCommit();
		if (!finalDecision) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("Aborts the transaction");
			
			if (abortCause != null)
				throw abortCause;
			else
				throw new ManuallyAbortException(abortMessage);
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Commits the transaction.");
	}
	
	private boolean performTwoPhaseCommit() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Performing two phase commit...");
		
		// Two Phase Commit
		boolean decision = (abortCause == null);
		decision = performPhaseOne(decision);
		decision = performPhaseTwo(decision);
		return decision;
	}
	
	private boolean performPhaseOne(boolean isCommitted) {
		if (localNodeId == MASTER_NODE) {
			// Master node: wait for all the decisions
			isCommitted = masterWaitForDecisions(isCommitted);
			
			if (logger.isLoggable(Level.INFO))
				logger.info("The final decision is: " + (isCommitted? "Commit": "Abort"));
		} else {
			// Other node: send its decision
			otherSendNotification(isCommitted);
		}
		return isCommitted;
	}
	
	private boolean performPhaseTwo(boolean finalDecision) {
		if (localNodeId == MASTER_NODE) {
			// Master node: send the final decision
			masterSendFianlDecision(finalDecision);
		} else {
			// Other node: wait for the final decision
			finalDecision = otherReceiveFinalDecision();
		}
		return finalDecision;
	}

	private boolean masterWaitForDecisions(boolean initalDecision) {
		if (!initalDecision) {
			if (abortMessage.isEmpty())
				abortMessage = "aborted by node 0";
			else
				abortMessage = String.format("aborted by node 0: %s", abortMessage);
		}
		
		// Wait for decisions
		for (int nodeId = 0; nodeId < numOfParts; nodeId++)
			if (nodeId != MASTER_NODE) {
				if (logger.isLoggable(Level.FINE))
					logger.fine("Waiting for the decision from node no." + nodeId);

				PrimaryKey notKey = NotificationPartitionPlan.createRecordKey(nodeId, MASTER_NODE);
				CachedRecord rec = cacheMgr.readFromRemote(notKey);
				Constant con = rec.getVal(FIELD_DECISION);
				boolean isCommitted = con.equals(COMMIT);
				if (initalDecision && !isCommitted) {
					initalDecision = false;
					String message = rec.getVal(FIELD_MESSAGE).toString();
					if (message.isEmpty())
						abortMessage = String.format("aborted by node %d", nodeId);
					else
						abortMessage = String.format("aborted by node %d: %s", nodeId, message);
				}
				
				if (logger.isLoggable(Level.FINE))
					logger.fine("Receive the decision from node no." + nodeId);
			}
		return initalDecision;
	}

	private void otherSendNotification(boolean isCommitted) {
		// Create a key value set
		PrimaryKey notKey = NotificationPartitionPlan.createRecordKey(Elasql.serverId(), MASTER_NODE);
		CachedRecord notVal = NotificationPartitionPlan.createRecord(Elasql.serverId(), MASTER_NODE, txNum);
		notVal.addFldVal(FIELD_DECISION, (isCommitted? COMMIT : ABORT));
		notVal.addFldVal(FIELD_MESSAGE, new VarcharConstant(abortMessage));

		TupleSet ts = new TupleSet(-1);
		// Use node id as source tx number
		ts.addTuple(notKey, txNum, txNum, notVal);
		Elasql.connectionMgr().pushTupleSet(MASTER_NODE, ts);

		if (logger.isLoggable(Level.FINE))
			logger.fine("The decision is sent to the master by tx." + txNum);
	}
	
	private void masterSendFianlDecision(boolean finalDecision) {
		// Send decisions
		for (int nodeId = 0; nodeId < numOfParts; nodeId++)
			if (nodeId != MASTER_NODE) {
				if (logger.isLoggable(Level.FINE))
					logger.fine("Sending the final decision to node no." + nodeId);

				// Create a key value set
				PrimaryKey notKey = NotificationPartitionPlan.createRecordKey(MASTER_NODE, nodeId);
				CachedRecord notVal = NotificationPartitionPlan.createRecord(MASTER_NODE, nodeId, txNum);
				notVal.addFldVal(FIELD_DECISION, (finalDecision? COMMIT : ABORT));
				notVal.addFldVal(FIELD_MESSAGE, new VarcharConstant(abortMessage));

				TupleSet ts = new TupleSet(-1);
				// Use node id as source tx number
				ts.addTuple(notKey, txNum, txNum, notVal);
				Elasql.connectionMgr().pushTupleSet(nodeId, ts);
			}
	}
	
	private boolean otherReceiveFinalDecision() {
		// Create a key value set
		PrimaryKey notKey = NotificationPartitionPlan.createRecordKey(MASTER_NODE, Elasql.serverId());
		CachedRecord rec = cacheMgr.readFromRemote(notKey);
		boolean isCommitted = rec.getVal(FIELD_DECISION).equals(COMMIT);
		abortMessage = rec.getVal(FIELD_MESSAGE).toString();

		if (logger.isLoggable(Level.FINE))
			logger.fine("The decision is sent to the master by tx." + txNum);
		
		return isCommitted;
	}
}

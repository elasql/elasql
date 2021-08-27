package org.elasql.procedure.tpart;

import java.util.Set;

import org.elasql.procedure.StoredProcedureTask;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.util.Timer;

public class TPartStoredProcedureTask
		extends StoredProcedureTask<TPartStoredProcedure<?>> {
	
	static {
		// For Debugging
//		TimerStatistics.startReporting();
	}
	
	private static long firstTxStartTime;
	
	public static void setFirstTxStartTime(long firstTxStartTime) {
		TPartStoredProcedureTask.firstTxStartTime = firstTxStartTime;
	}

	private TPartStoredProcedure<?> tsp;
	private int clientId, connectionId, parId;
	private long txNum;
	
	// Timestamps
	// The time that the stored procedure call arrives the system
	private long arrivedTime;
	private long planGenStartTime;
	private long planGenStopTime;
	private long threadInitStartTime;

	public TPartStoredProcedureTask(int cid, int connId, long txNum, long arrivedTime, TPartStoredProcedure<?> sp) {
		super(cid, connId, txNum, sp);
		this.clientId = cid;
		this.connectionId = connId;
		this.txNum = txNum;
		this.arrivedTime = arrivedTime;
		this.tsp = sp;		
	}

	@Override
	public void run() {
		SpResultSet rs = null;
		
		Thread.currentThread().setName("Tx." + txNum);
		
		// Initialize a thread-local timer
		Timer timer = Timer.getLocalTimer();
		timer.reset();
		
		// XXX: since we do not count OU0 for now,
		// so we use the start time of OU1 as the transaction start time.
		timer.setStartExecutionTime(planGenStartTime);
//		timer.startExecution();
		
		// OU1
		timer.startComponentTimer("OU1 - Generate Plan", planGenStartTime);
		timer.stopComponentTimer("OU1 - Generate Plan", planGenStopTime);
		
		// OU2
		timer.startComponentTimer("OU2 - Initialize Thread", threadInitStartTime);
		timer.stopComponentTimer("OU2 - Initialize Thread");
		
		// Transaction Execution
		rs = tsp.execute();

		// Stop the timer for the whole execution
		timer.stopExecution();
		
		if (tsp.isMaster()) {
			if (clientId != -1)
				Elasql.connectionMgr().sendClientResponse(clientId, connectionId, txNum, rs);

			// TODO: Uncomment this when the migration module is migrated
//			if (tsp.getProcedureType() == ProcedureType.MIGRATION) {
//				// Send a notification to the sequencer
//				TupleSet ts = new TupleSet(MigrationMgr.MSG_COLD_FINISH);
//				Elasql.connectionMgr().pushTupleSet(PartitionMetaMgr.NUM_PARTITIONS, ts);
//			}
			
			// For Debugging
//			timer.addToGlobalStatistics();
		}
		
		// Record the timer result
		String role = tsp.isMaster()? "Master" : "Slave";
		Elasql.performanceMgr().addTransactionMetics(txNum, role, timer);
	}

	public long getTxNum() {
		return txNum;
	}
	
	public long getArrivedTime() {
		return arrivedTime;
	}

	public Set<PrimaryKey> getReadSet() {
		return tsp.getReadSet();
	}

	public Set<PrimaryKey> getWriteSet() {
		return tsp.getWriteSet();
	}

	public double getWeight() {
		return tsp.getWeight();
	}

	public int getPartitionId() {
		return parId;
	}

	public void setPartitionId(int parId) {
		this.parId = parId;
	}

	public void decideExceutionPlan(SunkPlan plan) {
		tsp.decideExceutionPlan(plan);
	}

	public TPartStoredProcedure<?> getProcedure() {
		return tsp;
	}

	public ProcedureType getProcedureType() {
		if (tsp == null)
			return ProcedureType.NOP;
		return tsp.getProcedureType();
	}

	public boolean isReadOnly() {
		return tsp.isReadOnly();
	}
	
	public void recordPlanGenerationStart() {
		planGenStartTime = System.nanoTime();
	}
	
	public void recordPlanGenerationStop() {
		planGenStopTime = System.nanoTime();
	}
	
	public void recordThreadInitStart() {
		threadInitStartTime = System.nanoTime();
	}
}

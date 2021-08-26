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
	// The time that the stored procedure call arrives the system
	private long arrivedTime;
	private long txStartTime, sinkStartTime, sinkStopTime, threadInitStartTime;

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
		timer.startExecution();
		// MODIFIED:
		timer.recordTime("Txn Start TimeStamp", System.nanoTime() / 1000);

		// MODIFIED: Cannot be fixed by merge conflict
		// timer.setStartExecutionTime(txStartTime);
		// timer.startComponentTimer("Generate plan", sinkStartTime);
		// timer.stopComponentTimer("Generate plan", sinkStopTime);
		// timer.startComponentTimer("Init thread", threadInitStartTime);
		// timer.stopComponentTimer("Init thread");
//		timer.startExecution();
		
		rs = tsp.execute();
			
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
		
		// Stop the timer for the whole execution
		timer.stopExecution();
		
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

	public void setStartTime(long txStartTime, long sinkStartTime, long sinkStopTime, long threadInitStartTime) {
		this.txStartTime = txStartTime;
		this.sinkStartTime = sinkStartTime;
		this.sinkStopTime = sinkStopTime;
		this.threadInitStartTime = threadInitStartTime;
	}
}

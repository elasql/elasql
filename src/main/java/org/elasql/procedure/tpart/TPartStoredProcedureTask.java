package org.elasql.procedure.tpart;

import java.util.Set;

import org.elasql.perf.tpart.ai.TransactionEstimation;
import org.elasql.procedure.StoredProcedureTask;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.util.TransactionProfiler;

public class TPartStoredProcedureTask
		extends StoredProcedureTask<TPartStoredProcedure<?>> {
	
	static {
		// For Debugging
//		TimerStatistics.startReporting();
	}

	private TPartStoredProcedure<?> tsp;
	private int clientId, connectionId, parId;
	private long txNum;
	
	// Timestamps
	// The time that the stored procedure call arrives the system
	private long arrivedTime;
	private TransactionEstimation estimation;
	private TransactionProfiler profiler;

	public TPartStoredProcedureTask(int cid, int connId, long txNum, long arrivedTime,
			TransactionProfiler profiler, TPartStoredProcedure<?> sp, TransactionEstimation estimation) {
		super(cid, connId, txNum, sp);
		this.clientId = cid;
		this.connectionId = connId;
		this.txNum = txNum;
		this.arrivedTime = arrivedTime;
		this.profiler = profiler;
		this.tsp = sp;
		this.estimation = estimation;
	}

	@Override
	public void run() {
		SpResultSet rs = null;
		
		Thread.currentThread().setName("Tx." + txNum);
		
		// Initialize a thread-local profiler which is from scheduler
		TransactionProfiler.setProfiler(profiler);
		TransactionProfiler profiler =  TransactionProfiler.getLocalProfiler();

		// OU2
		profiler.stopComponentProfiler("OU2 - Initialize Thread");
		
		// Transaction Execution
		rs = tsp.execute();

		// Stop the profiler for the whole execution
		profiler.stopExecution();
		
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
		
		// Record the profiler result
		String role = tsp.isMaster()? "Master" : "Slave";
		
		Elasql.performanceMgr().addTransactionMetics(txNum, role, tsp.isTxDistributed(), profiler);
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
	
	public Set<PrimaryKey> getUpdateSet() {
		return tsp.getUpdateSet();
	}
	
	public Set<PrimaryKey> getInsertSet() {
		return tsp.getInsertSet();
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

	public TransactionProfiler getTxProfiler() {
		return profiler;
	}
	
	public ProcedureType getProcedureType() {
		if (tsp == null)
			return ProcedureType.NOP;
		return tsp.getProcedureType();
	}

	public boolean isReadOnly() {
		return tsp.isReadOnly();
	}
	
	public void setEstimation(TransactionEstimation estimation) {
		this.estimation = estimation;
	}
	
	public TransactionEstimation getEstimation() {
		return estimation;
	}
}

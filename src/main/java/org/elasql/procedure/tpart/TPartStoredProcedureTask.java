package org.elasql.procedure.tpart;

import java.util.Set;

import org.elasql.procedure.StoredProcedureTask;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.util.TransactionStatisticsRecorder;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.util.Timer;

public class TPartStoredProcedureTask
		extends StoredProcedureTask<TPartStoredProcedure<?>> {
	
	private static class WaitingForStartingRecordTask extends Task {
//		private static final int WARM_UP_TIME = 100_000;
		private static final int WARM_UP_TIME = 0; // in milliseconds
		@Override
		public void run() {
			try {
				Thread.sleep(WARM_UP_TIME);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			TransactionStatisticsRecorder.startRecording();
		}
	}
	
	static {
		// For Debugging
//		TimerStatistics.startReporting();
//		TransactionStatisticsRecorder.startRecording();
		VanillaDb.taskMgr().runTask(new WaitingForStartingRecordTask());
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
		TransactionStatisticsRecorder.recordResult(txNum, timer);
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

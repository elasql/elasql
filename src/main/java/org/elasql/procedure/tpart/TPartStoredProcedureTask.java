package org.elasql.procedure.tpart;

import java.util.Set;

import org.elasql.procedure.StoredProcedureTask;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.util.FeatureCollector;
import org.elasql.util.TransactionCpuTimeRecorder;
import org.elasql.util.TransactionFeaturesRecorder;
import org.elasql.util.TransactionStatisticsRecorder;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.util.ThreadMXBean;
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
			TransactionCpuTimeRecorder.startRecording();
			TransactionFeaturesRecorder.startRecording();
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
	private long txStartTime, sinkStartTime, sinkStopTime, threadInitStartTime;
	private long sinkCpuStartTime, sinkCpuStopTime, threadInitCpuStartTime;

	public TPartStoredProcedureTask(int cid, int connId, long txNum, TPartStoredProcedure<?> sp) {
		super(cid, connId, txNum, sp);
		this.clientId = cid;
		this.connectionId = connId;
		this.txNum = txNum;
		this.tsp = sp;		
	}

	@Override
	public void run() {
		SpResultSet rs = null;
		
		Thread.currentThread().setName("Tx." + txNum);
		
		// Initialize a thread-local timer
		Timer timer = Timer.getLocalTimer();
//		CpuTimer cpuTimer = CpuTimer.getLocalTimer();
		timer.reset();
		timer.setStartExecutionTime(txStartTime);
		timer.startComponentTimer("Generate plan", sinkStartTime);
		timer.stopComponentTimer("Generate plan", sinkStopTime);
		timer.startComponentTimer("Init thread", threadInitStartTime);
		timer.stopComponentTimer("Init thread");
//		timer.startExecution();
		
		// Initialize a thread-local CPU timer
		Timer cpuTimer = Timer.getLocalCpuTimer();
		cpuTimer.reset();
		cpuTimer.setStartExecutionTime(ThreadMXBean.getCpuTime()); //set tx CPU start time for current thread
		cpuTimer.startComponentTimer("Generate plan", sinkCpuStartTime);
		cpuTimer.stopComponentTimer("Generate plan", sinkCpuStopTime);
		cpuTimer.startComponentTimer("Init thread", threadInitCpuStartTime);
		cpuTimer.stopComponentTimer("Init thread",ThreadMXBean.getCpuTime());
		
		// Initialize a thread-local feature collector
		FeatureCollector collector = FeatureCollector.getLocalFeatureCollector();
		collector.setFeatureValue(FeatureCollector.keys[0], (txStartTime - firstTxStartTime)/1000);
		collector.setFeatureValue(FeatureCollector.keys[1], tsp.getReadSet());
		collector.setFeatureValue(FeatureCollector.keys[2], tsp.getWriteSet());
		// Record the feature result
		TransactionFeaturesRecorder.recordResult(txNum, collector);
		
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
		
		cpuTimer.setStopExecutionTime(ThreadMXBean.getCpuTime());
		// Stop the timer for the whole execution
		timer.stopExecution();
		
		// Record the timer result
		TransactionStatisticsRecorder.recordResult(txNum, timer);
		TransactionCpuTimeRecorder.recordResult(txNum, cpuTimer);
	}

	public long getTxNum() {
		return txNum;
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
	public void setCpuStartTime(long sinkCpuStartTime, long sinkCpuStopTime, long threadInitCpuStartTime) {
		this.sinkCpuStartTime = sinkCpuStartTime;
		this.sinkCpuStopTime = sinkCpuStopTime;
		this.threadInitCpuStartTime = threadInitCpuStartTime;
	}
}

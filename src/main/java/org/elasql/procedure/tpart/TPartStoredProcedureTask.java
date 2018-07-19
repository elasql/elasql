package org.elasql.procedure.tpart;

import java.util.Set;

import org.elasql.migration.MigrationMgr;
import org.elasql.procedure.StoredProcedureTask;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class TPartStoredProcedureTask extends StoredProcedureTask {

	private TPartStoredProcedure<?> tsp;
	private int clientId, connectionId, parId;
	private long txNum;

	public TPartStoredProcedureTask(int cid, int connId, long txNum, TPartStoredProcedure<?> sp) {
		super(cid, connId, txNum, sp);
		this.clientId = cid;
		this.connectionId = connId;
		this.txNum = txNum;
		this.tsp = sp;
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Tx." + txNum);
		
		//Timers.createTimer(txNum);
		SpResultSet rs = null;
		//Timers.getTimer().startExecution();

		// try {
		// long start = System.nanoTime();
		rs = tsp.execute();
		// long time = System.nanoTime() - start;
		// System.out.println(time / 1000);
		// } finally {
		//Timers.getTimer().stopExecution();
		// }

		if (tsp.isMaster()) {
			if (clientId != -1)
				Elasql.connectionMgr().sendClientResponse(clientId, connectionId, txNum, rs);
			
			if (tsp.getProcedureType() == ProcedureType.MIGRATION) {
				// Send a notification to the sequencer
				TupleSet ts = new TupleSet(MigrationMgr.MSG_COLD_FINISH);
				Elasql.connectionMgr().pushTupleSet(PartitionMetaMgr.NUM_PARTITIONS, ts);
			}
				
			// System.out.println("Commit: " + (System.nanoTime() - startTime));
		}
		// System.out.println("task time:" + (System.nanoTime() -
		// taskStartTime));
		//Timers.addToStatstics();
	}

	public long getTxNum() {
		return txNum;
	}

	public Set<RecordKey> getReadSet() {
		return tsp.getReadSet();
	}

	public Set<RecordKey> getWriteSet() {
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
}

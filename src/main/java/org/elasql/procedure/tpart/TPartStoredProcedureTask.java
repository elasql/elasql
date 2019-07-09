package org.elasql.procedure.tpart;

import java.util.Set;

import org.elasql.migration.MigrationMgr;
import org.elasql.procedure.StoredProcedureTask;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.util.Timer;
import org.vanilladb.core.util.TimerStatistics;

public class TPartStoredProcedureTask extends StoredProcedureTask {
	
	static {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(150_000);
					TimerStatistics.startRecording();
					TimerStatistics.startAutoReporting();
					Thread.sleep(2000_000);
					TimerStatistics.stopRecording();
					TimerStatistics.stopAutoReporting();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		}).start();
	}

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
		Timer timer = Timer.getLocalTimer();
		SpResultSet rs = null;
		
		Thread.currentThread().setName("Tx." + txNum);
		timer.reset();
		timer.startExecution();

		try {
			rs = tsp.execute();
		} finally {
			timer.stopExecution();
		}

		if (tsp.isMaster()) {
			if (clientId != -1)
				Elasql.connectionMgr().sendClientResponse(clientId, connectionId, txNum, rs);

			if (tsp.getProcedureType() == ProcedureType.MIGRATION) {
				// Send a notification to the sequencer
				TupleSet ts = new TupleSet(MigrationMgr.MSG_COLD_FINISH);
				Elasql.connectionMgr().pushTupleSet(PartitionMetaMgr.NUM_PARTITIONS, ts);
			}
			timer.addToGlobalStatistics();
		}
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
	
	private StoredProcedureCall call;
	
	public void setSpCall(StoredProcedureCall call) {
		this.call = call;
	}
	
	public StoredProcedureCall getSpCall() {
		return call;
	}
}

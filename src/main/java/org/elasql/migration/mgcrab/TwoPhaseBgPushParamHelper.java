package org.elasql.migration.mgcrab;

import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class TwoPhaseBgPushParamHelper extends StoredProcedureParamHelper {
	
	private BgPushPhases currentPhase;
	private MigrationRangeUpdate update;
	private int sourceNodeId;
	private int destNodeId;
	private int pushingKeyCount;
	private long lastTxNum;
	
	// We cache the raw parameters and translate them on-the-fly
	// since it is too costly to copy them.
	private Object[] rawParameters;
	
	@Override
	public void prepareParameters(Object... pars) {
		currentPhase = (BgPushPhases) pars[0];
		if (pars[1] != null)
			update = (MigrationRangeUpdate) pars[1];
		sourceNodeId = (Integer) pars[2];
		destNodeId = (Integer) pars[3];
		lastTxNum = (Long) pars[4];
		pushingKeyCount = (Integer) pars[5];
		rawParameters = pars;
	}
	
	public int getSourceNodeId() {
		return sourceNodeId;
	}
	
	public int getDestNodeId() {
		return destNodeId;
	}
	
	public BgPushPhases getCurrentPhase() {
		return currentPhase;
	}
	
	public MigrationRangeUpdate getMigrationRangeUpdate() {
		return update;
	}
	
	public long getLastPushTxNum() {
		return lastTxNum;
	}
	
	public int getPushingKeyCount() {
		return pushingKeyCount;
	}
	
	public PrimaryKey getPushingKey(int index) {
		return (PrimaryKey) rawParameters[index + 6];
	}
	
	@Override
	public Schema getResultSetSchema() {
		return new Schema();
	}

	@Override
	public SpResultRecord newResultSetRecord() {
		return new SpResultRecord();
	}
}

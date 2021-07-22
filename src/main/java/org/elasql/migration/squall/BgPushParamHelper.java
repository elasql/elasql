package org.elasql.migration.squall;

import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class BgPushParamHelper extends StoredProcedureParamHelper {
	
	private MigrationRangeUpdate update;
	private int sourceNodeId;
	private int destNodeId;
	private int pushingKeyCount;
	
	// We cache the raw parameters and translate them on-the-fly
	// since it is too costly to copy them.
	private Object[] rawParameters;
	
	@Override
	public void prepareParameters(Object... pars) {
		update = (MigrationRangeUpdate) pars[0];
		sourceNodeId = (Integer) pars[1];
		destNodeId = (Integer) pars[2];
		pushingKeyCount = (Integer) pars[3];
		rawParameters = pars;
	}
	
	public int getSourceNodeId() {
		return sourceNodeId;
	}
	
	public int getDestNodeId() {
		return destNodeId;
	}
	
	public MigrationRangeUpdate getMigrationRangeUpdate() {
		return update;
	}
	
	public int getPushingKeyCount() {
		return pushingKeyCount;
	}
	
	public PrimaryKey getPushingKey(int index) {
		return (PrimaryKey) rawParameters[index + 4];
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

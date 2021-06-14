package org.elasql.migration.mgcrab;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class PhaseChangeParamHelper extends StoredProcedureParamHelper {
	
	private Phase nextPhase;
	
	@Override
	public void prepareParameters(Object... pars) {
		nextPhase = (Phase) pars[0];
	}
	
	@Override
	public boolean isReadOnly() {
		return false;
	}
	
	public Phase getNextPhase() {
		return nextPhase;
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

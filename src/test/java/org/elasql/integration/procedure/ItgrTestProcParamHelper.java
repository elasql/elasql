package org.elasql.integration.procedure;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class ItgrTestProcParamHelper extends StoredProcedureParamHelper {

	protected int id, value, overflow;

	@Override
	public void prepareParameters(Object... pars) {
		// ignore
	}

	@Override
	public Schema getResultSetSchema() {
		return null;
	}

	@Override
	public SpResultRecord newResultSetRecord() {
		return null;
	}
}

package org.elasql.integration.procedure;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class ItgrTestValidationProcParamHelper extends StoredProcedureParamHelper {

	@Override
	public void prepareParameters(Object... pars) {
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

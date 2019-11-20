package org.elasql.migration.tpart.sp;

import org.elasql.migration.MigrationPlan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class MigrationStartParamHelper extends StoredProcedureParamHelper {
	
	private MigrationPlan migratePlan;
	
	@Override
	public void prepareParameters(Object... pars) {
		migratePlan = (MigrationPlan) pars[0];
	}
	
	@Override
	public boolean isReadOnly() {
		return false;
	}
	
	public MigrationPlan getMigrationPlan() {
		return migratePlan;
	}
	
	@Override
	public SpResultSet createResultSet() {
		// Return the result
		Schema sch = new Schema();
		Type t = Type.VARCHAR(10);
		sch.addField("status", t);
		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, t));
		return new SpResultSet(sch, rec);
	}
}

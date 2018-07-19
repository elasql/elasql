package org.elasql.migration.tpart.sp;

import org.elasql.migration.MigrationRange;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class ColdMigrationParamHelper extends StoredProcedureParamHelper {
	
	private MigrationRange targetRange;
	
	@Override
	public void prepareParameters(Object... pars) {
		targetRange = (MigrationRange) pars[0];
	}
	
	@Override
	public boolean isReadOnly() {
		return false;
	}
	
	public MigrationRange getMigrationRange() {
		return targetRange;
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

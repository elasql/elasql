package org.elasql.migration.mgcrab;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
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

package org.elasql.migration.sp;

import org.elasql.migration.Phase;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class MigrationStartParamHelper extends StoredProcedureParamHelper {
	
	private PartitionPlan newPlan;
	private Phase initialPhase;
	
	@Override
	public void prepareParameters(Object... pars) {
		newPlan = (PartitionPlan) pars[0];
		initialPhase = (Phase) pars[1];
	}
	
	@Override
	public boolean isReadOnly() {
		return false;
	}
	
	public PartitionPlan getNewPartitionPlan() {
		return newPlan;
	}
	
	public Phase getInitialPhase() {
		return initialPhase;
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

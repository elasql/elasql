package org.elasql.server.migration.procedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class BroadcastMigrationKeysParamHelper extends StoredProcedureParamHelper {
	
	private Integer[] migrateKeys;
	
	@Override
	public void prepareParameters(Object... pars) {
		migrateKeys = (Integer[]) pars;
		this.setReadOnly(false);
	}
	@Override
	public boolean isReadOnly() {
		return true;
	}
	
	public Integer[] getMigrateKeys() {
		return migrateKeys;
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

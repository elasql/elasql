package org.elasql.server.migration.procedure;

import org.elasql.sql.RecordKey;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class BroadcastMigrationKeysParamHelper extends StoredProcedureParamHelper {

	private RecordKey[] migrateKeys;
	private Integer SourceNode, DestNode;

	@Override
	public void prepareParameters(Object... pars) {
		int indexCnt = 0;
		SourceNode = (Integer) pars[indexCnt++];
		DestNode = (Integer) pars[indexCnt++];
		migrateKeys = new RecordKey[pars.length - 2];

		for (int i = 0; i < pars.length - 2; i++)
			migrateKeys[i] = (RecordKey) pars[indexCnt++];

		this.setReadOnly(false);
	}

	@Override
	public boolean isReadOnly() {
		return true;
	}

	public RecordKey[] getMigrateKeys() {
		return migrateKeys;
	}

	public Integer getSouceNode() {
		return SourceNode;
	}

	public Integer getDestNode() {
		return DestNode;
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

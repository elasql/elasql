package org.elasql.migration.sp;

import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class BgPushParamHelper extends StoredProcedureParamHelper {
	
	private MigrationRangeUpdate update;
	private int sourceNodeId;
	private int destNodeId;
	private RecordKey[] pushingKeys;
	private RecordKey[] storingKeys;
	
	@Override
	public void prepareParameters(Object... pars) {
		if (pars[0] != null)
			update = (MigrationRangeUpdate) pars[0];
		sourceNodeId = (Integer) pars[1];
		destNodeId = (Integer) pars[2];
		
		// Read pushing keys
		int pushingCount = (Integer) pars[3];
		pushingKeys = new RecordKey[pushingCount];
		for (int i = 0; i < pushingCount; i++)
			pushingKeys[i] = (RecordKey) pars[i + 4];
		
		// Read storing keys
		int storingCount = (Integer) pars[pushingCount + 4];
		storingKeys = new RecordKey[storingCount];
		for (int i = 0; i < storingCount; i++)
			storingKeys[i] = (RecordKey) pars[i + 5 + pushingCount];
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
	
	public RecordKey[] getPushingKeys() {
		return pushingKeys;
	}
	
	public RecordKey[] getStoringKeys() {
		return storingKeys;
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

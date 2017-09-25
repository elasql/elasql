package org.elasql.procedure.tpart;

import java.util.Map;
import java.util.Set;

import org.elasql.cache.CachedRecord;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class TPartCacheWriteBackProc extends TPartStoredProcedure<StoredProcedureParamHelper> {
	
	private static long nextTxNumber = 1;
	
	private static long getNextTxNum() {
		return nextTxNumber++;
	}
	
	private Set<RecordKey> writeBackKeys;

	public TPartCacheWriteBackProc(Set<RecordKey> writeBackKeys) {
		super(getNextTxNum(), StoredProcedureParamHelper.DefaultParamHelper());
		
		this.writeBackKeys = writeBackKeys;
	}

	@Override
	public double getWeight() {
		return 0;
	}

	@Override
	protected void prepareKeys() {
		for (RecordKey key : writeBackKeys) {
			addReadKey(key);
			addWriteKey(key);
		}
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		Thread.currentThread().setName("Writeback Thread");
		// do nothing
	}

}

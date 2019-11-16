package org.elasql.migration.tpart.sp;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.migration.MigrationRange;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.schedule.tpart.graph.Node;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class ColdMigrationProcedure extends TPartStoredProcedure<ColdMigrationParamHelper> {
	private static Logger logger = Logger.getLogger(ColdMigrationProcedure.class.getName());
	
	// the tuples that are not moved
	private Set<RecordKey> coldKeys = new HashSet<RecordKey>(); 
	
	// the tuples that have been moved to the dest node by other txs
	// these need to be deleted from cache and inserted to the local storage
	private Set<RecordKey> localInsertKeys = new HashSet<RecordKey>();
	
	public ColdMigrationProcedure(long txNum) {
		super(txNum, new ColdMigrationParamHelper());
	}

	@Override
	protected void prepareKeys() {
		// Do nothing
	}
	
	public void prepareMigrationKeys(TGraph graph) {
		MigrationRange range = paramHelper.getMigrationRange();
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
		Map<RecordKey, Node> resPos = graph.getResourceNodeMap();
		
		// Iterate over the keys in the migration range
		Iterator<RecordKey> keyIter = Elasql.migrationMgr().toKeyIterator(range);
		while (keyIter.hasNext()) {
			RecordKey key = keyIter.next();
			
			// Check if someone in the T-Graph uses it
			// Summary:
			// - In dest => localInsertKeys
			// - In source => coldKeys
			// - The other => do nothing (it should be written back by other txs)
			Node node = resPos.get(key);
			if (node != null) {
				if (node.getPartId() == range.getSourcePartId()) // Not moved
					coldKeys.add(key);
				else if (node.getPartId() == range.getDestPartId()) // Moved & in dest
					localInsertKeys.add(key);
				else
					continue; // Moved & not in dest
			} else {
				// Check if it has been moved (before this TGraph)
				Integer partId = partMgr.queryLocationTable(key);
				if (partId != null) {
					if (partId == range.getDestPartId()) // Moved & in dest
						localInsertKeys.add(key);
					else
						continue; // Moved & not in dest
				} else {
					// No one moves this key
					coldKeys.add(key);
				}
			}
		}
		
		// Add to read & write keys
		for (RecordKey key : coldKeys) {
			addReadKey(key);
			addWriteKey(key);
		}
		for (RecordKey key : localInsertKeys) {
			addReadKey(key);
			addWriteKey(key);
		}
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Tx." + txNum + " starts a cold migration");
		
		// Do nothing
	}

	@Override
	public double getWeight() {
		return coldKeys.size() + localInsertKeys.size();
	}
	
	@Override
	public ProcedureType getProcedureType() {
		return ProcedureType.MIGRATION;
	}
	
	public Set<RecordKey> getLocalCacheToStorage() {
		return localInsertKeys;
	}
	
	public MigrationRange getMigrationRange() {
		return paramHelper.getMigrationRange();
	}
}

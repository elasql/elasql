package org.elasql.schedule.tpart.hermes;

import org.elasql.cache.tpart.TPartCacheMgr;
import org.elasql.schedule.tpart.graph.Edge;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.schedule.tpart.sink.Sinker;
import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.sql.PrimaryKey;

public class FusionSinker extends Sinker {
	
	private FusionTable fusionTable;
	
	public FusionSinker(FusionTable table) {
		fusionTable = table;
	}
	
	// Writing back (to sinks)
	@Override
	protected void generateWritingBackPlans(SunkPlan plan, TxNode node) {
		// TODO: Uncomment this when the migration module is migrated
		// MigrationTx: remove the corresponding range from the migration manager
//		if (node.getTask().getProcedureType() == ProcedureType.MIGRATION) {
//			// Migration Tx:
//			// 1. The master node must be at the destination node
//			// 2. All write backs are insertions to local storage
//			ColdMigrationProcedure sp = (ColdMigrationProcedure) node.getTask().getProcedure();
//			MigrationMgr migraMgr = Elasql.migrationMgr();
//			MigrationRange range = sp.getMigrationRange();
//			
//			for (Edge e : node.getWriteBackEdges()) {
//				RecordKey k = e.getResourceKey();
//				int id = migraMgr.toNumericId(k);
//				
//				if (range.getDestPartId() == myId && range.contains(id)) {
//					plan.addStorageInsertion(k);
//					plan.addLocalWriteBackInfo(k);
////									cm.registerSinkWriteback(k, txNum);
//				}
//			}
//			
//			// Update the migration status
//			migraMgr.markMigrationRangeMoved(sp.getMigrationRange());
//		} else { // Normal tx
			for (Edge e : node.getWriteBackEdges()) {
				int dataWriteBackPos = e.getTarget().getPartId();
				PrimaryKey k = e.getResourceKey();
				int dataCurrentPos = getRecordCurrentLocation(k);
				int dataOriginalPos = parMeta.getPartition(k);
				
				// Hermes-specific code
				// The record's final location changes
				if(dataCurrentPos != dataWriteBackPos) {
					// the non-origin destination perform insertion
					// Note that it still needs the write back edges
					// since it need to get the record to be inserted
					if (dataWriteBackPos != dataOriginalPos	&&
							dataWriteBackPos == myId)
						plan.addCacheInsertion(k);
					
					// the non-origin source perform deletion
					if(dataCurrentPos != dataOriginalPos &&
							dataCurrentPos == myId) {
						plan.addCacheDeletion(k);
						
						// Since the node does not have the write-back edge,
						// we need to register the lock here.
						// TODO: Think if we need a "write back deletion"
//										cm.registerSinkWriteback(k, txNum);
					}
					
					setRecordCurrentLocation(k, dataWriteBackPos);
				}
				
				// == Hermes specific code ==
				// A special case:
				// If there is a cold migration happened,
				// some records in the fusion table may not be
				// inserted to the storage of the destination node.
				// These records should currently locate in the cache
				// of the destination node. For such records, we
				// delete it from cache and insert them to the storage.
				// TODO: Uncomment this when the migration module is migrated
//				Integer fusionRecord = parMeta.queryLocationTable(k);
//				if (fusionRecord != null && fusionRecord == dataOriginalPos
//						&& dataWriteBackPos == dataOriginalPos) {
//					if (dataWriteBackPos == myId) {
//						// This action will also delete it form cache.
//						plan.addStorageInsertion(k);
//					}
//					parMeta.removeFromLocationTable(k);
//				}
				
				// For any node (which may not be the master node),
				// if it is the destination of a write-back,
				// add this info to the plan.
				if (dataWriteBackPos == myId) {
					// tell the task to write back local
					plan.addLocalWriteBackInfo(k);
				} else if (plan.isHereMaster()) { // XXX: Untested
					// push the write-back data to the remote node
					plan.addPushingInfo(k, dataWriteBackPos, TPartCacheMgr.toSinkId(dataWriteBackPos));
				}
			}
//		} // TODO: Uncomment this when the migration module is migrated
	}
	
	private int getRecordCurrentLocation(PrimaryKey key) {
		int location = fusionTable.getLocation(key);
		if (location != -1)
			return location;
		
		// TODO: Uncomment this when the migration module is migrated
//		if (isInMigration) {
//			Integer partId = Elasql.migrationMgr().getPartition(key);
//			if (partId != null)
//				return partId;
//		}
		
		return parMeta.getPartition(key);
	}
	
	private void setRecordCurrentLocation(PrimaryKey key, int loc) {
		if (parMeta.getPartition(key) == loc && fusionTable.containsKey(key))
			fusionTable.remove(key);
		else
			fusionTable.setLocation(key, loc);
	}
}

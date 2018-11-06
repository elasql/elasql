package org.elasql.migration;

import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;

public interface MigrationSystemController {
	
	int MSG_RANGE_FINISH = -8787;
	
//	long START_MIGRATION_TIME = 60_000; // debug
//	long START_MIGRATION_TIME = 150_000; // normal
	// long START_MIGRATION_TIME = 180_000;
	 long START_MIGRATION_TIME = 5000_000; // long enough to disable
	
	int CONTROLLER_NODE_ID = PartitionMetaMgr.NUM_PARTITIONS;
	
	void startMigrationTrigger();
	
	void sendMigrationStartRequest(PartitionPlan newPartPlan);
	
	void onReceiveMigrationRangeFinishMsg(MigrationRangeFinishMessage msg);
	
	void sendMigrationFinishRequest();
}

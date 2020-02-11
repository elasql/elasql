package org.elasql.migration;

import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;

public interface MigrationSystemController {
	
	boolean ENABLE_MIGRATION = true;
	
	int MSG_RANGE_FINISH = -8787;
	
//	long START_MIGRATION_TIME = 90_000; // debug
	long START_MIGRATION_TIME = 500_000; // normal
	
	int CONTROLLER_NODE_ID = PartitionMetaMgr.NUM_PARTITIONS;
	
	void startMigrationTrigger();
	
	void sendMigrationStartRequest(PartitionPlan newPartPlan);
	
	void onReceiveMigrationRangeFinishMsg(MigrationRangeFinishMessage msg);
	
	void sendMigrationFinishRequest();
}

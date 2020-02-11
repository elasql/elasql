package org.elasql.migration;

import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;

public interface MigrationSystemController {
	
	int MSG_RANGE_FINISH = -8787;
	
	int CONTROLLER_NODE_ID = PartitionMetaMgr.NUM_PARTITIONS;
	
	void startMigrationTrigger();
	
	void sendMigrationStartRequest(PartitionPlan newPartPlan);
	
	void onReceiveMigrationRangeFinishMsg(MigrationRangeFinishMessage msg);
	
	void sendMigrationFinishRequest();
}

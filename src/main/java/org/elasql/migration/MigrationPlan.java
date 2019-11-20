package org.elasql.migration;

import java.io.Serializable;
import java.util.Deque;

import org.elasql.storage.metadata.PartitionPlan;

public interface MigrationPlan extends Serializable {
	
	PartitionPlan oldPartitionPlan();
	
	PartitionPlan newPartitionPlan();
	
	Deque<MigrationRange> generateMigrationRanges();
	
}

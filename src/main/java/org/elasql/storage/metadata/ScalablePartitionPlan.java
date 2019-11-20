package org.elasql.storage.metadata;

import org.elasql.migration.MigrationPlan;

public interface ScalablePartitionPlan extends PartitionPlan {
	
	MigrationPlan scaleOut();
	
	MigrationPlan scaleIn();
	
}

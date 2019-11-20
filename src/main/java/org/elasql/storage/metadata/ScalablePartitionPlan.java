package org.elasql.storage.metadata;

import org.elasql.migration.MigrationPlan;

public interface ScalablePartitionPlan {
	
	MigrationPlan scaleOut();
	
	MigrationPlan scaleIn();
	
}

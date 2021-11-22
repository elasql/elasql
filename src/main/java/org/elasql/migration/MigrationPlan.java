package org.elasql.migration;

import java.io.Serializable;
import java.util.List;

import org.elasql.storage.metadata.PartitionPlan;

public interface MigrationPlan extends Serializable {
	
	PartitionPlan getNewPart();
	
	List<MigrationRange> getMigrationRanges(MigrationComponentFactory factory);
	
	// TODO: Check if we need this
	List<MigrationPlan> splits();
}

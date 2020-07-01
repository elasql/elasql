package org.elasql.migration;

import java.io.Serializable;

/**
 * Used by BG push to update the migration status.
 */
public interface MigrationRangeUpdate extends Serializable {
	
	int getSourcePartId();
	
	int getDestPartId();

}

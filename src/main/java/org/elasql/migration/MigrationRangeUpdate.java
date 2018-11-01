package org.elasql.migration;

/**
 * Used by BG push to update the migration status.
 */
public interface MigrationRangeUpdate {
	
	int getSourcePartId();
	
	int getDestPartId();

}

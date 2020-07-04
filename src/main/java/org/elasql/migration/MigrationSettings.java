package org.elasql.migration;

import org.elasql.util.ElasqlProperties;

public final class MigrationSettings {
	
	public static final boolean ENABLE_MIGRATION;
	
	public static final long START_MONITOR_TIME;
	public static final long MIGRATION_PERIOD;
	
	public static final MigrationAlgorithm MIGRATION_ALGORITHM;
	public static final PlanningAlgorithm PLANNING_ALGORITHM;
	public static final boolean USE_PREDEFINED_PLAN;
	
	public static final boolean USE_BYTES_FOR_CHUNK_SIZE;
	public static final int CHUNK_SIZE_IN_BYTES;
	public static final int CHUNK_SIZE_IN_COUNT;
	public static final int CHUNK_SIZE;
	
	static {
		ENABLE_MIGRATION = ElasqlProperties.getLoader().getPropertyAsBoolean(
				MigrationSettings.class.getName() + ".ENABLE_MIGRATION", false);
		START_MONITOR_TIME = ElasqlProperties.getLoader().getPropertyAsLong(
				MigrationSettings.class.getName() + ".START_MONITOR_TIME", 180_000);
		MIGRATION_PERIOD = ElasqlProperties.getLoader().getPropertyAsLong(
				MigrationSettings.class.getName() + ".MIGRATION_PERIOD", 60_000);
		int algorithm = ElasqlProperties.getLoader().getPropertyAsInteger(
				MigrationSettings.class.getName() + ".MIGRATION_ALGORITHM", 3);
		MIGRATION_ALGORITHM = MigrationAlgorithm.fromInteger(algorithm);
		algorithm = ElasqlProperties.getLoader().getPropertyAsInteger(
				MigrationSettings.class.getName() + ".PLANNING_ALGORITHM", 0);
		PLANNING_ALGORITHM = PlanningAlgorithm.fromInteger(algorithm);
		USE_PREDEFINED_PLAN = ElasqlProperties.getLoader().getPropertyAsBoolean(
				MigrationSettings.class.getName() + ".USE_PREDEFINED_PLAN", true);
		
		// Chunk size
		USE_BYTES_FOR_CHUNK_SIZE = ElasqlProperties.getLoader().getPropertyAsBoolean(
				MigrationSettings.class.getName() + ".USE_BYTES_FOR_CHUNK_SIZE", false);
		CHUNK_SIZE_IN_BYTES = ElasqlProperties.getLoader().getPropertyAsInteger(
				MigrationSettings.class.getName() + ".CHUNK_SIZE_IN_BYTES", 1_000_000);
		CHUNK_SIZE_IN_COUNT = ElasqlProperties.getLoader().getPropertyAsInteger(
				MigrationSettings.class.getName() + ".CHUNK_SIZE_IN_COUNT", 40000);
		CHUNK_SIZE = USE_BYTES_FOR_CHUNK_SIZE? CHUNK_SIZE_IN_BYTES : CHUNK_SIZE_IN_COUNT;
	}
}

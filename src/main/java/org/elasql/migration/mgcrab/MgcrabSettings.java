package org.elasql.migration.mgcrab;

import org.elasql.util.ElasqlProperties;

public final class MgcrabSettings {

	// Two Background Pushes
	public static final boolean ENABLE_TWO_PHASE_BG_PUSH;
	public static final boolean ENABLE_PIPELINING_TWO_PHASE_BG;
	public static final long BG_PUSH_START_DELAY; // in ms.
	
	// General settings
	public static final Phase INIT_PHASE = Phase.CRABBING;
	
	// Caught-up mode
	public static final boolean ENABLE_CAUGHT_UP;
	// Scaling-out (18 nodes): 90_000
	// Scaling-out (3 nodes): 105_000
	// Sensitivity test: 150_000
	public static final long START_CAUGHT_UP_DELAY;
	
	static {
		ENABLE_TWO_PHASE_BG_PUSH = ElasqlProperties.getLoader().getPropertyAsBoolean(
				MgcrabSettings.class.getName() + ".ENABLE_TWO_PHASE_BG_PUSH", true);
		ENABLE_PIPELINING_TWO_PHASE_BG = ElasqlProperties.getLoader().getPropertyAsBoolean(
				MgcrabSettings.class.getName() + ".ENABLE_PIPELINING_TWO_PHASE_BG", true);
		BG_PUSH_START_DELAY = ElasqlProperties.getLoader().getPropertyAsLong(
				MgcrabSettings.class.getName() + ".BG_PUSH_START_DELAY", 0);
		ENABLE_CAUGHT_UP = ElasqlProperties.getLoader().getPropertyAsBoolean(
				MgcrabSettings.class.getName() + ".ENABLE_CAUGHT_UP", true);
		START_CAUGHT_UP_DELAY = ElasqlProperties.getLoader().getPropertyAsLong(
				MgcrabSettings.class.getName() + ".START_CAUGHT_UP_DELAY", 105_000);
	}
}

package org.elasql.perf.tpart.ai;

import org.elasql.util.ElasqlProperties;

public class Estimator {
	
	public static final boolean ENABLE_COLLECTING_DATA;

	static {
		ENABLE_COLLECTING_DATA = ElasqlProperties.getLoader()
				.getPropertyAsBoolean(Estimator.class.getName() + ".ENABLE_COLLECTING_DATA", false);
	}	
	
}

package org.elasql.migration;

public enum PlanningAlgorithm {
	PREDEFINED_PLANS, CLAY;

	static PlanningAlgorithm fromInteger(int index) {
		switch (index) {
		case 0:
			return PREDEFINED_PLANS;
		case 1:
			return CLAY;
		default:
			throw new RuntimeException("Unsupport service type");
		}
	}
}

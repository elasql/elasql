package org.elasql.migration;

public enum PlanningAlgorithm {
	PREDEFINED, CLAY;

	static PlanningAlgorithm fromInteger(int index) {
		switch (index) {
		case 0:
			return PREDEFINED;
		case 1:
			return CLAY;
		default:
			throw new RuntimeException("Unsupport service type");
		}
	}
}

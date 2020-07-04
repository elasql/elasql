package org.elasql.migration;

public enum PlanningAlgorithm {
	CLAY;

	static PlanningAlgorithm fromInteger(int index) {
		switch (index) {
		case 0:
			return CLAY;
		default:
			throw new RuntimeException("Unsupport service type");
		}
	}
}

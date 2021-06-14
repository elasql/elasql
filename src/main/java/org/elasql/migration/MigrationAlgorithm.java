package org.elasql.migration;

public enum MigrationAlgorithm {
	STOP_COPY, ALBATROSS, SQUALL, MGCRAB;

	static MigrationAlgorithm fromInteger(int index) {
		switch (index) {
		case 0:
			return STOP_COPY;
		case 1:
			return ALBATROSS;
		case 2:
			return SQUALL;
		case 3:
			return MGCRAB;
		default:
			throw new RuntimeException("Unsupport service type");
		}
	}
}

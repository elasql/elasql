package org.elasql.schedule.tpart;

import org.elasql.schedule.DdStoredProcedure;
import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.sql.RecordKey;

public interface TPartStoredProcedure extends DdStoredProcedure {

	public static final int NOP = 0, KEY_ACCESS = 1, RECONNAISSANCE = 2,
			PRE_LOAD = 3, POPULATE = 4, PROFILE = 5;

	RecordKey[] getReadSet();

	RecordKey[] getWriteSet();

	void setSunkPlan(SunkPlan plan);

	SunkPlan getSunkPlan();

	double getWeight();

	int getProcedureType();

	boolean isReadOnly();

	boolean isMaster();

	void requestConservativeLocks();
}

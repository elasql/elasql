package org.elasql.perf;

import org.elasql.remote.groupcomm.StoredProcedureCall;

public class DummyPerformanceManager implements PerformanceManager {

	@Override
	public void monitorTransaction(StoredProcedureCall spc) {
		// Do nothing
	}
	
}

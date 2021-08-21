package org.elasql.perf;

import org.elasql.remote.groupcomm.StoredProcedureCall;

public class DummyPerformanceManager extends PerformanceManager {

	@Override
	public void run() {
		// Do nothing
	}

	@Override
	public void monitorTransaction(StoredProcedureCall spc) {
		// Do nothing
	}
	
}

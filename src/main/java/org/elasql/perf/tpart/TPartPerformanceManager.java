package org.elasql.perf.tpart;

import org.elasql.perf.PerformanceManager;
import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;

public class TPartPerformanceManager implements PerformanceManager {

	private FeatureCollector featureCollector;
	
	public TPartPerformanceManager(TPartStoredProcedureFactory factory) {
		if (Estimator.ENABLE_COLLECTING_DATA) {
			if (Elasql.isStandAloneSequencer()) {
				featureCollector = new FeatureCollector(factory);
				Elasql.taskMgr().runTask(featureCollector);
			}
		}
	}

	@Override
	public void monitorTransaction(StoredProcedureCall spc) {
		if (Estimator.ENABLE_COLLECTING_DATA) {
			if (Elasql.isStandAloneSequencer()) {
				featureCollector.monitorTransaction(spc);
			}
		}
	}
}

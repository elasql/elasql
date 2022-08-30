package org.elasql.perf.tpart;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.Route;
import org.elasql.schedule.tpart.graph.TGraph;

public interface CentralRoutingAgent {
	
	public Route suggestRoute(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse);
	
	public void onTxRouted(long txNum, int routeDest);
	
	public void onTxCommitted(long txNum, int masterId, long latency);
	
}

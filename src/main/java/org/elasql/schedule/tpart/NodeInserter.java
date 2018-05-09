package org.elasql.schedule.tpart;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;

public interface NodeInserter {

	/**
	 * Insert a transaction node into the given T-graph with the specified task.
	 * 
	 * @param graph
	 * @param node
	 */
	void insert(TGraph graph, CostEstimator costEstimator, TPartStoredProcedureTask task);
}

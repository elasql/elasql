package org.elasql.schedule.tpart;

import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;

public interface BatchNodeInserter {

	/**
	 * Insert a batch of transaction requests into the given T-Graph.
	 * 
	 * @param graph the graph that the inserter inserts the tasks to
	 * @param tasks the tasks to be inserted to the graph
	 */
	void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks);
}

package org.elasql.schedule.tpart.sink;

import java.util.Iterator;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;

public abstract class Sinker {

	/**
	 * Sink the graph.
	 * 
	 * @param graph
	 *            the target t-graph
	 * @return A sunk tasks iterator. If no plan has been sunk in this round,
	 *         return null.
	 */
	public abstract Iterator<TPartStoredProcedureTask> sink(TGraph graph);

	/**
	 * Sink the node whose tx number is less than or equal to specified one.
	 * 
	 * @param graph
	 *            the target t-graph
	 * @param txNum
	 *            the maximal tx number to be sunk
	 * @return A sunk task iterator. If no plan has been sunk in this round,
	 *         return null.
	 */
	public abstract Iterator<TPartStoredProcedureTask> sink(TGraph graph, long txNum);

}

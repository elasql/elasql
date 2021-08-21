package org.elasql.perf;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.core.server.task.Task;

/**
 * A manager that collects workload and system metrics.
 * 
 * @author Yu-Shan Lin
 */
public abstract class PerformanceManager extends Task {
	
	/**
	 * Analyzes and record the characteristics of a
	 * transaction request. Only the sequencer will
	 * call this method.
	 * 
	 * @param spc
	 */
	public abstract void monitorTransaction(StoredProcedureCall spc);
	
}

package org.elasql.perf;

import org.elasql.remote.groupcomm.StoredProcedureCall;

/**
 * A manager that collects workload and system metrics.
 * 
 * @author Yu-Shan Lin
 */
public interface PerformanceManager {
	
	/**
	 * Analyzes and record the characteristics of a
	 * transaction request. Only the sequencer will
	 * call this method.
	 * 
	 * @param spc
	 */
	void monitorTransaction(StoredProcedureCall spc);
	
}

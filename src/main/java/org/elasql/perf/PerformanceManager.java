package org.elasql.perf;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.core.util.TransactionProfiler;

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
	 * @param spc the transaction request
	 */
	void monitorTransaction(StoredProcedureCall spc);
	
	/**
	 * Adds a transaction's metrics to the performance
	 * manager.
	 * 
	 * @param txNum the transaction number
	 * @param role the role of this machine for the transaction
	 * @param profiler the metrics for the transaction
	 */
	void addTransactionMetics(long txNum, String role, TransactionProfiler profiler);
	
	/**
	 * Receives the metric report coming from other database servers.
	 * 
	 * @param report the metric report
	 */
	void receiveMetricReport(MetricReport report);
	
	/**
	 * Get the instance of MetricWarehouse.
	 */
	MetricWarehouse getMetricWarehouse();
}

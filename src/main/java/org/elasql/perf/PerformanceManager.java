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
	 * Pre-process the transaction request so that it can estimate
	 * the performance of the transaction or record the
	 * characteristics of the transaction. <br>
	 * <br>
	 * The method should be called by the sequencer and before
	 * sending the request to total ordering. The performance
	 * manager will take the responsibility of sending the 
	 * processed requests to all DB servers via total-ordering.
	 * 
	 * @param spc the transaction request
	 */
	void preprocessSpCall(StoredProcedureCall spc);
	
	/**
	 * Adds a transaction's metrics to the performance
	 * manager.
	 * 
	 * @param txNum the transaction number
	 * @param role the role of this machine for the transaction
	 * @param isTxDistributed true if tx is a distributed tx
	 * @param profiler the metrics for the transaction
	 */
	void addTransactionMetics(long txNum, String role, boolean isTxDistributed, TransactionProfiler profiler);
	
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

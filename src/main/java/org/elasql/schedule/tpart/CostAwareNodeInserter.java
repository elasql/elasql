package org.elasql.schedule.tpart;

import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

public class CostAwareNodeInserter implements BatchNodeInserter {
	
	public static final double BETA;

	static {
		BETA = ElasqlProperties.getLoader().getPropertyAsDouble(CostAwareNodeInserter.class.getName() + ".BETA", 1.0);
	}
	
	static double truncate(double number, int precision) {
		double prec = Math.pow(10, precision);
		int integerPart = (int) number;
		double fractionalPart = number - integerPart;
		fractionalPart *= prec;
		int fractPart = (int) fractionalPart;
		fractionalPart = (double) (integerPart) + (double) (fractPart) / prec;
		return fractionalPart;
	}
	
	private double[] loadPerPart = new double[PartitionMetaMgr.NUM_PARTITIONS];
	private PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();

	/**
	 * Insert this node to the partition that will result in minimal cost.
	 */
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		// Sequentially insert each node
		for (TPartStoredProcedureTask task : tasks) {
			insertNode(graph, task);
		}
		
		// Reset the statistics
		for (int partId = 0; partId < PartitionMetaMgr.NUM_PARTITIONS; partId++)
			loadPerPart[partId] = 0.0;
	}
		
	private void insertNode(TGraph graph, TPartStoredProcedureTask task) {
		// Evaluate the cost on each part
		double minCost = Double.MAX_VALUE;
		int minCostPart = 0;
		
		for (int partId = 0; partId < PartitionMetaMgr.NUM_PARTITIONS; partId++) {
			double cost = estimateCost(graph, task, partId);
			if (cost < minCost) {
				minCost = cost;
				minCostPart = partId;
			}
		}
		
		// Insert the node
		graph.insertTxNode(task, minCostPart);
		
		// Update the statistics
		loadPerPart[minCostPart] += task.getWeight();
	}
	
	private double estimateCost(TGraph graph, TPartStoredProcedureTask task, int targetPart) {
		// calculate cross partition edge cost
		double crossEdgeCost = 0;
		
		// count read edges
		for (RecordKey key : task.getReadSet()) {
			if (graph.getResourcePosition(key).getPartId() != targetPart) {
				crossEdgeCost++;
			}
		}
		
		// count write-back edges
		for (RecordKey key : task.getWriteSet()) {
			if (partMgr.getPartition(key) != targetPart) {
				crossEdgeCost++;
			}
		}
		
		double loadCost = loadPerPart[targetPart] + task.getWeight();

		return truncate(loadCost * (1 - BETA) + crossEdgeCost * BETA, 4);
	}
}

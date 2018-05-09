package org.elasql.schedule.tpart;

import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

public class CostEstimator {
	public static final double BETA;

	static {
		BETA = ElasqlProperties.getLoader().getPropertyAsDouble(CostEstimator.class.getName() + ".BETA", 1.0);
	}
	
	private double[] partLoads = new double[PartitionMetaMgr.NUM_PARTITIONS];
	
	// Cache the count of source records per partition for a node
	private long cachedTxId = Long.MIN_VALUE;
	private int[] readRecPerPart;

	protected static double truncate(double number, int precision) {
		double prec = Math.pow(10, precision);
		int integerPart = (int) number;
		double fractionalPart = number - integerPart;
		fractionalPart *= prec;
		int fractPart = (int) fractionalPart;
		fractionalPart = (double) (integerPart) + (double) (fractPart) / prec;
		return fractionalPart;
	}

	public CostEstimator() {
		reset();
	}

	public void reset() {
		for (int i = 0; i < partLoads.length; i++)
			partLoads[i] = 0;
	}
	
	public void analyzeBatch(List<TPartStoredProcedureTask> batch) {
		// Do nothing
	}

	/**
	 * Calculate the cost with a given task assigned to a specified partition.
	 */
	public double estimateTaskCostOnPart(TGraph graph, TPartStoredProcedureTask task, int assignedPart) {
		int[] readPerParts = calPartitionRecordCount(task, graph);
		
		// calculate cross partition edge cost
		double edgeCost = 0;
		for (int part = 0; part < PartitionMetaMgr.NUM_PARTITIONS; part++) {
			if (part != assignedPart)
				edgeCost += readPerParts[part];
		}

		// calculate partition load cost
		// Load Calculation Method 1
		// Note that it sums the difference of loads instead of the total load for each node
//		double averageLoad = (totalLoads + newNode.getWeight()) / PartitionMetaMgr.NUM_PARTITIONS;
//		double loadCost = 0;
//
//		for (int part = 0; part < PartitionMetaMgr.NUM_PARTITIONS; part++) {
//			if (part == newNode.getPartId())
//				loadCost += Math.abs(partLoads[part] + newNode.getWeight() - averageLoad);
//			else
//				loadCost += Math.abs(partLoads[part] - averageLoad);
//		}
		
		// Load Calculation Method 2
		double loadCost = partLoads[assignedPart] + task.getWeight();

		return truncate(loadCost * (1 - BETA) + edgeCost * BETA, 4);
	}

	/**
	 * Incrementally calculate the cost after adding one node.
	 */
	public void updateAddNodeCost(TGraph graph, TPartStoredProcedureTask task, int assignedPartId) {
		partLoads[assignedPartId] += task.getWeight();
	}
	
	/**
	 * Calculate the number of record for each partition as an array
	 */
	private int[] calPartitionRecordCount(TPartStoredProcedureTask task, TGraph graph) {
		if (task.getTxNum() == cachedTxId)
			return readRecPerPart;
		
		cachedTxId = task.getTxNum();
		readRecPerPart = new int[PartitionMetaMgr.NUM_PARTITIONS];

		for (RecordKey res : task.getReadSet()) {
			if (Elasql.partitionMetaMgr().isFullyReplicated(res))
				continue;

			readRecPerPart[graph.getResourcePosition(res).getPartId()]++;
		}

		return readRecPerPart;
	}
}

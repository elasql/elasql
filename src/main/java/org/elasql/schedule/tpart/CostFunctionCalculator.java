package org.elasql.schedule.tpart;

import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

public class CostFunctionCalculator {
	public static final double BETA;

	static {
		BETA = ElasqlProperties.getLoader().getPropertyAsDouble(CostFunctionCalculator.class.getName() + ".BETA", 1.0);
	}
	
	private double[] partLoads = new double[PartitionMetaMgr.NUM_PARTITIONS];
	private double totalLoads = 0; // used to speed up the sum of partLoads
	private int crossEdgeCount;
	
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

	public CostFunctionCalculator() {
		reset();
	}

	public void reset() {
		for (int i = 0; i < partLoads.length; i++)
			partLoads[i] = 0;
		crossEdgeCount = 0;
		totalLoads = 0;
	}
	
	public void analyzeBatch(List<TPartStoredProcedureTask> batch) {
		// Do nothing
	}

	/**
	 * Update the cost with a node added or removed.
	 */
	private void updateCost(Node node, TGraph graph, boolean isAdd) {
		int coef = isAdd ? 1 : -1;

		partLoads[node.getPartId()] += (node.getWeight() * coef);
		totalLoads += (node.getWeight() * coef);

		if (node.getTask() == null || node.getTask().getReadSet() == null)
			return;

		// XXX Consider the edge weight
		for (RecordKey res : node.getTask().getReadSet()) {
			if (Elasql.partitionMetaMgr().isFullyReplicated(res))
				continue;
			if (graph.getResourcePosition(res).getPartId() != node.getPartId()) {
				crossEdgeCount += coef;
			}
		}
	}

	/**
	 * Incrementally calculate the cost after adding one node.
	 */
	public void updateAddNodeCost(Node newNode, TGraph graph) {
		updateCost(newNode, graph, true);
	}

	/**
	 * Incrementally calculate the cost after removing one node.
	 * XXX: Not used
	 */
	public void updateRemoveNodeCost(Node removeNode, TGraph graph) {
		updateCost(removeNode, graph, false);
	}

	/**
	 * Calculate the cost with a node added. ! The real cost will not be updated
	 * by this method.
	 */
	public double calAddNodeCost(Node newNode, TGraph graph) {
		int[] readPerParts = calPartitionRecordCount(newNode, graph);
		
		// calculate cross partition edge cost
		double edgeCost = 0;
		for (int part = 0; part < PartitionMetaMgr.NUM_PARTITIONS; part++) {
			if (part != newNode.getPartId())
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
		double loadCost = partLoads[newNode.getPartId()] + newNode.getWeight();

		return truncate(loadCost * (1 - BETA) + edgeCost * BETA, 4);
	}

	/**
	 * Calculate the cost with a node removed. ! The real cost will not be
	 * updated by this method.
	 * XXX: Not used
	 */
	public double calRemoveNodeCost(Node removeNode, TGraph graph) {
		updateCost(removeNode, graph, false);
		double cost = getCost();
		updateCost(removeNode, graph, true);
		return cost;
	}

	/**
	 * Get the cost of current cost function state.
	 * 
	 * XXX: Not used
	 * 
	 * @return The cost of current cost function state. TODO alpha, beta
	 */
	public double getCost() {
		double totalLoad = 0;
		for (Double d : partLoads) {
			totalLoad += d;
		}
		double loadBalance = 0;
		for (Double d : partLoads)
			loadBalance += Math.abs(d - totalLoad / partLoads.length);
		return truncate(loadBalance * (1 - BETA) + crossEdgeCount * BETA, 4);
	}
	
	// XXX: Not used
	public double[] getPartLoads() {
		return partLoads;
	}
	
	/**
	 * Calculate the number of record for each partition as an array
	 */
	private int[] calPartitionRecordCount(Node node, TGraph graph) {
		if (node.getTxNum() == cachedTxId)
			return readRecPerPart;
		
		cachedTxId = node.getTxNum();
		readRecPerPart = new int[PartitionMetaMgr.NUM_PARTITIONS];

		for (RecordKey res : node.getTask().getReadSet()) {
			if (Elasql.partitionMetaMgr().isFullyReplicated(res))
				continue;

			readRecPerPart[graph.getResourcePosition(res).getPartId()]++;
		}

		return readRecPerPart;
	}
}

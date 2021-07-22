package org.elasql.schedule.tpart;

import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

public class CostAwareNodeInserter implements BatchNodeInserter {
	
	public static final double BETA;
	
	protected static final long CHANGE_TX_NUM = 2000000;
//	protected boolean isScalingOut = false;
//	protected boolean isConsolidating = false;

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
	
//	private int[] warehouses = new int[400];

	/**
	 * Insert this node to the partition that will result in minimal cost.
	 */
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		// XXX: Check warehouse distributions
//		Arrays.fill(warehouses, 0);
		
		// Sequentially insert each node
		for (TPartStoredProcedureTask task : tasks) {
			insertNode(graph, task);
			
			// XXX: Check warehouse distributions
//			for (RecordKey key : task.getReadSet()) {
//				if (key.getTableName().equals("warehouse")) {
//					int wid = (Integer) key.getKeyVal("w_id").asJavaVal();
//					warehouses[wid]++;
//				}
//			}
		}
		
		// XXX: Check warehouse distributions
//		StringBuilder sb = new StringBuilder("Warehouses => ");
//		for (int i = 0; i < warehouses.length; i++) {
//			if (warehouses[i] != 0)
//				sb.append(String.format("%d: %d, ", i, warehouses[i]));
//		}
//		System.out.println(sb.toString());
			
		
		// Reset the statistics
		for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++)
			loadPerPart[partId] = 0.0;
	}
		
	private void insertNode(TGraph graph, TPartStoredProcedureTask task) {
		// for scaling-out experiments
//		if (!isScalingOut && task.getTxNum() >= CHANGE_TX_NUM) {
//			isScalingOut = true;
//			System.out.println("Start scaling out at " + 
//					(System.currentTimeMillis() - Elasql.START_TIME_MS) + " ms");
//		}
		// for consolidation experiments
//		if (!isConsolidating && task.getTxNum() >= CHANGE_TX_NUM) {
//			isConsolidating = true;
//			System.out.println("Start consolidation at " + 
//					(System.currentTimeMillis() - Elasql.START_TIME_MS) + " ms");
//		}
		
		// Evaluate the cost on each part
		double minCost = Double.MAX_VALUE;
		int minCostPart = 0;
		
		for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++) {
			// for scaling-out experiments
//			if (!isScalingOut && partId > 2)
//				break;
			// for consolidation experiments
//			if (isConsolidating && partId > 2)
//				break;
			
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
		for (PrimaryKey key : task.getReadSet()) {
			
			// Skip replicated records
			if (partMgr.isFullyReplicated(key))
				continue;
			
			if (graph.getResourcePosition(key).getPartId() != targetPart) {
				crossEdgeCost++;
			}
		}
		
		// count write-back edges
//		for (RecordKey key : task.getWriteSet()) {
//			if (partMgr.getPartition(key) != targetPart) {
//				crossEdgeCost++;
//			}
//		}
		
		double loadCost = loadPerPart[targetPart] + task.getWeight();

		return truncate(loadCost * (1 - BETA) + crossEdgeCost * BETA, 4);
	}
}

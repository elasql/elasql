package org.elasql.schedule.tpart;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class HeuristicNodeInserter implements NodeInserter {

	/**
	 * Insert this node to the partition that will result in minimal cost.
	 */
	public void insert(TGraph graph, CostEstimator costEstimator, TPartStoredProcedureTask task) {
		double minCost = Double.MAX_VALUE;
		int minCostPart = 0;
		
		//===
//		StringBuilder sb = null;
//		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
//		for (RecordKey key : node.getTask().getReadSet())
//			if (partMgr.getPartition(key) == 0) {
//				sb = new StringBuilder();
//				break;
//			}
//		if (sb == null)
//			for (RecordKey key : node.getTask().getWriteSet())
//				if (partMgr.getPartition(key) == 0) {
//					sb = new StringBuilder();
//					break;
//				}
//		
//		if (sb != null) {
//			showReadWriteSet(graph, sb, node);
//			sb.append("costs: [");
//		}
		//===
		
		for (int partId = 0; partId < PartitionMetaMgr.NUM_PARTITIONS; partId++) {
			double cost = costEstimator.estimateTaskCostOnPart(graph, task, partId);
			if (cost < minCost) {
				minCost = cost;
				minCostPart = partId;
			}
			
//			if (sb != null) {
//				sb.append("" + cost + ", ");
//			}
		}
		
//		if (sb != null) {
//			sb.delete(sb.length() - 2, sb.length());
//			sb.append("], final choose: " + partId);
//			System.out.println(sb.toString());
//		}
		
		costEstimator.updateAddNodeCost(graph, task, minCostPart);
		graph.insertTxNode(task, minCostPart);
	}
	
//	private void showReadWriteSet(TGraph graph, StringBuilder sb, Node node) {
//		sb.append("Tx " + node.getTxNum() + " reads [");
//		
//		for (RecordKey key : node.getTask().getReadSet()) {
//			long source = graph.getResourcePosition(key).getPartId();
//			long ycsbId = Long.parseLong((String) key.getKeyVal("ycsb_id").asJavaVal());
//			sb.append(String.format("(%d, %d), ", ycsbId, source));
//		}
//		sb.delete(sb.length() - 2, sb.length());
//		sb.append("], ");
//		
//		if (node.getTask().getWriteSet().size() > 0)
//			sb.append("ITS RW Tx, ");
//	}
}

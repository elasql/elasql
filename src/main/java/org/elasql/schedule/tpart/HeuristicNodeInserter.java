package org.elasql.schedule.tpart;

public class HeuristicNodeInserter implements NodeInserter {

	public HeuristicNodeInserter() {
	}

	/**
	 * Insert this node to the partition that will result in minimal cost.
	 */
	public void insert(TGraph graph, Node node) {
		double minCost = Double.MAX_VALUE;
		int partId = 0;
		
		// first count the number of record for each partition and cache
		// them in the node
		node.setPartRecordCntArray(TPartPartitioner.costFuncCal.calPartitionRecordCount(node, graph));
		
		for (int p = 0; p < TPartPartitioner.NUM_PARTITIONS; p++) {
			node.setPartId(p);
			double cost = TPartPartitioner.costFuncCal.calAddNodeCost(node,
					graph);
			if (cost < minCost) {
				minCost = cost;
				partId = p;
			}
		}
		node.setPartId(partId);
		TPartPartitioner.costFuncCal.updateAddNodeCost(node, graph);
		graph.insertNode(node);
	}
}

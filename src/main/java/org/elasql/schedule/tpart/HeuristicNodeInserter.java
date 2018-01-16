package org.elasql.schedule.tpart;

import org.elasql.storage.metadata.PartitionMetaMgr;

public class HeuristicNodeInserter implements NodeInserter {

	public HeuristicNodeInserter() {
	}

	/**
	 * Insert this node to the partition that will result in minimal cost.
	 */
	public void insert(TGraph graph, Node node) {
		double minCost = Double.MAX_VALUE;
		int partId = 0;

		for (int p = 0; p < PartitionMetaMgr.NUM_PARTITIONS; p++) {
			node.setPartId(p);
			double cost = TPartPartitioner.costFuncCal.calAddNodeCost(node, graph);
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

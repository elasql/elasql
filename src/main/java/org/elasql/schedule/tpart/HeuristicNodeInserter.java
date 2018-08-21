/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
		
		// first count the number of record for each partition and cache
		// them in the node
		node.setPartRecordCntArray(TPartPartitioner.costFuncCal.calPartitionRecordCount(node, graph));
		
		for (int p = 0; p < PartitionMetaMgr.NUM_PARTITIONS; p++) {
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

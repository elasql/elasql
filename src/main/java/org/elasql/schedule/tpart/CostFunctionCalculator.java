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

import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

public class CostFunctionCalculator {
	private static final double BETA;
	private double[] partLoads = new double[PartitionMetaMgr.NUM_PARTITIONS];
	private double totalLoads = 0; // used to speed up the sum of partLoads
	public int crossEdgeCount;

	static {
		BETA = ElasqlProperties.getLoader().getPropertyAsDouble(CostFunctionCalculator.class.getName() + ".BETA", 1.0);
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
	 */
	public void updateRemoveNodeCost(Node removeNode, TGraph graph) {
		updateCost(removeNode, graph, false);
	}

	/**
	 * Calculate the cost with a node added. ! The real cost will not be updated
	 * by this method.
	 */
	public double calAddNodeCost(Node newNode, TGraph graph) {
		// calculate cross partition edge cost
		double edgeCost = 0;
		for (int part = 0; part < PartitionMetaMgr.NUM_PARTITIONS; part++) {
			if (part != newNode.getPartId())
				edgeCost += newNode.getPartRecordCntArray()[part];
		}

		// calculate partition load cost
		double averageLoad = (totalLoads + newNode.getWeight()) / PartitionMetaMgr.NUM_PARTITIONS;
		double loadCost = 0;

		for (int part = 0; part < PartitionMetaMgr.NUM_PARTITIONS; part++) {
			if (part == newNode.getPartId())
				loadCost += Math.abs(partLoads[part] + newNode.getWeight() - averageLoad);
			else
				loadCost += Math.abs(partLoads[part] - averageLoad);
		}

		return truncate(loadCost * (1 - BETA) + edgeCost * BETA, 4);
	}

	/**
	 * Calculate the cost with a node removed. ! The real cost will not be
	 * updated by this method.
	 */
	public double calRemoveNodeCost(Node removeNode, TGraph graph) {
		updateCost(removeNode, graph, false);
		double cost = getCost();
		updateCost(removeNode, graph, true);
		return cost;
	}

	/**
	 * Calculate the number of record for each partition as an array
	 */
	public int[] calPartitionRecordCount(Node node, TGraph graph) {
		int[] recordsPerPart = new int[PartitionMetaMgr.NUM_PARTITIONS];

		for (RecordKey res : node.getTask().getReadSet()) {
			if (Elasql.partitionMetaMgr().isFullyReplicated(res))
				continue;

			recordsPerPart[graph.getResourcePosition(res).getPartId()]++;
		}

		return recordsPerPart;
	}

	/**
	 * Get the cost of current cost function state.
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

	private double truncate(double number, int precision) {
		double prec = Math.pow(10, precision);
		int integerPart = (int) number;
		double fractionalPart = number - integerPart;
		fractionalPart *= prec;
		int fractPart = (int) fractionalPart;
		fractionalPart = (double) (integerPart) + (double) (fractPart) / prec;
		return fractionalPart;
	}

	public double[] getPartLoads() {
		return partLoads;
	}
}

package org.elasql.schedule.tpart;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class LapCostEstimator extends CostEstimator {
	
	private static class UseCount {
		
		private int count;
		
		UseCount(int count) {
			this.count = count;
		}
		
		int get() {
			return count;
		}
		
		void increment() {
			count++;
		}
		
		void decrement() {
			count--;
		}
	}
	
	private double[] partLoads;
	private Map<RecordKey, UseCount> useCounts;
	
	@Override
	public void reset() {
		if (partLoads == null)
			partLoads = new double[PartitionMetaMgr.NUM_PARTITIONS];
		if (useCounts == null)
			useCounts = new HashMap<RecordKey, UseCount>();
		
		for (int i = 0; i < partLoads.length; i++)
			partLoads[i] = 0;
		useCounts.clear();
	}
	
	// Analyze batch for looking-ahead when calculate the costs
	public void analyzeBatch(List<TPartStoredProcedureTask> batch) {
		for (TPartStoredProcedureTask task : batch) {
			for (RecordKey readKey : task.getReadSet()) {
				UseCount count = useCounts.get(readKey);
				if (count == null) {
					count = new UseCount(0);
					useCounts.put(readKey, count);
				}
				count.increment();
			}
		}
	}
	
	@Override
	public double estimateTaskCostOnPart(TGraph graph, TPartStoredProcedureTask task, int assignedPart) {
		// calculate cross partition edge cost
		double crossEdgeCost = 0;
		for (RecordKey key : task.getReadSet()) {
			if (graph.getResourcePosition(key).getPartId() != assignedPart) {
				UseCount count = useCounts.get(key);
				
				if (count == null) {
					throw new RuntimeException("We do not have use count for " + key);
				}
				
				crossEdgeCost += count.get();
			}
		}

		// calculate partition load cost
		double loadCost = partLoads[assignedPart] + task.getWeight();

		return truncate(loadCost * (1 - BETA) + crossEdgeCost * BETA, 4);
	}
	
	@Override
	public void updateAddNodeCost(TGraph graph, TPartStoredProcedureTask task, int assignedPartId) {
		// Update use counts of resource
		for (RecordKey key : task.getReadSet()) {
			UseCount count = useCounts.get(key);
			
			if (count == null) {
				throw new RuntimeException("We do not have use count for " + key);
			}
			
			count.decrement();
		}
		
		partLoads[assignedPartId] += task.getWeight();
	}
}

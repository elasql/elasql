package org.elasql.migration.planner.clay;

import java.util.Iterator;
import java.util.Map;

import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;

public class ScatterPartitionPlan extends PartitionPlan {
	
	private PartitionPlan basePartition;
	private Map<RecordKey, Integer> scatterPartition; // <Partitioning Key -> Partition ID>
	
	public ScatterPartitionPlan(PartitionPlan basePartition, Map<RecordKey, Integer> scatterPartition) {
		this.basePartition = basePartition;
		this.scatterPartition = scatterPartition;
	}

	@Override
	public boolean isFullyReplicated(RecordKey key) {
		return basePartition.isFullyReplicated(key);
	}

	@Override
	public int getPartition(RecordKey key) {
		RecordKey partKey = basePartition.getPartitioningKey(key);
		Integer part = scatterPartition.get(partKey);
		if (part != null)
			return part;
		return basePartition.getPartition(key);
	}

	@Override
	public PartitionPlan getBasePlan() {
		return basePartition;
	}

	@Override
	public void setBasePlan(PartitionPlan plan) {
		basePartition = plan;
	}
	
	public Map<RecordKey, Integer> getMapping() {
		return scatterPartition;
	}
	
	@Override
	public String toString() {
		// Sample some records
		StringBuilder sb = new StringBuilder();
		Iterator<Map.Entry<RecordKey, Integer>> iter = scatterPartition.entrySet().iterator();
		int count = 0;
		while (count < 5 && iter.hasNext()) {
			count++;
			Map.Entry<RecordKey, Integer> entry = iter.next();
			RecordKey key = entry.getKey();
			Integer part = entry.getValue();
			if (count > 1) {
				sb.append(", ");
			}
			sb.append(String.format("%s = %d", key, part));
		}
		
		return String.format("Scatter Partition Plan (base plan: %s, except %d keys: [samples: %s])",
				basePartition.toString(), scatterPartition.size(), sb.toString());
	}

	@Override
	public RecordKey getPartitioningKey(RecordKey key) {
		return basePartition.getPartitioningKey(key);
	}
}

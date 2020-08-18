package org.elasql.migration.planner.clay;

import java.util.Iterator;
import java.util.Map;

import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionPlan;

public class ScatterPartitionPlan extends PartitionPlan {
	
	private PartitionPlan basePartition;
	private Map<PrimaryKey, Integer> scatterPartition; // <Partitioning Key -> Partition ID>
	
	public ScatterPartitionPlan(PartitionPlan basePartition, Map<PrimaryKey, Integer> scatterPartition) {
		this.basePartition = basePartition;
		this.scatterPartition = scatterPartition;
	}

	@Override
	public boolean isFullyReplicated(PrimaryKey key) {
		return basePartition.isFullyReplicated(key);
	}

	@Override
	public int getPartition(PrimaryKey key) {
		PrimaryKey partKey = basePartition.getPartitioningKey(key);
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
	
	public Map<PrimaryKey, Integer> getMapping() {
		return scatterPartition;
	}
	
	@Override
	public String toString() {
		// Sample some records
		StringBuilder sb = new StringBuilder();
		Iterator<Map.Entry<PrimaryKey, Integer>> iter = scatterPartition.entrySet().iterator();
		int count = 0;
		while (count < 5 && iter.hasNext()) {
			count++;
			Map.Entry<PrimaryKey, Integer> entry = iter.next();
			PrimaryKey key = entry.getKey();
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
	public PrimaryKey getPartitioningKey(PrimaryKey key) {
		return basePartition.getPartitioningKey(key);
	}
}

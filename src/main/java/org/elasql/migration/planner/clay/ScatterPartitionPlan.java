package org.elasql.migration.planner.clay;

import java.util.Map;

import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;

public class ScatterPartitionPlan extends PartitionPlan {
	
	private PartitionPlan basePartition;
	private Map<RecordKey, Integer> scatterPartition;
	
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
		Integer part = scatterPartition.get(key);
		if (part != null)
			return part; 
		return basePartition.getPartition(key);
	}
	
	@Override
	public String toString() {
		return String.format("Scatter Partition Plan (base plan: %s, except: %s)",
				basePartition.toString(), scatterPartition);
	}

	@Override
	public PartitionPlan getBasePlan() {
		return basePartition;
	}

	@Override
	public void setBasePlan(PartitionPlan plan) {
		basePartition = plan;
	}
}

package org.elasql.schedule.tpart.bandit;

import org.elasql.perf.tpart.bandit.BanditRewardUpdateProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.TGraph;

import java.util.List;

public class PuppetInserter implements BatchNodeInserter {
	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			if (task.getProcedure().getClass().equals(BanditRewardUpdateProcedure.class)) {
				continue;
			}
			graph.insertTxNode(task, task.getAssignedPartition());
		}
	}
}

package org.elasql.schedule.tpart.bandit;

import org.elasql.perf.tpart.bandit.BanditRewardUpdateProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.schedule.tpart.hermes.HermesNodeInserter;

import java.util.List;

public class PuppetInserter extends HermesNodeInserter {
	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			if (task.getProcedure().getClass().equals(BanditRewardUpdateProcedure.class)) {
				continue;
			}
			graph.insertTxNode(task, task.getRoute());
		}
		
		// Debug: show the distribution of assigned masters
		for (TxNode node : graph.getTxNodes())
			assignedCounts[node.getPartId()]++;
		reportRoutingDistribution(tasks.get(0).getArrivedTime());
	}
}

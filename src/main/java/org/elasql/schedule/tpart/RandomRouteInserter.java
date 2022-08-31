package org.elasql.schedule.tpart;

import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class RandomRouteInserter implements BatchNodeInserter {
	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			int route = (int) (task.getTxNum() % PartitionMetaMgr.NUM_PARTITIONS);
			graph.insertTxNode(task, route);
		}
	}
}

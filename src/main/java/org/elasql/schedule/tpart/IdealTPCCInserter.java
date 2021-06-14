package org.elasql.schedule.tpart;

import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.sql.PrimaryKey;

public class IdealTPCCInserter implements BatchNodeInserter {
	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			int partId = -1;
			
			for (PrimaryKey key : task.getReadSet()) {
				if (key.getTableName().equals("warehouse")) {
					int wid = (Integer) key.getVal("w_id").asJavaVal();
					if (wid <= 18)
						partId = wid - 1;
					else
						partId = (wid - 1) / 20;
					System.out.println(String.format("Tx.%d for warehouse %d to part.%d.",
							task.getTxNum(), wid, partId));
					break;
				}
			}
			
			if (partId == -1)
				throw new RuntimeException("Something wrong");
			graph.insertTxNode(task, partId);
		}
	}
}

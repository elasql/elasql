package org.elasql.server.migration;

import java.util.Collection;
import java.util.LinkedList;

import org.elasql.server.Elasql;
import org.elasql.server.migration.heatgraph.HeatGraph;
import org.elasql.sql.RecordKey;

public class WorkloadMonitor {

	private MigrationManager migraMgr;
	private HeatGraph heatGraph = new HeatGraph();
	
	public WorkloadMonitor(MigrationManager migraMgr) {
		this.migraMgr = migraMgr;
	}
	
	public void recordATransaction(Collection<RecordKey> accessedKeys) {
		LinkedList<Integer> vertexIdSet = new LinkedList<Integer>();
		Integer vetxId;
		int partId;
		// Since only the sequence node can get in, remoteReadKeys basically
		// contains all the read keys of the transaction.
		for (RecordKey k : accessedKeys) {
			vetxId = mapToVertexId(migraMgr.keyToInteger(k));
			partId = Elasql.partitionMetaMgr().getPartition(k);

			heatGraph.updateWeightOnVertex(vetxId, partId);
			vertexIdSet.add(vetxId);
		}
		heatGraph.updateWeightOnEdges(vertexIdSet);
	}
	
	// E.g. 1~10 => 0, 11~20 => 1
	public int mapToVertexId(int key) {
		return (key - 1) / MigrationManager.DATA_RANGE_SIZE;
	}
	
	/**
	 * Retrieve the resulted heat graph and reset the status of the monitor.
	 * 
	 * @return
	 */
	public HeatGraph retrieveHeatGraphAndReset() {
		HeatGraph h = heatGraph;
		heatGraph = new HeatGraph();
		return h;
	}
}

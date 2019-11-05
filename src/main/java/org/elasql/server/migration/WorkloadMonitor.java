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
		LinkedList<RecordKey> vertexKeySet = new LinkedList<RecordKey>();
		RecordKey vertexKey;
		int partId;
		// Since only the sequence node can get in, remoteReadKeys basically
		// contains all the read keys of the transaction.
		for (RecordKey k : accessedKeys) {
			vertexKey = migraMgr.getRepresentative(k);
			partId = Elasql.partitionMetaMgr().getPartition(k);

			heatGraph.updateWeightOnVertex(vertexKey, partId);
			vertexKeySet.add(vertexKey);
		}
		heatGraph.updateWeightOnEdges(vertexKeySet);
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

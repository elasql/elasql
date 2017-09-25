package org.elasql.schedule.tpart;

import java.util.Map.Entry;
import java.util.Set;

import org.elasql.procedure.tpart.TPartCacheWriteBackProc;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class SupaTGraph extends TGraph {
	public SupaTGraph() {
		super();
		System.out.println("LAP running");
	}

	@Override
	/**
	 * Write back to where TGraph assigned
	 */
	public void addWriteBackEdge() {
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
		
		// Add a node for handling overflowed keys in the location table
		Set<RecordKey> overflowedKeys = partMgr.chooseOverflowedKeys();
		if (overflowedKeys != null && overflowedKeys.size() > 0) {
			
			// Insert a node to handle the removed keys
			TPartStoredProcedure<?> writeBackProc = new TPartCacheWriteBackProc(overflowedKeys);
			writeBackProc.prepare();
			TPartStoredProcedureTask task = new TPartStoredProcedureTask(-1, -1, writeBackProc.getTxNum(),
					writeBackProc);
			Node node = new Node(task);
			node.setPartId(0);
			this.insertNode(node);
			
			// Let the node write back the data of removed keys to the original position
			for (RecordKey key : overflowedKeys) {
				int originalLocation = partMgr.getLocation(key);
				node.addWriteBackEdges(new Edge(sinkNodes[originalLocation], key));
				
				// remove the handled resource from the map
				resPos.remove(key);
			}
		}
		
		// Add write back edges to other nodes
		for (Entry<RecordKey, Node> resPosPair : resPos.entrySet()) {
			RecordKey res = resPosPair.getKey();
			Node node = resPosPair.getValue();

			// null means it is sink node
			if (node.getTask() != null)
				node.addWriteBackEdges(new Edge(sinkNodes[node.getPartId()], res));
		}
		
		// Clear the resource map for the next run
		resPos.clear();
	}
}

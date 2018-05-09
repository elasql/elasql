package org.elasql.schedule.tpart.graph;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class LapTGraph extends TGraph {
	
	public LapTGraph() {
		super();
	}

	@Override
	/**
	 * Write back to where TGraph assigned
	 */
	public void addWriteBackEdge() {
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
		
		// Get the overflowed keys that need to be placed back to the original locations
		Set<RecordKey> overflowedKeys = partMgr.chooseOverflowedKeys();
		if (overflowedKeys != null && overflowedKeys.size() > 0) {
			
			// Make each key that will be processed in this graph
			// be written back to the original location by the last one using it
			Set<RecordKey> noOneHandledKeys = new HashSet<RecordKey>();
			for (RecordKey key : overflowedKeys) {
				TxNode handler = resPos.remove(key);
				if (handler != null) {
					int originalLocation = partMgr.getPartition(key);
					handler.addWriteBackEdges(new Edge(sinkNodes[originalLocation], key));
				} else
					noOneHandledKeys.add(key);
			}
			
			// For the keys that on one handles, let the last node read and write them back.
			TxNode lastNode = getLastInsertedTxNode();
			for (RecordKey key : noOneHandledKeys) {
				int originalLocation = partMgr.getPartition(key);
				lastNode.addReadEdges(new Edge(getResourcePosition(key), key));
				lastNode.addWriteBackEdges(new Edge(sinkNodes[originalLocation], key));
			}
		}
		
		// Put the rest of the records on where they are
		for (Entry<RecordKey, TxNode> resPosPair : resPos.entrySet()) {
			RecordKey res = resPosPair.getKey();
			TxNode node = resPosPair.getValue();
			node.addWriteBackEdges(new Edge(sinkNodes[node.getPartId()], res));
		}
		
		// Clear the resource map for the next run
		resPos.clear();
	}
}

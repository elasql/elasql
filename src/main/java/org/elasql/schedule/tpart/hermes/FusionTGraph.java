package org.elasql.schedule.tpart.hermes;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.elasql.schedule.tpart.graph.Edge;
import org.elasql.schedule.tpart.graph.Node;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.sql.PrimaryKey;

public class FusionTGraph extends TGraph {
	
	private FusionTable fusionTable;
	
	public FusionTGraph(FusionTable table) {
		fusionTable = table;
	}

	/**
	 * Write back to where TGraph assigned
	 */
	@Override
	public void addWriteBackEdge() {
		// Get the overflowed keys that need to be placed back to the original locations
		Set<PrimaryKey> overflowedKeys = fusionTable.getOverflowKeys();
		if (overflowedKeys != null && overflowedKeys.size() > 0) {
			
			// Make each key that will be processed in this graph
			// be written back to the original location by the last one using it
			Set<PrimaryKey> noOneHandledKeys = new HashSet<PrimaryKey>();
			for (PrimaryKey key : overflowedKeys) {
				TxNode handler = resPos.remove(key);
				if (handler != null) {
					int originalLocation = parMeta.getPartition(key);
					handler.addWriteBackEdges(new Edge(sinkNodes[originalLocation], key));
				} else
					noOneHandledKeys.add(key);
			}
			
			// For the keys that on one handles, let the last node read and write them back.
			TxNode lastNode = getLastInsertedTxNode();
			for (PrimaryKey key : noOneHandledKeys) {
				int originalLocation = parMeta.getPartition(key);
				lastNode.addReadEdges(new Edge(getResourcePosition(key), key));
				lastNode.addWriteBackEdges(new Edge(sinkNodes[originalLocation], key));
			}
		}
		
		// Handle the rest of written records
		for (Entry<PrimaryKey, TxNode> resPosPair : resPos.entrySet()) {
			PrimaryKey res = resPosPair.getKey();
			TxNode node = resPosPair.getValue();
			
			// Quick fix: ignore insert-only tables
//			if (res.getTableName().equals("orders") || res.getTableName().equals("new_order") ||
//					res.getTableName().equals("order_line"))
//				node.addWriteBackEdges(new Edge(sinkNodes[parMeta.getPartition(res)], res));
//			else
			// Put the records on where they are
				node.addWriteBackEdges(new Edge(sinkNodes[node.getPartId()], res));
		}
		
		// Clear the resource map for the next run
		resPos.clear();
	}
	
	/**
	 * Hermes queries the fusion table to determine the location of the data.
	 */
	@Override
	public Node getResourcePosition(PrimaryKey res) {
		if (resPos.containsKey(res))
			return resPos.get(res);
		
		// Query the fusion table
		int location = fusionTable.getLocation(res);
		if (location != -1)
			return sinkNodes[location];
		
		return sinkNodes[parMeta.getPartition(res)];
	}
}

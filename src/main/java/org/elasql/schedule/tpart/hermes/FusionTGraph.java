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
		// == Handles overflowed keys ==
		// Check if there is a transaction can handle overflowed keys
		TxNode lastTxNode = getLastInsertedTxNode();
		if (lastTxNode != null) {
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
				for (PrimaryKey key : noOneHandledKeys) {
					int originalLocation = parMeta.getPartition(key);
					lastTxNode.addReadEdges(new Edge(getResourcePosition(key), key));
					lastTxNode.addWriteBackEdges(new Edge(sinkNodes[originalLocation], key));
				}
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

	@Override
	public void clear() {
		for (TxNode node : getTxNodes()) {
			for (Edge e : node.getWriteBackEdges()) {
				PrimaryKey key = e.getResourceKey();
				int writeBackPos = e.getTarget().getPartId();
				setRecordCurrentLocation(key, writeBackPos);
			}
		}
		
		super.clear();
	}
	
	public int getCachedLocation(PrimaryKey key) {
		return fusionTable.getLocation(key);
	}
	
	public int getFusionTableOverflowCount() {
		return fusionTable.getOverflowKeys().size();
	}
	
	private void setRecordCurrentLocation(PrimaryKey key, int loc) {
		if (parMeta.getPartition(key) == loc) {
			if (fusionTable.containsKey(key)) {
				fusionTable.remove(key);
			}
		} else
			fusionTable.setLocation(key, loc);
	}
}

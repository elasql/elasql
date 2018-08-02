package org.elasql.server.migration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.storage.metadata.PartitionMetaMgr;

public class HeatGraph {
	
	private Map<Integer, Vertex> vertices = new HashMap<Integer, Vertex>(1000000);
	
	public void updateWeightOnVertex(Integer vetxId, int partId) {
		Vertex vertex = vertices.get(vetxId);
		// Note that a vertex represents a range of records.
		if (vertex == null)
			vertices.put(vetxId, new Vertex(vetxId, partId));
		else
			vertex.incrementWeight();
	}
	
	// Update weights for co-accessed vertices
	public void updateWeightOnEdges(Collection<Integer> coaccessedVertices) {
		for (int i : coaccessedVertices)
			for (int j : coaccessedVertices)
				if (i != j)
					vertices.get(i).addEdge(j, vertices.get(j).getPartId());
	}
	
	public List<Partition> splitToPartitions() {
		List<Partition> partitions = new ArrayList<Partition>();
		for (int i = 0; i < PartitionMetaMgr.NUM_PARTITIONS; i++)
			partitions.add(new Partition(i));

		for (Vertex e : vertices.values())
			partitions.get(e.getPartId()).addVertex(e);
		
		return partitions;
	}
	
	public Vertex getVertex(Integer id) {
		return vertices.get(id);
	}
}

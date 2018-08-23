package org.elasql.server.migration.heatgraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.server.migration.MigrationManager;
import org.elasql.server.migration.clay.Partition;

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
					vertices.get(i).addEdgeTo(vertices.get(j));
	}
	
	public List<Partition> splitToPartitions() {
		List<Partition> partitions = new ArrayList<Partition>();
		for (int i = 0; i < MigrationManager.currentNumOfPartitions(); i++)
			partitions.add(new Partition(i));

		for (Vertex v : vertices.values())
			partitions.get(v.getPartId()).addVertex(v);
		
		return partitions;
	}
	
	public void removeVertex(Vertex v) {
		vertices.remove(v.getId());
		for (OutEdge e : v.getOutEdges().values())
			e.getOpposite().getOutEdges().remove(v.getId());
	}
	
	public Vertex getVertex(Integer id) {
		return vertices.get(id);
	}
}

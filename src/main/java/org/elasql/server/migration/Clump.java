package org.elasql.server.migration;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasql.server.migration.heatgraph.OutEdge;
import org.elasql.server.migration.heatgraph.Vertex;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class Clump {

	private class Neighbor implements Comparable<Neighbor> {
		long weight;
		int id;
		int partId;

		public Neighbor(OutEdge neighborEdge) {
			this.id = neighborEdge.opposite().getId();
			this.partId = neighborEdge.opposite().getPartId();
			this.weight = neighborEdge.weight();
		}

		@Override
		public int compareTo(Neighbor other) {
			if (this.weight > other.weight)
				return 1;
			else if (this.weight < other.weight)
				return -1;
			return 0;
		}
		
		@Override
		public String toString() {
			return "<" + id + " on " + partId + " with " + weight +
					" weight>";
		}
	}

	private Map<Integer, Vertex> vertices;
	private Map<Integer, Neighbor> neighbors;
//	private double load;

	public Clump(Vertex initVertex) {
//		this.load = 0;
		this.vertices = new HashMap<Integer, Vertex>();
		this.neighbors = new HashMap<Integer, Neighbor>();
		
		addVertex(initVertex);
	}
	
	public void expand(Vertex neighbor) {
		if (neighbors.remove(neighbor.getId()) == null) {
			throw new RuntimeException("There is no neighbor with id " +
					neighbor.getId() + " in the clump.");
		}
		addVertex(neighbor);
	}

	public int getHotestNeighbor() {
		return Collections.max(neighbors.values()).id;
	}
	
	public boolean hasNeighbor() {
		return !neighbors.isEmpty();
	}
	
	public int size() {
		return vertices.size();
	}
	
	public Collection<Vertex> getVertices() {
		return vertices.values();
	}
	
	public List<MigrationPlan> toMigrationPlans(int destPart) {
		MigrationPlan[] sources = new MigrationPlan[PartitionMetaMgr.NUM_PARTITIONS];
		for (int i = 0; i < sources.length; i++)
			sources[i] = new MigrationPlan(i, destPart);
		
		// Put the vertices to the plans
		for (Vertex v : vertices.values())
			sources[v.getPartId()].addKey(v.getId());
		
		List<MigrationPlan> candidatePlans = new LinkedList<MigrationPlan>();
		
		for (int i = 0; i < sources.length; i++)
			if (i != destPart && sources[i].keyCount() > 0)
				candidatePlans.add(sources[i]);
		
		return candidatePlans;
	}

	@Override
	public String toString() {
//		String str = "Load : " + this.load + " Neighbor : " + neighbors.size() + "\n";
//		str += "Candidate Id : ";
//		ArrayList<Integer> cc = new ArrayList<Integer>(candidateIds);
//		Collections.sort(cc);
//		for (int id : cc)
//			str += ", " + id;
		/*
		 * for (int id : candidateIds) if (id*MigrationManager.dataRange >
		 * 100000) str += "Somethigs Wrong Tuple " + id + "in candidateIds\n";
		 */
//		str += "\n";
//		return str;
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("{vertices: [");
		sb.append(vertices);
		sb.append("], neighbor: [");
		sb.append(neighbors);
		sb.append("]}");

		return sb.toString();
	}

	private void addVertex(Vertex v) {
//		load += v.getVertexWeight();
		vertices.put(v.getId(), v);
		
		// Simplified version: it does not consider 
		// the vertices on other partition nodes.
//		for (OutEdge o : v.getEdge().values())
//			if (o.partId == v.getPartId())
//				addNeighbor(o.id, o.partId, o.weight);
		
		// Correct version: consider the vertices on all partitions
		for (OutEdge o : v.getOutEdges().values())
			addNeighbor(o);
	}

	private void addNeighbor(OutEdge neighborEdge) {
		// Avoid self loop
		if (vertices.containsKey(neighborEdge.opposite().getId()))
			return;

		Neighbor w = neighbors.get(neighborEdge.opposite().getId());
		if (w == null)
			neighbors.put(neighborEdge.opposite().getId(),
					new Neighbor(neighborEdge));
		else
			w.weight += neighborEdge.weight();
	}
}

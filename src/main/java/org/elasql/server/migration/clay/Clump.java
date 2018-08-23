package org.elasql.server.migration.clay;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasql.server.migration.MigrationManager;
import org.elasql.server.migration.MigrationPlan;
import org.elasql.server.migration.heatgraph.OutEdge;
import org.elasql.server.migration.heatgraph.Vertex;

public class Clump {

	private class Neighbor implements Comparable<Neighbor> {
		double weight;
		int id;
		int partId;

		public Neighbor(OutEdge neighborEdge) {
			this.id = neighborEdge.getOpposite().getId();
			this.partId = neighborEdge.getOpposite().getPartId();
			this.weight = neighborEdge.getWeight();
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
	private int destPartitionId = -1;

	public Clump(Vertex initVertex) {
		this.vertices = new HashMap<Integer, Vertex>();
		this.neighbors = new HashMap<Integer, Neighbor>();
		
		addVertex(initVertex);
	}

	public Clump(Clump clump) {
		this.vertices = new HashMap<Integer, Vertex>();
		this.neighbors = new HashMap<Integer, Neighbor>();
		this.destPartitionId = clump.destPartitionId;
		
		for (Vertex v : clump.vertices.values())
			addVertex(v);
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
	
	public void setDestination(int partId) {
		destPartitionId = partId;
	}
	
	public int getDestination() {
		return destPartitionId;
	}
	
	public List<MigrationPlan> toMigrationPlans() {
		if (destPartitionId == -1)
			throw new RuntimeException("The destination has not been decided yet.");
		
		MigrationPlan[] sources = new MigrationPlan[MigrationManager.currentNumOfPartitions()];
		for (int i = 0; i < sources.length; i++)
			sources[i] = new MigrationPlan(i, destPartitionId);
		
		// Put the vertices to the plans
		for (Vertex v : vertices.values())
			sources[v.getPartId()].addKey(v.getId());
		
		List<MigrationPlan> candidatePlans = new LinkedList<MigrationPlan>();
		
		for (int i = 0; i < sources.length; i++)
			if (i != destPartitionId && sources[i].keyCount() > 0)
				candidatePlans.add(sources[i]);
		
		return candidatePlans;
	}
	
	public int getMostCoaccessedPartition() {
		double[] coaccess = new double[MigrationManager.currentNumOfPartitions()];
		
		for (Neighbor n : neighbors.values()) {
			coaccess[n.partId] += n.weight;
		}
		
		int mostCoaccessedPart = -1;
		for (int partId = 0; partId < coaccess.length; partId++) {
			if (coaccess[partId] > 0.5) {
				if (mostCoaccessedPart == -1 ||
						coaccess[partId] > coaccess[mostCoaccessedPart]) {
					mostCoaccessedPart = partId;
				}
			}
		}
		
		return mostCoaccessedPart;
	}

	@Override
	public String toString() {
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
		for (OutEdge o : v.getOutEdges().values())
			if (o.getOpposite().getPartId() == v.getPartId())
				addNeighbor(o);
		
		// Correct version: consider the vertices on all partitions
//		for (OutEdge o : v.getOutEdges().values())
//			addNeighbor(o);
	}

	private void addNeighbor(OutEdge neighborEdge) {
		// Avoid self loop
		if (vertices.containsKey(neighborEdge.getOpposite().getId()))
			return;

		Neighbor w = neighbors.get(neighborEdge.getOpposite().getId());
		if (w == null)
			neighbors.put(neighborEdge.getOpposite().getId(),
					new Neighbor(neighborEdge));
		else
			w.weight += neighborEdge.getWeight();
	}
}

package org.elasql.migration.planner.clay;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.elasql.sql.PartitioningKey;

class Clump {

	private class Neighbor implements Comparable<Neighbor> {
		double weight;
		PartitioningKey key;
		int partId;

		Neighbor(OutEdge neighborEdge) {
			this.key = neighborEdge.getOpposite().getKey();
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
			return "<" + key + " on " + partId + " with " + weight +
					" weight>";
		}
	}

	private Map<PartitioningKey, Vertex> vertices;
	private Map<PartitioningKey, Neighbor> neighbors;
	private int destPartitionId = -1;

	Clump(Vertex initVertex) {
		this.vertices = new HashMap<PartitioningKey, Vertex>();
		this.neighbors = new HashMap<PartitioningKey, Neighbor>();
		
		addVertex(initVertex);
	}

	Clump(Clump clump) {
		this.vertices = new HashMap<PartitioningKey, Vertex>();
		this.neighbors = new HashMap<PartitioningKey, Neighbor>();
		this.destPartitionId = clump.destPartitionId;
		
		for (Vertex v : clump.vertices.values())
			addVertex(v);
	}
	
	void expand(Vertex neighbor) {
		// Expansion must follow neighbors
		if (neighbors.remove(neighbor.getKey()) == null) {
			throw new RuntimeException("There is no neighbor with key " +
					neighbor.getKey() + " in the clump.");
		}
		addVertex(neighbor);
	}

	PartitioningKey getHotestNeighbor() {
		return Collections.max(neighbors.values()).key;
	}
	
	boolean hasNeighbor() {
		return !neighbors.isEmpty();
	}
	
	int size() {
		return vertices.size();
	}
	
	Collection<Vertex> getVertices() {
		return vertices.values();
	}
	
	void setDestination(int partId) {
		destPartitionId = partId;
	}
	
	int getDestination() {
		return destPartitionId;
	}
	
	// Check if any vertex has to be migrated
	boolean needMigration() {
		for (Vertex v : vertices.values())
			if (v.getPartId() != destPartitionId)
				return true;
		return false;
	}
	
	ScatterMigrationPlan toMigrationPlan() {
		if (destPartitionId == -1)
			throw new RuntimeException("The destination has not been decided yet.");
		
		ScatterMigrationPlan plan = new ScatterMigrationPlan();
		
		// Put the vertices to the plans
		for (Vertex v : vertices.values())
			if (v.getPartId() != destPartitionId)
				plan.addPartKey(v.getKey(), v.getPartId(), destPartitionId);
		
		return plan;
	}
	
	int getMostCoaccessedPartition(int totalPartitions) {
		double[] coaccess = new double[totalPartitions];
		
		for (Neighbor n : neighbors.values()) {
			coaccess[n.partId] += n.weight;
		}
		
		int mostCoaccessedPart = -1;
		for (int partId = 0; partId < coaccess.length; partId++) {
			if (mostCoaccessedPart == -1 ||
					coaccess[partId] > coaccess[mostCoaccessedPart]) {
				mostCoaccessedPart = partId;
			}
		}
		
		return mostCoaccessedPart;
	}
	
	/**
	 * The second formula in Section 7.2 of Clay's paper.
	 */
	double calcRecvLoadDelta(int destPartId, double multiPartsCost) {
		// Added node loading
		double addedNodeLoad = 0;
		for (Vertex v : vertices.values())
			if (v.getPartId() != destPartId)
				addedNodeLoad += v.getVertexWeight();
		
		// Cross-partition edge loading
		double addedCrossLoad = 0, reducedCrossLoad = 0;
		for (Vertex v : vertices.values()) {
			if (v.getPartId() != destPartId) {
				for (OutEdge e : v.getOutEdges()) {
					Vertex u = e.getOpposite();
					if (u.getPartId() == destPartId) {
						reducedCrossLoad += e.getWeight();
					} else {
						if (!vertices.containsKey(u.getKey()))
							addedCrossLoad += e.getWeight();
					}
				}
			}
		}
		
		return addedNodeLoad + multiPartsCost *
				(addedCrossLoad - reducedCrossLoad);
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
		vertices.put(v.getKey(), v);
		
		// Simplified version: it does not consider 
		// the vertices on other partition nodes.
//		for (OutEdge o : v.getOutEdges().values())
//			if (o.getOpposite().getPartId() == v.getPartId())
//				addNeighbor(o);
		
		// Correct version: consider the vertices on all partitions
		for (OutEdge o : v.getOutEdges())
			addNeighbor(o);
	}

	private void addNeighbor(OutEdge neighborEdge) {
		// Avoid loops
		if (vertices.containsKey(neighborEdge.getOpposite().getKey()))
			return;

		Neighbor n = neighbors.get(neighborEdge.getOpposite().getKey());
		if (n == null)
			neighbors.put(neighborEdge.getOpposite().getKey(),
					new Neighbor(neighborEdge));
		else
			n.weight += neighborEdge.getWeight();
	}
}

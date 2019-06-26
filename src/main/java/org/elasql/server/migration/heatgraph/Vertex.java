package org.elasql.server.migration.heatgraph;

import java.util.HashMap;

import org.elasql.server.migration.MigrationManager;

public class Vertex implements Comparable<Vertex> {
	
	private HashMap<Integer, OutEdge> edges;
	private int weight;
	private int id;
	private int partId;

	public Vertex(int id, int partId) {
		this(id, partId, 1);
	}

	Vertex(int id, int partId, int weight) {
		this.id = id;
		this.weight = weight;
		this.partId = partId;
		edges = new HashMap<Integer, OutEdge>();
	}

	public int getId() {
		return id;
	}

	public void setPartId(int part) {
		this.partId = part;
	}

	public int getPartId() {
		return partId;
	}

	public void incrementWeight() {
		this.weight++;
	}

	public void addEdgeTo(Vertex opposite) {
		OutEdge e = edges.get(opposite.getId());

		if (e == null)
			edges.put(opposite.getId(), new OutEdge(opposite));
		else
			e.incrementWeight();
	}

	void addEdgeWithWeight(Vertex opposite, int weight) {
		edges.put(opposite.getId(), new OutEdge(opposite, weight));
	}

	public void clear() {
		this.weight = 0;
		edges.clear();
	}

	public int getVertexWeight() {
		return weight;
	}
	
	public double getNormalizedVertexWeight() {
		return (double) weight / MigrationManager.MONITORING_TIME;
	}

	public int getEdgeWeight() {
		int w = 0;
		for (OutEdge e : edges.values())
			w += e.getWeight();
		return w;
	}
	
	public double getNormalizedEdgeWeight() {
		double w = 0.0;
		for (OutEdge e : edges.values())
			w += e.getNormalizedWeight();
		return w;
	}

	public HashMap<Integer, OutEdge> getOutEdges() {
		return edges;
	}
	
	public int getOutEdgeCount() {
		return edges.size();
	}
	
	public String toMetisFormat() {
		StringBuilder sb = new StringBuilder(weight + " ");
		for (OutEdge o : edges.values()) {
			sb.append(String.format("%d %d ", 
					o.getOpposite().id + 1,
					o.getWeight()));
		}
		return sb.toString();
	}

	public String toString() {
		String str = "Vertex id : " + this.id + " Weight :" + this.weight + "\n";
		for (OutEdge e : edges.values()) {
			str = str + e.getOpposite().id + " w: " + e.getWeight() + "\n ";
		}
		return str;
	}

	@Override
	public int compareTo(Vertex other) {
		if (this.weight > other.weight)
			return 1;
		else if (this.weight < other.weight)
			return -1;
		return 0;
	}

	public boolean excatlyEquals(Vertex v) {
		if (this.id != v.id)
			return false;
		
		if (this.weight != v.weight)
			return false;
		
		if (this.partId != v.partId)
			return false;
		
		for (OutEdge edge : edges.values()) {
			OutEdge itsEdge = v.edges.get(edge.getOpposite().id);
			if (!edge.equals(itsEdge))
				return false;
		}
			
		return true;
	}

	@Override
	public boolean equals(Object that) {
		if (that instanceof Vertex) {
			Vertex p = (Vertex) that;
			return this.id == p.id;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return id;
	}

}

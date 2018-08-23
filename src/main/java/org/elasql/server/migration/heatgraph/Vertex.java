package org.elasql.server.migration.heatgraph;

import java.util.HashMap;

import org.elasql.server.migration.MigrationManager;

public class Vertex implements Comparable<Vertex> {

	private HashMap<Integer, OutEdge> edges;
	private int weight;
	private int id;
	private int partId;

	public Vertex(int id, int partId) {
		this.id = id;
		this.weight = 1;
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

	public void clear() {
		this.weight = 0;
		edges.clear();
	}

	public double getVertexWeight() {
		return (double) this.weight / MigrationManager.MONITORING_TIME;
	}

	public double getEdgeWeight() {
		double w = 0.0;
		for (OutEdge e : edges.values())
			w += ((double) e.getWeight()) / MigrationManager.MONITORING_TIME;
		return w;
	}

	public HashMap<Integer, OutEdge> getOutEdges() {
		return edges;
	}
	
	public int toMetis(StringBuilder sb) {
		sb.append(weight + "");
		for (OutEdge o : edges.values()) {
			sb.append(" " + (o.getOpposite().id + 1));
			sb.append(" " + o.getWeight());
		}
		sb.append("\n");
		return edges.size();
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

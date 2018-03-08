package org.elasql.server.migration;

import java.util.HashMap;

public class Vertex implements Comparable<Vertex> {

	public class OutEdge implements Comparable<OutEdge> {
		int weight;
		int id;
		int partId;

		public OutEdge(int id, int partId) {
			this.id = id;
			this.partId = partId;
			this.weight = 1;
		}

		@Override
		public int compareTo(OutEdge other) {
			if (this.weight > other.weight)
				return 1;
			else if (this.weight < other.weight)
				return -1;
			return 0;
		}

	}

	private HashMap<Integer, OutEdge> edge;
	private int weight;
	private int id;
	private int partId;

	public Vertex(int id, int partId) {
		this.id = id;
		this.weight = 1;
		this.partId = partId;
		edge = new HashMap<Integer, OutEdge>();
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

	public void addEdge(Integer id, int partId) {
		OutEdge w = edge.get(id);

		if (w == null)
			edge.put(id, new OutEdge(id, partId));
		else
			w.weight++;
	}
	

	public void clear() {
		this.weight = 0;
		edge.clear();
	}

	public double getVertexWeight() {
		return (double) this.weight / MigrationManager.MONITORING_TIME;
	}

	public double getEdgeWeight() {
		double w = 0.0;
		for (OutEdge e : edge.values())
			w += ((double) e.weight) / MigrationManager.MONITORING_TIME;
		return w;
	}

	public HashMap<Integer, OutEdge> getEdge() {
		return this.edge;
	}
	
	public int toMetis(StringBuilder sb) {
		sb.append(weight + "");
		for (OutEdge o : edge.values()) {
			sb.append(" " + (o.id+1));
			sb.append(" " + o.weight);
		}
		sb.append("\n");
		return edge.size();
	}

	public String toString() {
		String str = "Vertex id : " + this.id + " Weight :" + this.weight + "\n";
		for (OutEdge e : edge.values()) {
			str = str + e.id + " w: " + e.weight + "\n ";
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

package org.elasql.server.migration;

import java.util.HashMap;

public class Vertex implements Comparable<Vertex> {

	public class OutEdge implements Comparable<OutEdge> {
		int weight;
		int vertexId;
		int partId;

		public OutEdge(int id, int partId) {
			this.vertexId = id;
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

	private HashMap<Integer, OutEdge> edges;
	private int weight;
	private int id;
	private int partId;
	private boolean moved;

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

	public void addEdge(Integer id, int partId) {
		OutEdge w = edges.get(id);

		if (w == null)
			edges.put(id, new OutEdge(id, partId));
		else
			w.weight++;
	}

	public void clear() {
		this.weight = 0;
		edges.clear();
	}
	
	public void markMoved() {
		moved = true;
	}
	
	public boolean isMoved() {
		return moved;
	}

	public double getVertexWeight() {
		return (double) this.weight / MigrationManager.MONITORING_TIME;
	}

	public double getEdgeWeight() {
		double w = 0.0;
		for (OutEdge e : edges.values())
			w += ((double) e.weight) / MigrationManager.MONITORING_TIME;
		return w;
	}

	public HashMap<Integer, OutEdge> getOutEdges() {
		return edges;
	}
	
	public int toMetis(StringBuilder sb) {
		sb.append(weight + "");
		for (OutEdge o : edges.values()) {
			sb.append(" " + (o.vertexId+1));
			sb.append(" " + o.weight);
		}
		sb.append("\n");
		return edges.size();
	}

	public String toString() {
		String str = "Vertex id : " + this.id + " Weight :" + this.weight + "\n";
		for (OutEdge e : edges.values()) {
			str = str + e.vertexId + " w: " + e.weight + "\n ";
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

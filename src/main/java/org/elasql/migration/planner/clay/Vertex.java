package org.elasql.migration.planner.clay;

import java.util.HashMap;
import java.util.Map;

import org.elasql.sql.RecordKey;

class Vertex {
	
	private HashMap<RecordKey, OutEdge> edges;
	private int weight;
	private RecordKey key;
	private int partId;

	Vertex(RecordKey key, int partId) {
		this(key, partId, 1);
	}

	Vertex(RecordKey key, int partId, int weight) {
		this.key = key;
		this.weight = weight;
		this.partId = partId;
		edges = new HashMap<RecordKey, OutEdge>();
	}

	RecordKey getKey() {
		return key;
	}

	void setPartId(int part) {
		this.partId = part;
	}

	int getPartId() {
		return partId;
	}

	void incrementWeight() {
		this.weight++;
	}

	void addEdgeTo(Vertex opposite) {
		OutEdge e = edges.get(opposite.getKey());

		if (e == null)
			edges.put(opposite.getKey(), new OutEdge(opposite));
		else
			e.incrementWeight();
	}
	
	void setEdgeTo(Vertex opposite, int weight) {
		edges.put(opposite.getKey(), new OutEdge(opposite, weight));
	}

	void clear() {
		this.weight = 0;
		edges.clear();
	}

	int getVertexWeight() {
		return weight;
	}

	int getEdgeWeight() {
		int w = 0;
		for (OutEdge e : edges.values())
			w += e.getWeight();
		return w;
	}

	Map<RecordKey, OutEdge> getOutEdges() {
		return edges;
	}
	
	int getOutEdgeCount() {
		return edges.size();
	}
	
	String toMetisFormat(Map<RecordKey, Integer> keyToInt) {
		StringBuilder sb = new StringBuilder(weight + " ");
		for (OutEdge o : edges.values()) {
			sb.append(String.format("%d %d ", 
					keyToInt.get(o.getOpposite().key),
					o.getWeight()));
		}
		return sb.toString();
	}

	@Override
	public String toString() {
		String str = "Vertex key: " + this.key + ", weight :" + this.weight + "\n";
		for (OutEdge e : edges.values()) {
			str = str + e.getOpposite().key + " w: " + e.getWeight() + "\n ";
		}
		return str;
	}

	// Only consider key
	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		
		if (!obj.getClass().equals(this.getClass()))
			return false;
		
		Vertex v = (Vertex) obj;
		return this.key.equals(v.key);
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

}

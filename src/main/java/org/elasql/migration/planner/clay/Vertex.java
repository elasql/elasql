package org.elasql.migration.planner.clay;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.sql.PartitioningKey;

class Vertex {
	
	private HashMap<PartitioningKey, OutEdge> keyToEdge;
	private List<OutEdge> edges;
	private int weight;
	private PartitioningKey key;
	private int partId;

	Vertex(PartitioningKey key, int partId) {
		this(key, partId, 1);
	}

	Vertex(PartitioningKey key, int partId, int weight) {
		this.key = key;
		this.weight = weight;
		this.partId = partId;
		keyToEdge = new HashMap<PartitioningKey, OutEdge>();
		edges = new ArrayList<OutEdge>();
	}

	PartitioningKey getKey() {
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
		OutEdge e = keyToEdge.get(opposite.getKey());

		if (e == null) {
			e = new OutEdge(opposite);
			keyToEdge.put(opposite.getKey(), e);
			edges.add(e);
		} else
			e.incrementWeight();
	}
	
	void setEdgeTo(Vertex opposite, int weight) {
		OutEdge e = new OutEdge(opposite, weight);
		keyToEdge.put(opposite.getKey(), e);
		edges.add(e);
	}

	void clear() {
		this.weight = 0;
		keyToEdge.clear();
		edges.clear();
	}

	int getVertexWeight() {
		return weight;
	}

	int getEdgeWeight() {
		int w = 0;
		for (OutEdge e : edges)
			w += e.getWeight();
		return w;
	}

	List<OutEdge> getOutEdges() {
		return edges;
	}
	
	int getOutEdgeCount() {
		return edges.size();
	}
	
	String toMetisFormat(Map<PartitioningKey, Integer> keyToInt) {
		StringBuilder sb = new StringBuilder(weight + " ");
		for (OutEdge o : edges) {
			sb.append(String.format("%d %d ", 
					keyToInt.get(o.getOpposite().key),
					o.getWeight()));
		}
		return sb.toString();
	}

	@Override
	public String toString() {
		String str = "Vertex key: " + this.key + ", weight :" + this.weight + "\n";
		for (OutEdge e : edges) {
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

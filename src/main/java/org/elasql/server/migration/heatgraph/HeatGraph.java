package org.elasql.server.migration.heatgraph;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.server.migration.MigrationManager;
import org.elasql.server.migration.clay.Partition;

public class HeatGraph implements Serializable {

	private static final long serialVersionUID = 20190612001L;
	
	public static HeatGraph deserializeFromFile(File inputFileName) {
		ObjectInputStream inputStream = null;
		try {
			inputStream = new ObjectInputStream(new FileInputStream(inputFileName));
			return (HeatGraph) inputStream.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

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
			if (v.getPartId() < MigrationManager.currentNumOfPartitions())
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

	public void writeInMetisFormat(Writer writer, int verticeCount) throws IOException {
		// Count vertices and edges
		long numEdge = 0;
		for (Vertex v : vertices.values()) {
			numEdge += v.getOutEdgeCount();
		}

		// Write the first line "[vertex count] [edge count] 011"
		writer.write(String.format("%d %d 011\n", verticeCount, numEdge / 2));

		// Write each vertice
		for (int i = 0; i < verticeCount; i++) {
			Vertex v = vertices.get(i);
			if (v != null) {
				writer.write(v.toMetisFormat());
				writer.write("\n");
			} else {
				writer.write("0\n");
			}
		}
	}
	
	public void serializeToFile(File outputFileName) {
		ObjectOutputStream outStream = null;
		try {
			outStream = new ObjectOutputStream(new FileOutputStream(outputFileName));
			outStream.writeObject(this);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (outStream != null) {
				try {
					outStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	void addVertex(Vertex v) {
		vertices.put(v.getId(), v);
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		// # of vertices
		out.writeInt(vertices.size());
		
		// Each vertex
		int edgeCount = 0;
		for (Vertex v : vertices.values()) {
			out.writeInt(v.getId());
			out.writeInt(v.getPartId());
			out.writeInt(v.getVertexWeight());
			edgeCount += v.getOutEdgeCount();
		}
		
		// # of edges
		out.writeInt(edgeCount);
		
		// Each edge
		for (Vertex v : vertices.values()) {
			int fromId = v.getId();
			for (OutEdge edge : v.getOutEdges().values()) {
				int toId = edge.getOpposite().getId();
				int weight = edge.getWeight();
				
				out.writeInt(fromId);
				out.writeInt(toId);
				out.writeInt(weight);
			}
		}
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		// # of vertices
		int vertexCount = in.readInt();
		this.vertices = new HashMap<Integer, Vertex>(vertexCount);
		
		// Each vertex
		for (int i = 0; i < vertexCount; i++) {
			int id = in.readInt();
			int partId = in.readInt();
			int weight = in.readInt();
			
			Vertex v = new Vertex(id, partId, weight);
			vertices.put(id, v);
		}
		
		// # of edges
		int edgeCount = in.readInt();
		
		// Each edge
		for (int i = 0; i < edgeCount; i++) {
			int fromId = in.readInt();
			int toId = in.readInt();
			int weight = in.readInt();
			
			Vertex from = vertices.get(fromId);
			Vertex to = vertices.get(toId);
			from.addEdgeWithWeight(to, weight);
		}
	}
}

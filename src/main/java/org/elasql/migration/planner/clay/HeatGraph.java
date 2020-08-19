package org.elasql.migration.planner.clay;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.sql.PartitioningKey;

public class HeatGraph implements Serializable {

	private static final long serialVersionUID = 20190612001L;
	
	static HeatGraph deserializeFromFile(File inputFileName) {
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

	private Map<PartitioningKey, Vertex> vertices = new HashMap<PartitioningKey, Vertex>(1000000);

	void updateWeightOnVertex(PartitioningKey key, int partId) {
		Vertex vertex = vertices.get(key);
		// Note that a vertex represents a range of records.
		if (vertex == null)
			vertices.put(key, new Vertex(key, partId));
		else
			vertex.incrementWeight();
	}

	// Update weights for co-accessed vertices
	void updateWeightOnEdges(Collection<PartitioningKey> coaccessedVertices) {
		for (PartitioningKey i : coaccessedVertices)
			for (PartitioningKey j : coaccessedVertices)
				if (!i.equals(j))
					vertices.get(i).addEdgeTo(vertices.get(j));
	}

	List<Partition> splitToPartitions(int totalPartitions, double multiPartsCost) {
		List<Partition> partitions = new ArrayList<Partition>();
		for (int i = 0; i < totalPartitions; i++)
			partitions.add(new Partition(i, multiPartsCost));

		for (Vertex v : vertices.values())
			if (v.getPartId() < totalPartitions)
				partitions.get(v.getPartId()).addVertex(v);

		return partitions;
	}

	Vertex getVertex(PartitioningKey key) {
		return vertices.get(key);
	}

	Map<PartitioningKey, Vertex> getVertice() {
		return new HashMap<PartitioningKey, Vertex>(vertices);
	}
	
	void generateMetisGraphFile(File dirPath) throws IOException {
		// Ensure the existence of the directory
		if (dirPath.exists() && !dirPath.isDirectory())
			throw new IllegalArgumentException(String.format("'%s' is not a directory.", dirPath));
		else if (!dirPath.exists())
			dirPath.mkdirs();
		
		// Create a mapping from vertex keys to integers and count edges
		int edgeCount = 0;
		List<PartitioningKey> keys = new ArrayList<PartitioningKey>();
		Map<PartitioningKey, Integer> keyToInt = new HashMap<PartitioningKey, Integer>(vertices.size());
		for (Vertex v : vertices.values()) {
			edgeCount += v.getOutEdgeCount();
			keys.add(v.getKey());
			keyToInt.put(v.getKey(), keys.size()); // id starts from 1
		}
		edgeCount /= 2; // because each edge is counted twice
		
		// Write the mapping file
		File mappingFile = new File(dirPath, "mapping.bin");
		writeMetisMappingFile(mappingFile, keyToInt);
		
		// Write the metis graph file
		File metisFile = new File(dirPath, "metis.txt");
		writeMetisFile(metisFile, keys, keyToInt, vertices.size(), edgeCount);
	}
	
	private void writeMetisMappingFile(File filePath, Map<PartitioningKey, Integer> mapping) throws IOException {
		try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(
				new FileOutputStream(filePath)))) {
			out.writeObject(mapping);
		}
	}

	void writeMetisFile(File filePath, List<PartitioningKey> keys,
			Map<PartitioningKey, Integer> keyToInt, int vertexCount, int edgeCount) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
			// Write the first line "[vertex count] [edge count] 011"
			writer.write(String.format("%d %d 011\n", vertexCount, edgeCount));
	
			// Write each vertex
			for (PartitioningKey key : keys) {
				Vertex v = vertices.get(key);
				writer.write(v.toMetisFormat(keyToInt));
				writer.write("\n");
			}
		}
	}
	
	void serializeToFile(File outputFileName) {
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
		vertices.put(v.getKey(), v);
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		// # of vertices
		out.writeInt(vertices.size());
		
		// Each vertex
		int edgeCount = 0;
		for (Vertex v : vertices.values()) {
			out.writeObject(v.getKey());
			out.writeInt(v.getPartId());
			out.writeInt(v.getVertexWeight());
			edgeCount += v.getOutEdgeCount();
		}
		
		// # of edges
		out.writeInt(edgeCount);
		
		// Each edge
		for (Vertex v : vertices.values()) {
			PartitioningKey fromKey = v.getKey();
			for (OutEdge edge : v.getOutEdges()) {
				PartitioningKey toKey = edge.getOpposite().getKey();
				int weight = edge.getWeight();
				
				out.writeObject(fromKey);
				out.writeObject(toKey);
				out.writeInt(weight);
			}
		}
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		// # of vertices
		int vertexCount = in.readInt();
		this.vertices = new HashMap<PartitioningKey, Vertex>(vertexCount);
		
		// Each vertex
		for (int i = 0; i < vertexCount; i++) {
			PartitioningKey key = (PartitioningKey) in.readObject();
			int partId = in.readInt();
			int weight = in.readInt();
			
			Vertex v = new Vertex(key, partId, weight);
			vertices.put(key, v);
		}
		
		// # of edges
		int edgeCount = in.readInt();
		
		// Each edge
		for (int i = 0; i < edgeCount; i++) {
			PartitioningKey fromKey = (PartitioningKey) in.readObject();
			PartitioningKey toKey = (PartitioningKey) in.readObject();
			int weight = in.readInt();
			
			Vertex from = vertices.get(fromKey);
			Vertex to = vertices.get(toKey);
			from.setEdgeTo(to, weight);
		}
	}
}

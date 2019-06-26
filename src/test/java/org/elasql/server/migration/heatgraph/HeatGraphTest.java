package org.elasql.server.migration.heatgraph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

import junit.framework.Assert;

public class HeatGraphTest {
	
	@Test
	public void testSerialization() {
		// Create vertices
		Vertex[] vertices = new Vertex[6];
		vertices[0] = new Vertex(0, 0, 15);
		vertices[1] = new Vertex(1, 1, 23);
		vertices[2] = new Vertex(2, 0, 65);
		vertices[3] = new Vertex(3, 2, 82);
		vertices[4] = new Vertex(4, 1, 13);
		vertices[5] = new Vertex(5, 2, 2);
		
		// Create edges
		vertices[0].addEdgeTo(vertices[1]);
		vertices[0].addEdgeTo(vertices[2]);
		vertices[1].addEdgeTo(vertices[0]);
		vertices[1].addEdgeTo(vertices[3]);
		vertices[1].addEdgeTo(vertices[4]);
		vertices[2].addEdgeTo(vertices[0]);
		vertices[2].addEdgeTo(vertices[5]);
		vertices[3].addEdgeTo(vertices[1]);
		vertices[3].addEdgeTo(vertices[4]);
		vertices[4].addEdgeTo(vertices[1]);
		vertices[4].addEdgeTo(vertices[3]);
		vertices[4].addEdgeTo(vertices[5]);
		vertices[5].addEdgeTo(vertices[2]);
		vertices[5].addEdgeTo(vertices[4]);
		
		// Create a graph
		HeatGraph graph = new HeatGraph();
		for (Vertex v : vertices)
			graph.addVertex(v);
		
		try {
			// Perform serialization
			ByteArrayOutputStream byteBuf = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(byteBuf);
			out.writeObject(graph);
			byte[] data = byteBuf.toByteArray();
			
			// Perform deserialization
			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data));
			HeatGraph resultGraph = (HeatGraph) in.readObject();
			
			// Check vertices
			for (int i = 0; i < vertices.length; i++)
				Assert.assertTrue(vertices[i].equals(resultGraph.getVertex(i)));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}

package org.elasql.schedule.tpart.graph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasql.cache.tpart.TPartCacheMgr;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class GraphDumper {
	
	private static final String FILE_NAME = "auto-bencher-workspace/graph.txt";
	
	static {
		File file = new File(FILE_NAME);
		file.delete();
	}

	private static class DumpedEdge implements Comparable<DumpedEdge> {

		public final long source;
		public final long dest;
		public final PrimaryKey key;

		public DumpedEdge(long source, long dest) {
			this.source = source;
			this.dest = dest;
			this.key = null;
		}
		
		public DumpedEdge(long source, long dest, PrimaryKey key) {
			this.source = source;
			this.dest = dest;
			this.key = key;
		}

		@Override
		public int hashCode() {
			return (int) (source * 17 + dest * 17 + 37);
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof DumpedEdge))
				return false;
			DumpedEdge e = (DumpedEdge) o;
			return source == e.source && dest == e.dest;
		}

		@Override
		public int compareTo(DumpedEdge e) {
			int result = (int)(source - e.source);
			if (result != 0)
				return result;
			
			result = (int)(dest - e.dest);
			if (result != 0)
				return result;
			
			return key.hashCode() - e.key.hashCode();
		}
		
		@Override
		public String toString() {
			return String.format("{Resource: %s from tx.%d to tx.%d}", key, source, dest);
		}
	}
	
	public static void dumpToFilePlainText(TGraph graph, int id) {
		List<TxNode> nodes = graph.getTxNodes();
		ArrayList<DumpedEdge> edges = new ArrayList<DumpedEdge>();

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_NAME, true))) {
			// Print graph id
			writer.write(String.format("Graph no.%d", id));
			writer.newLine();
			
			// Print the number of nodes
			writer.write(String.format("Number of nodes: %d", nodes.size() + PartitionMetaMgr.NUM_PARTITIONS));
			writer.newLine();
			writer.newLine();

			// Print the partition of each node
			for (TxNode node : nodes) {
				long txNum = node.getTxNum();
				
				writer.write(String.format("Node.%d on part.%d", txNum, node.getPartId()));
				writer.newLine();
				
				// Read edges
				edges.clear();
				for (Edge edge : node.getReadEdges()) {
					edges.add(new DumpedEdge(edge.getTarget().getTxNum(), txNum, edge.getResourceKey()));
				}
				Collections.sort(edges);
				for (DumpedEdge d : edges) {
					writer.write(d.toString());
					writer.newLine();
				}
				
				// Write edges
				edges.clear();
				for (Edge edge : node.getWriteEdges()) {
					edges.add(new DumpedEdge(txNum, edge.getTarget().getTxNum(), edge.getResourceKey()));
				}
				Collections.sort(edges);
				for (DumpedEdge d : edges) {
					writer.write(d.toString());
					writer.newLine();
				}
				
				// Write back edges
				edges.clear();
				for (Edge edge : node.getWriteBackEdges()) {
					edges.add(new DumpedEdge(txNum, edge.getTarget().getTxNum(), edge.getResourceKey()));
				}
				Collections.sort(edges);
				for (DumpedEdge d : edges) {
					writer.write(d.toString());
					writer.newLine();
				}
			}
			
			writer.write("=================================================================");
			writer.newLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void dumpToFile(File file, TGraph graph) {
		List<TxNode> nodes = graph.getTxNodes();
		Map<DumpedEdge, Integer> edges = new HashMap<DumpedEdge, Integer>();

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			// Print the number of nodes
			writer.write(String.format("%d", nodes.size() + PartitionMetaMgr.NUM_PARTITIONS));
			writer.newLine();

			// Print the information of sink nodes
			for (int partId = 0; partId < PartitionMetaMgr.NUM_PARTITIONS; partId++) {
				writer.write(String.format("%d %d", TPartCacheMgr.toSinkId(partId), partId));
				writer.newLine();
			}

			// Print the partition of each node
			for (TxNode node : nodes) {
				long txNum = node.getTxNum();
				
				writer.write(String.format("%d %d", txNum, node.getPartId()));
				writer.newLine();
				
				// Read edges
				for (Edge edge : node.getReadEdges()) {
					edges.put(new DumpedEdge(edge.getTarget().getTxNum(), txNum),
							Integer.parseInt((String) edge.getResourceKey().getVal("ycsb_id").asJavaVal()));
				}
				
				// Write edges
				for (Edge edge : node.getWriteEdges()) {
					edges.put(new DumpedEdge(txNum, edge.getTarget().getTxNum()),
							Integer.parseInt((String) edge.getResourceKey().getVal("ycsb_id").asJavaVal()));
				}
				
				// Write back edges
				for (Edge edge : node.getWriteBackEdges()) {
					edges.put(new DumpedEdge(txNum, edge.getTarget().getTxNum()),
							Integer.parseInt((String) edge.getResourceKey().getVal("ycsb_id").asJavaVal()));
				}
			}

			// Print the edges
			writer.write(String.format("%d", edges.size()));
			writer.newLine();
			
			for (Entry<DumpedEdge, Integer> entry : edges.entrySet()) {
				writer.write(String.format("%d %d %d", entry.getKey().source, entry.getKey().dest,
						entry.getValue().intValue()));
				writer.newLine();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

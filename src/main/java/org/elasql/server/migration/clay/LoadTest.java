package org.elasql.server.migration.clay;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.server.migration.MigrationPlan;
import org.elasql.server.migration.heatgraph.HeatGraph;
import org.elasql.server.migration.heatgraph.Vertex;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;

public class LoadTest {

	public static void main(String[] args) {
		System.setProperty("org.elasql.server.migration.clay.ClayPlanner.CLUMP_MAX_SIZE", "50");
		System.setProperty("org.elasql.server.migration.clay.ClayPlanner.LOOK_AHEAD_MAX", "5");
		System.setProperty("org.elasql.server.migration.clay.ClayPlanner.OVERLOAD_PERCENTAGE", "1.2");
		System.setProperty("org.elasql.server.migration.clay.ClayPlanner.MULTI_PARTS_COST", "1");
		System.setProperty("org.elasql.storage.metadata.PartitionMetaMgr.NUM_PARTITIONS", "20");
		
		HeatGraph graph = HeatGraph.deserializeFromFile(
				new File("R:\\Experiments\\hermes\\2019-after-sigmod\\heatgraph_39.bin"));
		
		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		analyzeGraph(graph);
		
		ClayPlanner planner = new ClayPlanner(graph);
		List<MigrationPlan> plans = planner.generateMigrationPlan();
		analyzePlans(plans);
//		System.out.println(plans);
	}
	
	public static void analyzeGraph(HeatGraph graph) {
		// {(Part Id) -> {(table name) -> {{([Warehouse Range Id]) -> (count)}}}}
		Map<Integer, Map<String, int[]>> parts = new HashMap<Integer, Map<String, int[]>>();
		int partCount = 20;
		int warehousePerPart = 20;
		String[] tableNames = {"warehouse", "district", "customer",
				"history", "orders", "new_order", "item", "stock", "order_line"};
		
		// Initialize
		for (int partId = 0; partId < partCount; partId++) {
			Map<String, int[]> tables = new HashMap<String, int[]>();
			parts.put(partId, tables);
			for (int tblId = 0; tblId < tableNames.length; tblId++) {
				tables.put(tableNames[tblId], new int[partCount]);
			}
		}
		
		// Counting
		for (Map.Entry<RecordKey, Vertex> e : graph.getVertice().entrySet()) {
			RecordKey key = e.getKey();
			Vertex v = e.getValue();
			int wid = getWarehouseId(key);
			int rangeId = (wid - 1) / warehousePerPart;
			int partId = v.getPartId();
			int[] counts = parts.get(partId).get(key.getTableName());
			counts[rangeId]++;
		}
		
		// Show the result
		for (int partId = 0; partId < partCount; partId++) {
			Map<String, int[]> tables = parts.get(partId);
			System.out.println("Partition #" + partId);
			for (int tblId = 0; tblId < tableNames.length; tblId++) {
				int[] counts = tables.get(tableNames[tblId]); 
				System.out.println("Table '" + tableNames[tblId] + "': " + Arrays.toString(counts));
			}
		}
		
	}
	
	public static void analyzePlans(List<MigrationPlan> plans) {
		// {(Part Id) -> {(table name) -> {{([Warehouse Range Id]) -> (count)}}}}
		Map<Integer, Map<String, int[]>> parts = new HashMap<Integer, Map<String, int[]>>();
		int partCount = 20;
		int warehousePerPart = 20;
		String[] tableNames = {"warehouse", "district", "customer",
				"history", "orders", "new_order", "item", "stock", "order_line"};
		
		// Initialize
		for (int partId = 0; partId < partCount; partId++) {
			Map<String, int[]> tables = new HashMap<String, int[]>();
			parts.put(partId, tables);
			for (int tblId = 0; tblId < tableNames.length; tblId++) {
				tables.put(tableNames[tblId], new int[partCount]);
			}
		}
		
		// Counting
		for (MigrationPlan plan : plans) {
			for (RecordKey key : plan.getKeys()) {
				int wid = getWarehouseId(key);
				int rangeId = (wid - 1) / warehousePerPart;
				int partId = plan.getDestPart();
				int[] counts = parts.get(partId).get(key.getTableName());
				counts[rangeId]++;
			}
		}
		
		// Show the result
		for (int partId = 0; partId < partCount; partId++) {
			Map<String, int[]> tables = parts.get(partId);
			System.out.println("Partition #" + partId);
			for (int tblId = 0; tblId < tableNames.length; tblId++) {
				int[] counts = tables.get(tableNames[tblId]); 
				System.out.println("Table '" + tableNames[tblId] + "': " + Arrays.toString(counts));
			}
		}
		
	}
	
	public static int getWarehouseId(RecordKey key) {
		// For other tables, partitioned by wid
		Constant widCon;
		switch (key.getTableName()) {
		case "warehouse":
			widCon = key.getKeyVal("w_id");
			break;
		case "district":
			widCon = key.getKeyVal("d_w_id");
			break;
		case "stock":
			widCon = key.getKeyVal("s_w_id");
			break;
		case "customer":
			widCon = key.getKeyVal("c_w_id");
			break;
		case "history":
			widCon = key.getKeyVal("h_c_w_id");
			break;
		case "orders":
			widCon = key.getKeyVal("o_w_id");
			break;
		case "new_order":
			widCon = key.getKeyVal("no_w_id");
			break;
		case "order_line":
			widCon = key.getKeyVal("ol_w_id");
			break;
		default:
			throw new IllegalArgumentException("cannot find proper partition rule for key:" + key);
		}
		
		return (Integer) widCon.asJavaVal();
	}
}

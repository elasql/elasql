package org.elasql.server.migration.clay;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.elasql.server.migration.MigrationPlan;
import org.elasql.server.migration.heatgraph.HeatGraph;

public class LoadTest {

	public static void main(String[] args) {
		System.setProperty("org.elasql.server.migration.clay.ClayPlanner.CLUMP_MAX_SIZE", "5");
		System.setProperty("org.elasql.server.migration.clay.ClayPlanner.LOOK_AHEAD_MAX", "5");
		System.setProperty("org.elasql.server.migration.clay.ClayPlanner.OVERLOAD_PERCENTAGE", "1.05");
		System.setProperty("org.elasql.server.migration.clay.ClayPlanner.MULTI_PARTS_COST", "0.02");
		System.setProperty("org.elasql.storage.metadata.PartitionMetaMgr.NUM_PARTITIONS", "4");
		
		HeatGraph graph = HeatGraph.deserializeFromFile(
				new File("R:\\Experiments\\hermes\\2019-after-sigmod\\heatgraph_136.bin"));
		
		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		ClayPlanner planner = new ClayPlanner(graph);
		List<MigrationPlan> plans = planner.generateMigrationPlan();
		System.out.println(plans);
	}
}

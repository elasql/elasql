package org.elasql.migration.planner.clay;

import java.io.File;

import org.elasql.migration.MigrationPlan;

public class Test {

	public static void main(String[] args) {
		System.setProperty("java.util.logging.config.file", "R:\\Experiments\\hermes\\2020\\logging.properties");
		System.setProperty("org.elasql.storage.metadata.PartitionMetaMgr.NUM_PARTITIONS", "20");
		System.setProperty("org.elasql.migration.planner.clay.ClayPlanner.CLUMP_MAX_SIZE", "1000000");
		System.setProperty("org.elasql.migration.planner.clay.ClayPlanner.OVERLOAD_PERCENTAGE", "1.5");
		System.setProperty("org.elasql.migration.planner.clay.ClayPlanner.MULTI_PARTS_COST", "0.5");
		
		HeatGraph graph = HeatGraph.deserializeFromFile(
				new File("C:\\Users\\SLMT\\Desktop\\clay-heat.bin"));
		System.out.println("Graph is loaded");
		ClayPlanner planner = new ClayPlanner(graph);
		MigrationPlan plan = planner.generateMigrationPlan();
		for (MigrationPlan subPlan : plan.splits()) {
			System.out.println(subPlan);
		}
	}

}

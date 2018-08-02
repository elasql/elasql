package org.elasql.server.migration;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class ClayController {
	private static Logger logger = Logger.getLogger(ClayController.class.getName());
	
	private static final int MAX_CLUMPS = 3; // # of plans to generate
//	private static final int LOOK_AHEAD = 5; // Recommended by the paper
	private static final int LOOK_AHEAD = 20; // for Google-workloads
//	private static final int LOOK_AHEAD = 2000; // for multi-tanent
//	private static final int LOOK_AHEAD = 3000; // for consolidation
//	private static final int DATA_RANGE_SIZE = 10;
	
	private MigrationManager migraMgr;
	private HeatGraph heatGraph = new HeatGraph();
	private int numOfClumpsGenerated = 0;
	
	ClayController(MigrationManager migraMgr) {
		this.migraMgr = migraMgr;
	}
	
	public void addToHeatGraph(Collection<RecordKey> keys) {
		LinkedList<Integer> vertexIdSet = new LinkedList<Integer>();
		Integer vetxId;
		int partId;
		// Since only the sequence node can get in, remoteReadKeys basically
		// contains all the read keys of the transaction.
		for (RecordKey k : keys) {
			vetxId = mapToVertexId(migraMgr.getPartitioningKey(k));
			partId = Elasql.partitionMetaMgr().getPartition(k);

			heatGraph.updateWeightOnVertex(vetxId, partId);
			vertexIdSet.add(vetxId);
		}
		heatGraph.updateWeightOnEdges(vertexIdSet);
	}
	
	public List<MigrationPlan> generateMigrationPlan() {
		long startTime = System.currentTimeMillis();
		
		List<Partition> partitions = heatGraph.splitToPartitions();
		// Debug
		for (Partition p : partitions) {
			System.out
					.println("Part : " + p.getId() + " Weight : " + p.getLoad() + " Edge : " + p.getEdgeLoad());

			for (Vertex v : p.getFirstTen())
				System.out.println("Top Ten : " + v.getId() + " PartId : " + v.getPartId() + " Weight :"
						+ v.getVertexWeight());
		}
		
		// Normal Clay: Get Most Overloaded Partition
		double avgLoad = 0;
		for (Partition p : partitions)
			avgLoad += p.getTotalLoad();
		avgLoad /= PartitionMetaMgr.NUM_PARTITIONS;

		LinkedList<Partition> overloadParts = new LinkedList<Partition>();
		for (Partition p : partitions)
			if (p.getTotalLoad() > avgLoad)
				overloadParts.add(p);

		Collections.sort(overloadParts);
		Partition overloadPart = overloadParts.getLast();
		
		// For consolidation: always choose the 4th partition as the overloaded one
//		Partition overloadPart = partitions.remove(3);
		
		// Init Clump with most Hotest Vertex
		Clump clump = new Clump(overloadPart.getHotestVertex());
		
		// XXX: In the original design of Clay,
		// it first check if the clump is "feasible".
		// If the clump is, it will mark it as a candidate clump.
		// Then, it expands the clump LOOK_AHEAD times in order
		// to improve the candidate plan.
		// However, we skip the checking of "feasible" for simplification.
		// Expend LOOK_AHEAD times
		while (clump.size() < LOOK_AHEAD) {
			if (clump.hasNeighbor()) {
				int nId = clump.getHotestNeighbor();
				Vertex n = heatGraph.getVertex(nId);
				if (n.isMoved())
					clump.removeNeighbor(nId);
				else
					clump.expand(n);
			} else {
				if (logger.isLoggable(Level.INFO))
					logger.info("There is no more neighbor for the clump: " + clump);
				break;
			}
		}
		
		// XXX: Deterministic select last load partition as Dest
		Collections.sort(partitions);
		Integer destPart = partitions.get(0).getId();
		
		// Generate migration plans
		List<MigrationPlan> plans = clump.toMigrationPlans(destPart);
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info("Clay takes " + (System.currentTimeMillis() - startTime) +
					" ms to generate clump no." + numOfClumpsGenerated);
			logger.info("Generated migration plans: " + plans);
		}
		
		numOfClumpsGenerated++;
		
		return plans;
	}
	
	public boolean hasMorePlans() {
		return numOfClumpsGenerated < MAX_CLUMPS;
	}
	
	public void reset() {
		heatGraph = new HeatGraph();
		numOfClumpsGenerated = 0;
	}
	
	// E.g. 1~10 => 0, 11~20 => 1
	private int mapToVertexId(int key) {
		return (key - 1) / MigrationManager.DATA_RANGE_SIZE;
	}
}

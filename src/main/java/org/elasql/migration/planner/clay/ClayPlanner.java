package org.elasql.migration.planner.clay;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.MigrationPlan;
import org.elasql.migration.planner.MigrationPlanner;
import org.elasql.server.Elasql;
import org.elasql.sql.PartitioningKey;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

/**
 * An implementation of Clay [1]. <br />
 * <br />
 * [1] Serafini, Marco, et al.
 * "Clay: fine-grained adaptive partitioning for general database schemas."
 * Proceedings of the VLDB Endowment 10.4 (2016): 445-456.
 * 
 * @author yslin
 */
public class ClayPlanner implements MigrationPlanner {
	private static Logger logger = Logger.getLogger(ClayPlanner.class.getName());
	
	private static final double MULTI_PARTS_COST;
	private static final double OVERLOAD_PERCENTAGE;
	private static final int LOOK_AHEAD_MAX;
	private static final int CLUMP_MAX_SIZE;
	private static final int MAX_CLUMPS;
	private static final double SAMPLE_RATE;
	
	static {
		MULTI_PARTS_COST = ElasqlProperties.getLoader()
				.getPropertyAsDouble(ClayPlanner.class.getName() + ".MULTI_PARTS_COST", 1);
		OVERLOAD_PERCENTAGE = ElasqlProperties.getLoader()
				.getPropertyAsDouble(ClayPlanner.class.getName() + ".OVERLOAD_PERCENTAGE", 1.3);
		LOOK_AHEAD_MAX = ElasqlProperties.getLoader()
				.getPropertyAsInteger(ClayPlanner.class.getName() + ".LOOK_AHEAD_MAX", 5);
		CLUMP_MAX_SIZE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(ClayPlanner.class.getName() + ".CLUMP_MAX_SIZE", 20);
		MAX_CLUMPS = ElasqlProperties.getLoader()
				.getPropertyAsInteger(ClayPlanner.class.getName() + ".MAX_CLUMPS", 5000);
		SAMPLE_RATE = ElasqlProperties.getLoader()
				.getPropertyAsDouble(ClayPlanner.class.getName() + ".SAMPLE_RATE", 0.01);
	}
	
	private HeatGraph heatGraph;
	private int sampleGate = (int) (1 / SAMPLE_RATE);
	private int seenCount = 0, sampledCount = 0;
	private PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
	
	public ClayPlanner() {
		heatGraph = new HeatGraph();
	}
	
	public ClayPlanner(HeatGraph graph) {
		heatGraph = graph;
	}

	@Override
	public void monitorTransaction(Set<PrimaryKey> reads, Set<PrimaryKey> writes) {
		// Sample-based (delete this if we don't need it)
//		seenCount++;
//		if (seenCount % sampleGate == 0) {
//			Set<RecordKey> accessedKeys = new HashSet<RecordKey>();
//			accessedKeys.addAll(reads);
//			accessedKeys.addAll(writes);
//			
//			for (RecordKey k : accessedKeys) {
//				if (Elasql.partitionMetaMgr().isFullyReplicated(k))
//					throw new RuntimeException("Given read/write-set contain fully replicated keys");
//				
//				int partId = Elasql.partitionMetaMgr().getPartition(k);
//				heatGraph.updateWeightOnVertex(k, partId);
//			}
//			heatGraph.updateWeightOnEdges(accessedKeys);
//
//			// Only need to check the given number of transactions
//			sampledCount++;
//			if (sampledCount >= 10000) {
//				return true;
//			}
//		}
//		return false;
		
		// Consider all transactions
		List<PartitioningKey> accessedPartKeys = new ArrayList<PartitioningKey>();
		for (PrimaryKey k : reads) {
			PartitioningKey partKey = partMgr.getPartitioningKey(k);
			int partId = partMgr.getPartition(k);
			heatGraph.updateWeightOnVertex(partKey, partId);
			accessedPartKeys.add(partKey);
		}
		for (PrimaryKey k : writes) {
			PartitioningKey partKey = partMgr.getPartitioningKey(k);
			int partId = partMgr.getPartition(k);
			heatGraph.updateWeightOnVertex(partKey, partId);
			accessedPartKeys.add(partKey);
		}
		heatGraph.updateWeightOnEdges(accessedPartKeys);
	}

	@Override
	public MigrationPlan generateMigrationPlan() {
		long startTime = System.currentTimeMillis();
		ScatterMigrationPlan plan = new ScatterMigrationPlan();
		int totalPartitions = PartitionMetaMgr.NUM_PARTITIONS;
		int numOfClumpsGenerated = 0;
		
		// Debug
//		heatGraph.serializeToFile(new File("/home/db-team/clay-heat.bin"));
		
		if (logger.isLoggable(Level.FINE)) {
			logger.fine(String.format("%d transactions are seen and %d transactions are sampled", seenCount, sampledCount));
		}
		
		while (true) {
			List<Partition> partitions = heatGraph.splitToPartitions(totalPartitions, MULTI_PARTS_COST);
			double overloadThreasdhold = calculateOverloadThreasdhold(partitions);
			
			// Debug
			if (logger.isLoggable(Level.FINE)) {
				logger.fine(printPartitionLoading(partitions));
				logger.fine("Threasdhold: " + overloadThreasdhold);
			}
			
			// Find an overloaded partition
			Partition overloadedPart = null;
			for (Partition p : partitions) {
				if (p.getTotalLoad() > overloadThreasdhold) {
					overloadedPart = p;
					break;
				}
			}
			
			if (overloadedPart == null)
				break;
			
			// Generate a clump
			Clump clump = generateClump(overloadedPart, partitions, overloadThreasdhold);
			
			if (clump == null)
				break;
			
			// Some cases will come out a clump that does not have to be migrated.
			// Since it is hard to come out other plan when this case happens,
			// we will stop right here.
			if (!clump.needMigration())
				break;

			// Debug
			if (logger.isLoggable(Level.FINE)) {
				logger.fine("Clump #" + numOfClumpsGenerated + ": " + printClump(clump));
			}
			numOfClumpsGenerated++;
			
			// Generate migration plans from the clump
			ScatterMigrationPlan clumpPlan = clump.toMigrationPlan();
			plan.merge(clumpPlan);
			
			updateMigratedVertices(clump);
			
			if (numOfClumpsGenerated == MAX_CLUMPS) {
				if (logger.isLoggable(Level.WARNING))
					logger.warning("Reach the limit of the number of clumps");
				break;
			}
		}
		
		if (plan.isEmpty())
			return null;
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("Clay takes %d ms to generate %d clumps",
					(System.currentTimeMillis() - startTime), numOfClumpsGenerated));
			logger.info("Generated a migration plans with " + plan.countKeys() + " keys");
		}
		
		return plan;
	}

	@Override
	public void reset() {
		heatGraph = new HeatGraph();
		seenCount = 0;
		sampledCount = 0;
	}
	
	private double calculateOverloadThreasdhold(List<Partition> partitions) {
		double avgLoad = 0.0;
		for (Partition p : partitions)
			avgLoad += p.getTotalLoad();
		avgLoad = avgLoad / partitions.size();
		return avgLoad * OVERLOAD_PERCENTAGE;
	}
	
	private String printPartitionLoading(List<Partition> partitions) {
		StringBuilder sb = new StringBuilder();
		for (Partition p : partitions) {
			sb.append(String.format("Partition %d: local %f, cross %f, total %f\n", 
					p.getPartId(), p.getLocalLoad(), p.getCrossPartLoad(), p.getTotalLoad()));
		}
		return sb.toString();
	}
	
	/**
	 * Algorithm 1 on Clay's paper.
	 */
	private Clump generateClump(Partition overloadedPart,
			List<Partition> partitions, double overloadThreasdhold) {
		Clump currentClump = null, candidateClump = null;
		Partition destPart = null;
		int lookAhead = LOOK_AHEAD_MAX;
		Vertex addedVertex = null;
		
		while (true) {
			if (currentClump == null) {
				addedVertex = overloadedPart.getHotestVertex();
				currentClump = new Clump(addedVertex);
				destPart = findInitialDest(addedVertex, partitions);
				currentClump.setDestination(destPart.getPartId());
				
				if (logger.isLoggable(Level.FINER)) {
					logger.finer("Init clump: " + printClump(currentClump));
				}
			} else {
				if (!currentClump.hasNeighbor())
					return candidateClump;
				
				// Expand the clump
				PartitioningKey hotKey = currentClump.getHotestNeighbor();
				addedVertex = heatGraph.getVertex(hotKey);
				currentClump.expand(addedVertex);
				
				destPart = updateDestination(currentClump, destPart, partitions, overloadThreasdhold);
				currentClump.setDestination(destPart.getPartId());
				
				if (logger.isLoggable(Level.FINER)) {
					logger.finer("Expanded clump: " + printClump(currentClump));
					logger.finer(String.format("Delta for recv part %d: %f", destPart.getPartId(),
							currentClump.calcRecvLoadDelta(destPart.getPartId(), MULTI_PARTS_COST)));
					logger.finer(String.format("Is feasible ? %s", isFeasible(currentClump, destPart, overloadThreasdhold)));
					logger.finer(String.format("Delta for sender part %d: %f", addedVertex.getPartId(),
							currentClump.calcRecvLoadDelta(addedVertex.getPartId(), MULTI_PARTS_COST)));
				}
			}
			
			// Examine the clump
			if (isFeasible(currentClump, destPart, overloadThreasdhold)) {
				candidateClump = new Clump(currentClump);
			} else if (candidateClump != null) {
				lookAhead--;
			}
			
			// Limit the size of the clump
			if (currentClump.size() > CLUMP_MAX_SIZE) {
				if (candidateClump != null)
					return candidateClump;
				else
					return currentClump;
			}
			
			if (lookAhead == 0)
				return candidateClump;
		}
	}
	
	private Partition findInitialDest(Vertex v, List<Partition> partitions) {
		int destId = -1;
		
		// Find the most co-accessed partition (except for the original one)
		int[] coaccessed = new int[partitions.size()];
		for (OutEdge e : v.getOutEdges())
			coaccessed[e.getOpposite().getPartId()]++;
		for (int part = 0; part < coaccessed.length; part++) {
			// Skip the original one to avoid the clump tends to not move
			if (part != v.getPartId() && coaccessed[part] > 0) {
				if (destId == -1 || coaccessed[part] > coaccessed[destId]) {
					destId = part;
				}
			}
		}
		
		// There is no co-accessed partition
		if (destId == -1) {
			// Find the least load partition
			double minLoad = Integer.MAX_VALUE;
			for (Partition p : partitions)
				if (p.getPartId() != v.getPartId()) {
					if (destId == -1 || p.getTotalLoad() < minLoad) {
						destId = p.getPartId();
						minLoad = p.getTotalLoad();
					}
				}
		}
		
		return partitions.get(destId);
	}
	
	/**
	 * The formula in Section 7.1 of Clay's paper.
	 */
	private boolean isFeasible(Clump clump, Partition dest, double overloadThreasdhold) {
		double delta = clump.calcRecvLoadDelta(dest.getPartId(), MULTI_PARTS_COST);
		return delta <= 0.0 || (dest.getTotalLoad() + delta < overloadThreasdhold);
	}
	
	/**
	 * Algorithm 2 on Clay's paper.
	 */
	private Partition updateDestination(Clump clump, Partition currentDest,
			List<Partition> partitions, double overloadThreasdhold) {
		if (!isFeasible(clump, currentDest, overloadThreasdhold)) {
			int mostCoaccessedPart = clump.getMostCoaccessedPartition(partitions.size());
			if (mostCoaccessedPart != currentDest.getPartId() &&
					isFeasible(clump, partitions.get(mostCoaccessedPart), overloadThreasdhold))
				return partitions.get(mostCoaccessedPart);
			
			Partition leastLoadPart = getLeastLoadPartition(partitions);
			if (leastLoadPart.getPartId() != currentDest.getPartId() &&
				clump.calcRecvLoadDelta(mostCoaccessedPart, MULTI_PARTS_COST) <
				clump.calcRecvLoadDelta(leastLoadPart.getPartId(), MULTI_PARTS_COST) &&
				isFeasible(clump, leastLoadPart, overloadThreasdhold))
				return leastLoadPart;
		}
		
		return currentDest;
	}
	
	private Partition getLeastLoadPartition(List<Partition> partitions) {
		Partition minLoadPart = partitions.get(0);
		
		for (int partId = 1; partId < partitions.size(); partId++) {
			if (partitions.get(partId).getTotalLoad() < minLoadPart.getTotalLoad()) {
				minLoadPart = partitions.get(partId);
			}
		}
		
		return minLoadPart;
	}
	
	private String printClump(Clump clump) {
		StringBuilder sb = new StringBuilder("[");
		int count = 0;
		for (Vertex v : clump.getVertices()) {
			sb.append(String.format("%s (%d, %d, %d), ", v.getKey(), v.getPartId(),
					v.getVertexWeight(), v.getEdgeWeight()));
			count++;
			if (count >= 3)
				break;
		}
		sb.append(String.format("size: %d] ", clump.getVertices().size()));
		sb.append(String.format("to part.%d.", clump.getDestination()));
		
		return sb.toString();
	}
	
	private void updateMigratedVertices(Clump migratedClump) {
		int destPartId = migratedClump.getDestination();
		for (Vertex v : migratedClump.getVertices())
			v.setPartId(destPartId);
	}
}

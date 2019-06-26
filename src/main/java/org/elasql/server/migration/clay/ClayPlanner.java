package org.elasql.server.migration.clay;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.server.migration.MigrationManager;
import org.elasql.server.migration.MigrationPlan;
import org.elasql.server.migration.heatgraph.HeatGraph;
import org.elasql.server.migration.heatgraph.OutEdge;
import org.elasql.server.migration.heatgraph.Vertex;
import org.elasql.util.ElasqlProperties;

public class ClayPlanner {
	private static Logger logger = Logger.getLogger(ClayPlanner.class.getName());
	
//	public static final int MULTI_PARTS_COST = 1; // term 'k' in Clay's paper
//	public static final double OVERLOAD_THREASDHOLD = 15; // term 'theta' in Clay's paper
//	public static final double OVERLOAD_PERCENTAGE = 1.25; // For multi-tanents
//	public static final double OVERLOAD_PERCENTAGE = 1.3; // For Google workloads
//	private static final int LOOK_AHEAD_MAX = 5;
//	private static final int CLUMP_MAX_SIZE = 20; // For Google workloads
//	private static final int CLUMP_MAX_SIZE = 10000; // For multi-tanents
	
	public static final int MULTI_PARTS_COST;
	public static final double OVERLOAD_PERCENTAGE;
	private static final int LOOK_AHEAD_MAX;
	private static final int CLUMP_MAX_SIZE;
	
	static {
		MULTI_PARTS_COST = ElasqlProperties.getLoader()
				.getPropertyAsInteger(ClayPlanner.class.getName() + ".MULTI_PARTS_COST", 1);
		OVERLOAD_PERCENTAGE = ElasqlProperties.getLoader()
				.getPropertyAsDouble(ClayPlanner.class.getName() + ".OVERLOAD_PERCENTAGE", 1.3);
		LOOK_AHEAD_MAX = ElasqlProperties.getLoader()
				.getPropertyAsInteger(ClayPlanner.class.getName() + ".LOOK_AHEAD_MAX", 5);
		CLUMP_MAX_SIZE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(ClayPlanner.class.getName() + ".CLUMP_MAX_SIZE", 20);
	}
	
	private HeatGraph heatGraph;
	private int numOfClumpsGenerated = 0;
	private double overloadThreasdhold;
	
	public ClayPlanner(HeatGraph heatGraph) {
		this.heatGraph = heatGraph;
	}
	
	public List<MigrationPlan> generateMigrationPlan() {
		long startTime = System.currentTimeMillis();
		List<MigrationPlan> plans = new ArrayList<MigrationPlan>();
		
		while (true) {
			List<Partition> partitions = heatGraph.splitToPartitions();
			adjustOverloadThreasdhold(partitions);
			
			// Debug
			System.out.println(printPartitionLoading(partitions));
			System.out.println("Threasdhold: " + overloadThreasdhold);
			
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
			Clump clump = generateClump(overloadedPart, partitions);
			
			if (clump == null)
				break;
			
			// Some cases will come out a clump that does not have to be migrated.
			// Since it is hard to come out other plan when this case happends,
			// we will stop right here.
			if (!clump.needMigration())
				break;

			System.out.println("Clump #" + numOfClumpsGenerated + ": " + printClump(clump));
			numOfClumpsGenerated++;
			
			// Generate migration plans from the clump
			plans.addAll(clump.toMigrationPlans());
			
			updateMigratedVertices(clump);
		}
		
		if (plans.isEmpty())
			return null;
		
		plans = mergePlans(plans);
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("Clay takes %d ms to generate %d clumps",
					(System.currentTimeMillis() - startTime), numOfClumpsGenerated));
//			logger.info("Generated migration plans: " + plans);
		}
		
		return plans;
	}
	
	// XXX: Only for multi-tanents
	public List<MigrationPlan> generateConsolidationPlan() {
		if (numOfClumpsGenerated > 0)
			return null;
		
		List<MigrationPlan> plans = new LinkedList<MigrationPlan>();
		List<Partition> partitions = heatGraph.splitToPartitions();
		adjustOverloadThreasdhold(partitions);
		
		Partition leastLoadPart = getLeastLoadPartition(partitions);
		MigrationPlan plan = new MigrationPlan(3, leastLoadPart.getPartId());
		int start = 300000 / MigrationManager.DATA_RANGE_SIZE;
		int end = 400000 / MigrationManager.DATA_RANGE_SIZE;
		for (int i = start; i < end; i++)
			plan.addKey(i);
		plans.add(plan);
		
		numOfClumpsGenerated++;
		
		return plans;
	}
	
	private void adjustOverloadThreasdhold(List<Partition> partitions) {
		double avgLoad = 0.0;
		for (Partition p : partitions)
			avgLoad += p.getTotalLoad();
		avgLoad = avgLoad / MigrationManager.currentNumOfPartitions();
		overloadThreasdhold = avgLoad * OVERLOAD_PERCENTAGE;
	}
	
	/**
	 * Algorithm 1 on Clay's paper.
	 */
	private Clump generateClump(Partition overloadedPart, List<Partition> partitions) {
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
				
//				System.out.println("Init clump: " + printClump(candidateClump));
			} else {
				if (!currentClump.hasNeighbor())
					return candidateClump;
				
				// Expand the clump
				int nId = currentClump.getHotestNeighbor();
				addedVertex = heatGraph.getVertex(nId);
				currentClump.expand(addedVertex);
				
				destPart = updateDestination(currentClump, destPart, partitions);
				currentClump.setDestination(destPart.getPartId());
				
//				System.out.println("Expanded clump: " + printClump(currentClump));
//				System.out.println("Expanded clump size: " + currentClump.size());
//				System.out.println(String.format("Delta for recv part %d: %f", destPart.getPartId(),
//						calcRecvLoadDelta(currentClump, destPart.getPartId())));
//				System.out.println(String.format("Is feasible ? %s", isFeasible(currentClump, destPart)));
//				System.out.println(String.format("Delta for sender part %d: %f", addedVertex.getPartId(),
//						calcSendLoadDelta(currentClump, addedVertex.getPartId())));
			}
			
			// Examine the clump
			if (isFeasible(currentClump, destPart)) {
				candidateClump = new Clump(currentClump);
			} else if (candidateClump != null) {
				lookAhead--;
			}
			
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
		
		// Find the most co-accessed partitions
		int[] coaccessed = new int[MigrationManager.currentNumOfPartitions()];
		for (OutEdge e : v.getOutEdges().values())
			coaccessed[e.getOpposite().getPartId()]++;
		for (int part = 0; part < coaccessed.length; part++) {
			if (coaccessed[part] > 0) {
				if (destId == -1 || coaccessed[part] > coaccessed[destId]) {
					destId = part;
				}
			}
		}
		
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
	 * Algorithm 2 on Clay's paper.
	 */
	private Partition updateDestination(Clump clump, Partition oldDest, List<Partition> partitions) {
		if (!isFeasible(clump, oldDest)) {
//			int mostCoaccessedPart = clump.getMostCoaccessedPartition();
//			if (mostCoaccessedPart != -1 && mostCoaccessedPart != oldDest.getPartId() &&
//					isFeasible(clump, partitions.get(mostCoaccessedPart)))
//				return partitions.get(mostCoaccessedPart);
			
			Partition leastLoadPart = getLeastLoadPartition(partitions);
			if (leastLoadPart.getPartId() != oldDest.getPartId() &&
				isFeasible(clump, leastLoadPart))
				return leastLoadPart;
		}
		
		return oldDest;
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
	
	/**
	 * The formula in Section 7.1 of Clay's paper.
	 */
	private boolean isFeasible(Clump clump, Partition dest) {
		double delta = calcRecvLoadDelta(clump, dest.getPartId());
		return delta < 0 || (dest.getTotalLoad() + delta < overloadThreasdhold);
	}
	
	/**
	 * The first formula in Section 7.2 of Clay's paper.
	 */
	private double calcSendLoadDelta(Clump clump, int senderPartId) {
		Collection<Vertex> vertices = clump.getVertices();
		
		// Removed node loading
		double removedNodeLoad = 0;
		for (Vertex v : vertices)
			if (v.getPartId() == senderPartId)
				removedNodeLoad += v.getNormalizedVertexWeight();
		
		// Cross-partition edge loading
		double addedCrossLoad = 0, reducedCrossLoad = 0;
		for (Vertex v : vertices) {
			if (v.getPartId() == senderPartId) {
				for (OutEdge e : v.getOutEdges().values()) {
					Vertex u = e.getOpposite();
					if (u.getPartId() == senderPartId) {
						if (!vertices.contains(u))
							addedCrossLoad += e.getNormalizedWeight();
					} else {
						reducedCrossLoad += e.getNormalizedWeight();
					}
				}
			}
		}
		
		return -(removedNodeLoad) + MULTI_PARTS_COST *
				(addedCrossLoad - reducedCrossLoad);
	}
	
	/**
	 * The second formula in Section 7.2 of Clay's paper.
	 */
	private double calcRecvLoadDelta(Clump clump, int destPartId) {
		Collection<Vertex> vertices = clump.getVertices();
		
		// Added node loading
		double addedNodeLoad = 0;
		for (Vertex v : vertices)
			if (v.getPartId() != destPartId)
				addedNodeLoad += v.getNormalizedVertexWeight();
		
		// Cross-partition edge loading
		double addedCrossLoad = 0, reducedCrossLoad = 0;
		for (Vertex v : vertices) {
			if (v.getPartId() != destPartId) {
				for (OutEdge e : v.getOutEdges().values()) {
					Vertex u = e.getOpposite();
					if (u.getPartId() == destPartId) {
						reducedCrossLoad += e.getNormalizedWeight();
					} else {
						if (!vertices.contains(u))
							addedCrossLoad += e.getNormalizedWeight();
					}
				}
			}
		}
		
		return addedNodeLoad + MULTI_PARTS_COST *
				(addedCrossLoad - reducedCrossLoad);
	}
	
	private void updateMigratedVertices(Clump migratedClump) {
		int destPartId = migratedClump.getDestination();
		for (Vertex v : migratedClump.getVertices())
			v.setPartId(destPartId);
	}
	
	/**
	 * Merge the plans into a new set of plans such that:<br>
	 * <ul>
	 * <li>There is no tow plans having the same source and destination.</li>
	 * <li>There is no tuple showing up in two separate plans.</li>
	 * </ul>
	 * 
	 * @param planList
	 * @return
	 */
	private List<MigrationPlan> mergePlans(List<MigrationPlan> planList) {
		// (tuple id -> part id)
		Map<Integer, Integer> sources = new HashMap<Integer, Integer>();
		Map<Integer, Integer> dests = new HashMap<Integer, Integer>();
		
		// Record the (first) source node of each tuple
		for (MigrationPlan p : planList) {
			for (Integer tupleId: p.getKeys()) {
				sources.putIfAbsent(tupleId, p.getSourcePart());
			}
		}
		
		// Record the (last) destination node of each tuple
		for (MigrationPlan p : planList) {
			for (Integer tupleId: p.getKeys()) {
				dests.put(tupleId, p.getDestPart());
			}
		}
		
		// Output the results as plans
		// (source part id -> (dest part id -> plan))
		Map<Integer, Map<Integer, MigrationPlan>> mergedPlans =
				new HashMap<Integer, Map<Integer, MigrationPlan>>();
		for (Integer tupleId : sources.keySet()) {
			int sourcePart = sources.get(tupleId);
			int destPart = dests.get(tupleId);
			
			if (sourcePart == destPart)
				continue;
			
			Map<Integer, MigrationPlan> plans = mergedPlans.get(sourcePart);
			if (plans == null) {
				plans = new HashMap<Integer, MigrationPlan>();
				mergedPlans.put(sourcePart, plans);
			}
			
			MigrationPlan plan = plans.get(destPart);
			if (plan == null) {
				plan = new MigrationPlan(sourcePart, destPart);
				plans.put(destPart, plan);
			}
			
			plan.addKey(tupleId);
		}
		
		// Convert the map to a list
		planList = new ArrayList<MigrationPlan>();
		for (Map<Integer, MigrationPlan> map : mergedPlans.values()) {
			for (MigrationPlan plan : map.values()) {
				planList.add(plan);
			}
		}
		
		return planList;
	}
	
	private String printClump(Clump clump) {
		StringBuilder sb = new StringBuilder("[");
		int count = 0;
		for (Vertex v : clump.getVertices()) {
			sb.append(String.format("%d (%d, %f, %f), ", v.getId(), v.getPartId(),
					v.getNormalizedVertexWeight(), v.getNormalizedEdgeWeight()));
			count++;
			if (count >= 5)
				break;
		}
		sb.append(String.format("size: %d] ", clump.getVertices().size()));
		sb.append(String.format("to part.%d.", clump.getDestination()));
		
		return sb.toString();
	}
	
	private String printPartitionLoading(List<Partition> partitions) {
		StringBuilder sb = new StringBuilder();
		for (Partition p : partitions) {
			sb.append(String.format("Partition %d: local %f, cross %f, total %f\n", 
					p.getPartId(), p.getLocalLoad(), p.getCrossPartLoad(), p.getTotalLoad()));
		}
		return sb.toString();
	}
}

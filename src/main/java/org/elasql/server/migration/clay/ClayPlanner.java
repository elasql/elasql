package org.elasql.server.migration.clay;

import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.server.migration.MigrationManager;
import org.elasql.server.migration.MigrationPlan;
import org.elasql.server.migration.heatgraph.HeatGraph;
import org.elasql.server.migration.heatgraph.OutEdge;
import org.elasql.server.migration.heatgraph.Vertex;

public class ClayPlanner {
	private static Logger logger = Logger.getLogger(ClayPlanner.class.getName());
	
	public static final int MULTI_PARTS_COST = 5; // term 'k' in Clay's paper
//	public static final double OVERLOAD_THREASDHOLD = 15; // term 'theta' in Clay's paper
	public static final double OVERLOAD_PERCENTAGE = 1.2;
	private static final int LOOK_AHEAD_MAX = 5;
	
	private HeatGraph heatGraph;
	private int numOfClumpsGenerated = 0;
	private double overloadThreasdhold;
	
	public ClayPlanner(HeatGraph heatGraph) {
		this.heatGraph = heatGraph;
	}
	
	public List<MigrationPlan> generateMigrationPlan() {
		long startTime = System.currentTimeMillis();
		
		List<Partition> partitions = heatGraph.splitToPartitions();
		adjustOverloadThreasdhold(partitions);
		
		// Debug
		System.out.println(printPartitionLoading(partitions));
		System.out.println("Threasdhold: " + overloadThreasdhold);
		
		for (Partition targetPartition : partitions) {
			if (targetPartition.getTotalLoad() > overloadThreasdhold) {
				Clump clump = generateClump(targetPartition, partitions);
				
				if (clump == null)
					continue;
				
				// Generate migration plans
				List<MigrationPlan> plans = clump.toMigrationPlans();
				
				updateMigratedVertices(clump);
				
				if (logger.isLoggable(Level.INFO)) {
					logger.info("Clay takes " + (System.currentTimeMillis() - startTime) +
							" ms to generate clump no." + numOfClumpsGenerated);
//					logger.info("Generated migration plans: " + plans);
				}
				
				numOfClumpsGenerated++;
				
				return plans;
			}
		}
		
		return null;
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
		Clump candidateClump = null, finalClump = null;
		Partition destPart = null;
		int lookAhead = LOOK_AHEAD_MAX;
		Vertex addedVertex = null;
		
		while (true) {
			if (candidateClump == null) {
				addedVertex = overloadedPart.getHotestVertex();
				candidateClump = new Clump(addedVertex);
				destPart = findInitialDest(addedVertex, partitions);
				
//				System.out.println("Init clump: " + printClump(candidateClump));
			} else {
				if (!candidateClump.hasNeighbor())
					return finalClump;
				
				// Expand the clump
				int nId = candidateClump.getHotestNeighbor();
				addedVertex = heatGraph.getVertex(nId);
				candidateClump.expand(addedVertex);
				
//				System.out.println("Expanded clump: " + printClump(candidateClump));
//				System.out.println("Delta for sender: " + calcSendLoadDelta(candidateClump, addedVertex.getPartId()));
				
				destPart = updateDestination(candidateClump, destPart, partitions);
			}
			
			// Examine the clump
			if (isFeasible(candidateClump, destPart) &&
					calcSendLoadDelta(candidateClump, addedVertex.getPartId()) < 0) {
//				System.out.println("Found a good clump: " + printClump(candidateClump));
				
				finalClump = new Clump(candidateClump);
				finalClump.setDestination(destPart.getPartId());
			} else if (finalClump != null) {
				lookAhead--;
			}
			
			if (lookAhead == 0)
				return finalClump;
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
			int mostCoaccessedPart = clump.getMostCoaccessedPartition();
			if (mostCoaccessedPart != -1 && mostCoaccessedPart != oldDest.getPartId() &&
					isFeasible(clump, partitions.get(mostCoaccessedPart)))
				return partitions.get(mostCoaccessedPart);
			
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
				removedNodeLoad = v.getVertexWeight();
		
		// Cross-partition edge loading
		double addedCrossLoad = 0, reducedCrossLoad = 0;
		for (Vertex v : vertices) {
			if (v.getPartId() == senderPartId) {
				for (OutEdge e : v.getOutEdges().values()) {
					Vertex u = e.getOpposite();
					if (u.getPartId() == senderPartId) {
						if (!vertices.contains(u))
							addedCrossLoad += e.getWeight();
					} else {
						reducedCrossLoad += e.getWeight();
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
				addedNodeLoad = v.getVertexWeight();
		
		// Cross-partition edge loading
		double addedCrossLoad = 0, reducedCrossLoad = 0;
		for (Vertex v : vertices) {
			if (v.getPartId() != destPartId) {
				for (OutEdge e : v.getOutEdges().values()) {
					Vertex u = e.getOpposite();
					if (u.getPartId() == destPartId) {
						reducedCrossLoad += e.getWeight();
					} else {
						if (!vertices.contains(u))
							addedCrossLoad += e.getWeight();
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
	
	private String printClump(Clump clump) {
		StringBuilder sb = new StringBuilder("[");
		for (Vertex v : clump.getVertices()) {
			sb.append(String.format("%d (%d), ", v.getId(), v.getPartId()));
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.deleteCharAt(sb.length() - 1);
		sb.append("]");
		
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

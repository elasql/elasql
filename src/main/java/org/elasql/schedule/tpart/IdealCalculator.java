package org.elasql.schedule.tpart;

import java.util.Arrays;
import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class IdealCalculator  {

//	private double[] partLoads;
//	private boolean isOverloaded;
//	
//	public void reset() {
//		if (partLoads == null)
//			partLoads = new double[PartitionMetaMgr.NUM_PARTITIONS];
//		for (int i = 0; i < partLoads.length; i++)
//			partLoads[i] = 0;
//		isOverloaded = false;	
//	}
//	
//	// Analyze batch for looking-ahead when calculate the costs
//	public void analyzeBatch(List<TPartStoredProcedureTask> batch) {
//		// do nothing
//	}
//	
//	public double calAddNodeCost(Node newNode, TGraph graph) {
//		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
//		
//		// Find the main partition
//		int mainPartition = -1;
//		int count = 0;
//		for (RecordKey key : newNode.getTask().getReadSet()) {
//			int partId = partMgr.getPartition(key);
//			if (count == 0) {
//				mainPartition = partId;
//			} else if (count == 1) {
//				if (mainPartition == partId) {
//					break;
//				}
//			} else if (count == 2) {
//				mainPartition = partId;
//			}
//			count++;
//		}
//		
//		int nextPartId = (mainPartition + 1) % PartitionMetaMgr.NUM_PARTITIONS;
//		if (!isOverloaded && partLoads[mainPartition] - partLoads[nextPartId] > 20) {
//			isOverloaded = true;
//		}
//		
//		if (!isOverloaded) {
//			if (newNode.getPartId() == mainPartition)
//				return 0.0;
//			else
//				return 10000.0;
//		} else {
//			if (newNode.getPartId() == mainPartition)
//				return partLoads[mainPartition];
//			else if (newNode.getPartId() == nextPartId)
//				return partLoads[nextPartId];
//			else
//				return 10000.0;
//		}
//	}
//	
//	public void updateAddNodeCost(Node newNode, TGraph graph) {
//		partLoads[newNode.getPartId()] += 1;
//	}
//	
//	String getLoads() {
//		return Arrays.toString(partLoads);
//	}
}

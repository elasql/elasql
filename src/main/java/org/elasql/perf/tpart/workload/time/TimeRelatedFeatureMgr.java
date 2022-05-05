package org.elasql.perf.tpart.workload.time;

import java.util.Arrays;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.schedule.tpart.hermes.FusionTGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class TimeRelatedFeatureMgr {
	public static final int[] WINDOW_SIZES = new int[] { 100, 500, 1000 };
	public static final int SERVER_NUM = PartitionMetaMgr.NUM_PARTITIONS;

	private TimeRelatedInfoContainer[][] containers = new TimeRelatedInfoContainer[SERVER_NUM][WINDOW_SIZES.length];

	public TimeRelatedFeatureMgr() {
		for (int s = 0; s < SERVER_NUM; s++) {
			for (int w = 0; w < WINDOW_SIZES.length; w++) {
				containers[s][w] = new TimeRelatedInfoContainer(WINDOW_SIZES[w]);
			}
		}
	}

	public int pushInfo(TGraph graph) {
		PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
		int destPart = -1;
		
		for (TxNode txNode : graph.getTxNodes()) {
			TPartStoredProcedureTask task = txNode.getTask();
			destPart = txNode.getPartId();

			TimeRelatedInfo[] timeRelatedInfos = new TimeRelatedInfo[SERVER_NUM];
			for (int s = 0; s < SERVER_NUM; s++) {
				timeRelatedInfos[s] = new TimeRelatedInfo();
				timeRelatedInfos[s].time = task.getSequencerStartTime();
			}

			// read
			for (PrimaryKey key : task.getReadSet()) {
				// Skip fully replicated records
				if (partMgr.isFullyReplicated(key))
					continue;

				int partId = graph.getResourcePosition(key).getPartId();
				timeRelatedInfos[partId].readRecordNum += 1;
				timeRelatedInfos[partId].readRecordExcludingCacheNum += 1;

				partId = ((FusionTGraph) graph).getCachedLocation(key);
				if (partId != -1) {
					timeRelatedInfos[partId].readRecordExcludingCacheNum -= 1;
				}
			}

			// update
			for (PrimaryKey key : task.getUpdateSet()) {
				// Skip fully replicated records
				if (partMgr.isFullyReplicated(key))
					continue;

				int partId = graph.getResourcePosition(key).getPartId();
				timeRelatedInfos[partId].updateRecordNum += 1;
			}

			// insert
			timeRelatedInfos[destPart].insertRecordNum = task.getInsertSet().size();

			for (int s = 0; s < SERVER_NUM; s++) {
				for (int w = 0; w < WINDOW_SIZES.length; w++) {
					containers[s][w].push(timeRelatedInfos[s]);
				}
			}
		}
		
		return destPart;
	}

	public void onTxCommit(int serverId) {
		for (int w = 0; w < WINDOW_SIZES.length; w++) {
			containers[serverId][w].onTxCommit();
		}
	}

	public void calculate(long currentTime) {
		for (int s = 0; s < SERVER_NUM; s++) {
			for (int w = 0; w < WINDOW_SIZES.length; w++) {
				containers[s][w].calculate(currentTime);
			}
		}
	}

	public Integer[] getReadRecordNumInLastUs(int windowSizeInUs) {
		Integer[] readRecordSums = new Integer[SERVER_NUM];

		for (int w = 0; w < WINDOW_SIZES.length; w++) {
			if (WINDOW_SIZES[w] == windowSizeInUs) {
				final int windowIndex = w;
				Arrays.setAll(readRecordSums, i -> containers[i][windowIndex].getReadRecordSum());

				return readRecordSums;
			}
		}
		return null;
	}

	public Integer[] getReadRecordExcludingCacheNumInLastUs(int windowSizeInUs) {
		Integer[] readRecordExcludingCacheSums = new Integer[SERVER_NUM];

		for (int w = 0; w < WINDOW_SIZES.length; w++) {
			if (WINDOW_SIZES[w] == windowSizeInUs) {
				final int windowIndex = w;
				Arrays.setAll(readRecordExcludingCacheSums,
						i -> containers[i][windowIndex].getReadRecordExcludingCacheSum());

				return readRecordExcludingCacheSums;
			}
		}
		return null;
	}

	public Integer[] getUpdateRecordNumInLastUs(int windowSizeInUs) {
		Integer[] updateRecordSums = new Integer[SERVER_NUM];

		for (int w = 0; w < WINDOW_SIZES.length; w++) {
			if (WINDOW_SIZES[w] == windowSizeInUs) {
				final int windowIndex = w;
				Arrays.setAll(updateRecordSums, i -> containers[i][windowIndex].getUpdateRecordSum());

				return updateRecordSums;
			}
		}
		return null;
	}

	public Integer[] getInsertRecordNumInLastUs(int windowSizeInUs) {
		Integer[] insertRecordSums = new Integer[SERVER_NUM];

		for (int w = 0; w < WINDOW_SIZES.length; w++) {
			if (WINDOW_SIZES[w] == windowSizeInUs) {
				final int windowIndex = w;
				Arrays.setAll(insertRecordSums, i -> containers[i][windowIndex].getInsertRecordSum());

				return insertRecordSums;
			}
		}
		throw new RuntimeException("bad windowSize");
	}
	
	public Integer[] getCommitTxNumInLastUs(int windowSizeInUs) {
		Integer[] commitTxSums = new Integer[SERVER_NUM];

		for (int w = 0; w < WINDOW_SIZES.length; w++) {
			if (WINDOW_SIZES[w] == windowSizeInUs) {
				final int windowIndex = w;
				Arrays.setAll(commitTxSums, i -> containers[i][windowIndex].getRecentCommitSum());

				return commitTxSums;
			}
		}
		throw new RuntimeException("bad windowSize");
	}
}

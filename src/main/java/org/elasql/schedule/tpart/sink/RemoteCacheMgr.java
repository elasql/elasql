package org.elasql.schedule.tpart.sink;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasql.schedule.tpart.TPartPartitioner;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class RemoteCacheMgr {

	private Map<RecordKey, List<PositionInfo>> posInfoMap;

	public RemoteCacheMgr() {
		posInfoMap = new HashMap<RecordKey, List<PositionInfo>>();
	}

	public boolean contain(RecordKey key, long srcTxNum, int position) {
		List<PositionInfo> srcPoss = posInfoMap.get(key);
		if (srcPoss == null)
			return false;
		PositionInfo pi = null;
		for (PositionInfo p : srcPoss)
			if (p.srcTxNum == srcTxNum) {
				pi = p;
				break;
			}

		if (pi == null)
			return false;
		return pi.partitionIds[position];
	}

	public int[] getPosition(RecordKey key, long srcTxNum) {
		List<PositionInfo> srcPoss = posInfoMap.get(key);
		if (srcPoss == null)
			return null;
		PositionInfo pi = null;
		for (PositionInfo p : srcPoss)
			if (p.srcTxNum == srcTxNum) {
				pi = p;
				break;
			}

		if (pi == null)
			return null;
		return pi.getPosition();
	}

	public void add(RecordKey key, long srcTxNum, int... positions) {
		List<PositionInfo> srcPoss = posInfoMap.get(key);
		if (srcPoss == null) {
			srcPoss = new LinkedList<PositionInfo>();
			posInfoMap.put(key, srcPoss);
		}
		PositionInfo pi = null;
		for (PositionInfo p : srcPoss)
			if (p.srcTxNum == srcTxNum) {
				pi = p;
				break;
			}

		if (pi == null)
			pi = new PositionInfo(srcTxNum);
		pi.put(positions);
	}

	public void remove(RecordKey key, long srcTxNum, int... positions) {
		List<PositionInfo> srcPoss = posInfoMap.get(key);
		if (srcPoss == null)
			return;

		Iterator<PositionInfo> iter = srcPoss.iterator();
		PositionInfo pi;
		while (iter.hasNext()) {
			pi = iter.next();
			if (pi.srcTxNum == srcTxNum) {
				int n = pi.remove(positions);
				if (n == 0)
					iter.remove();
				break;
			}
		}
		if (srcPoss.size() == 0)
			posInfoMap.remove(key);
	}

	public void remove(RecordKey key, long srcTxNum) {
		List<PositionInfo> srcPoss = posInfoMap.get(key);
		if (srcPoss == null)
			return;

		Iterator<PositionInfo> iter = srcPoss.iterator();
		PositionInfo pi;
		while (iter.hasNext()) {
			pi = iter.next();
			if (pi.srcTxNum == srcTxNum) {
				iter.remove();
				break;
			}
		}
		if (srcPoss.size() == 0)
			posInfoMap.remove(key);
	}

	public void remove(RecordKey key) {
		posInfoMap.remove(key);
	}

	class PositionInfo {
		private long srcTxNum;
		private boolean[] partitionIds;
		private int numCopies;

		PositionInfo(long srcTxNum) {
			this.srcTxNum = srcTxNum;
			partitionIds = new boolean[PartitionMetaMgr.NUM_PARTITIONS];
			for (int i = 0; i < partitionIds.length; i++)
				partitionIds[i] = false;
		}

		long srcTxNum() {
			return srcTxNum;
		}

		void put(int... pos) {
			int c = 0;
			for (int i = 0; i < pos.length; i++)
				if (partitionIds[pos[i]] == false) {
					partitionIds[pos[i]] = true;
					c++;
				}

			numCopies += c;
		}

		int remove(int... pos) {
			int c = 0;
			for (int i = 0; i < pos.length; i++) {
				if (partitionIds[pos[i]] == true) {
					partitionIds[pos[i]] = false;
					c++;
				}
			}
			numCopies -= c;
			return numCopies;
		}

		int[] getPosition() {
			int[] result = new int[numCopies];
			int size = 0;
			for (int i = 0; i < partitionIds.length; i++) {
				if (partitionIds[i]) {
					result[size] = i;
					size++;
				}
			}
			return result;
		}
	}
}

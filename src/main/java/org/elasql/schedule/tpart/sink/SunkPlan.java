package org.elasql.schedule.tpart.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasql.sql.RecordKey;

public class SunkPlan {
	private int sinkProcessId;
	private boolean isLocalTask;

	// key->srcTxNum
	private Map<RecordKey, Long> readingInfoMap;

	// destServerId -> PushInfos
	private Map<Integer, Set<PushInfo>> pushingInfoMap;

	private List<RecordKey> localWriteBackInfo = new ArrayList<RecordKey>();

	private Map<Integer, List<RecordKey>> remoteWriteBackInfo;

	private Map<RecordKey, Set<Long>> writeDestMap = new HashMap<RecordKey, Set<Long>>();

	private Map<Integer, Set<PushInfo>> sinkPushingInfoMap = new HashMap<Integer, Set<PushInfo>>();

	private Set<RecordKey> sinkReadingSet = new HashSet<RecordKey>();

	public SunkPlan(int sinkProcessId, boolean isLocalTask) {
		this.sinkProcessId = sinkProcessId;
		this.isLocalTask = isLocalTask;
	}

	public void addReadingInfo(RecordKey key, long srcTxNum) {
		// not need to specify dest, that is the owner tx num

		if (readingInfoMap == null)
			readingInfoMap = new HashMap<RecordKey, Long>();
		readingInfoMap.put(key, srcTxNum);
	}

	public void addPushingInfo(RecordKey key, int targetNodeId, long srcTxNum, long destTxNum) {
		if (pushingInfoMap == null)
			pushingInfoMap = new HashMap<Integer, Set<PushInfo>>();
		Set<PushInfo> pushInfos = pushingInfoMap.get(targetNodeId);
		if (pushInfos == null) {
			pushInfos = new HashSet<PushInfo>();
			pushingInfoMap.put(targetNodeId, pushInfos);
		}
		pushInfos.add(new PushInfo(destTxNum, targetNodeId, key));
	}

	public void addWritingInfo(RecordKey key, long destTxNum) {
		if (writeDestMap.get(key) == null)
			writeDestMap.put(key, new HashSet<Long>());
		writeDestMap.get(key).add(destTxNum);
	}

	public void addSinkPushingInfo(RecordKey key, int destNodeId, long srcTxNum, long destTxNum) {
		Set<PushInfo> pushInfos = sinkPushingInfoMap.get(destNodeId);
		if (pushInfos == null) {
			pushInfos = new HashSet<PushInfo>();
			sinkPushingInfoMap.put(destNodeId, pushInfos);
		}
		pushInfos.add(new PushInfo(destTxNum, destNodeId, key));
	}

	public void addSinkReadingInfo(RecordKey key) {
		sinkReadingSet.add(key);
	}

	public Map<Integer, Set<PushInfo>> getSinkPushingInfo() {
		return sinkPushingInfoMap;
	}

	public Set<RecordKey> getSinkReadingInfo() {
		return sinkReadingSet;
	}

	public Long[] getWritingDestOfRecord(RecordKey key) {
		Set<Long> set = writeDestMap.get(key);
		return (set == null) ? null : set.toArray(new Long[0]);
	}

	public int sinkProcessId() {
		return sinkProcessId;
	}

	public boolean isLocalTask() {
		return isLocalTask;
	}

	public void addLocalWriteBackInfo(RecordKey key) {
		localWriteBackInfo.add(key);
	}

	public void addRemoteWriteBackInfo(RecordKey key, Integer destServerId) {
		if (remoteWriteBackInfo == null)
			remoteWriteBackInfo = new HashMap<Integer, List<RecordKey>>();
		List<RecordKey> keys = remoteWriteBackInfo.get(key);
		if (keys == null) {
			keys = new ArrayList<RecordKey>();
			remoteWriteBackInfo.put(destServerId, keys);
		}
		keys.add(key);
	}

	public long getReadSrcTxNum(RecordKey key) {
		return readingInfoMap.get(key);
	}

	public Map<Integer, Set<PushInfo>> getPushingInfo() {
		return pushingInfoMap;
	}

	public List<RecordKey> getLocalWriteBackInfo() {
		return localWriteBackInfo;
	}

	public Map<Integer, List<RecordKey>> getRemoteWriteBackInfo() {
		return remoteWriteBackInfo;
	}

	public boolean hasLocalWriteBack() {
		return localWriteBackInfo.size() > 0;
	}

	public boolean hasSinkPush() {
		return sinkPushingInfoMap.size() > 0;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("Sink Process Id: ");
		sb.append(sinkProcessId);
		sb.append("\n");

		sb.append("Is Local: ");
		sb.append(isLocalTask);
		sb.append("\n");

		sb.append("Reading Info: ");
		sb.append(readingInfoMap);
		sb.append("\n");

		sb.append("Pushing Info: ");
		sb.append(pushingInfoMap);
		sb.append("\n");

		sb.append("Local Writing Back Info: ");
		sb.append(localWriteBackInfo);
		sb.append("\n");

		sb.append("Remote Writing Back Info: ");
		sb.append(remoteWriteBackInfo);
		sb.append("\n");

		sb.append("Write Dest: ");
		sb.append(writeDestMap);
		sb.append("\n");

		sb.append("Sink Pushing Info: ");
		sb.append(sinkPushingInfoMap);
		sb.append("\n");

		sb.append("Sink Reading Info: ");
		sb.append(sinkReadingSet);
		sb.append("\n");

		return sb.toString();
	}
}
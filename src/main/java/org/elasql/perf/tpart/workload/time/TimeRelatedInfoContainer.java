package org.elasql.perf.tpart.workload.time;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TimeRelatedInfoContainer {
	private int windowSizeInUs;
	private ArrayDeque<TimeRelatedInfo> infoDeque = new ArrayDeque<TimeRelatedInfo>(1000);
	private ConcurrentLinkedQueue<Long> commitQueue = new ConcurrentLinkedQueue<Long>();
	private int readRecordSum;
	private int readRecordExcludingCacheSum;
	private int updateRecordSum;
	private int insertRecordSum;

	public TimeRelatedInfoContainer(int windowSizeInUs) {
		this.windowSizeInUs = windowSizeInUs;
	}

	public void push(TimeRelatedInfo info) {
		readRecordSum += info.readRecordNum;
		readRecordExcludingCacheSum += info.readRecordExcludingCacheNum;
		updateRecordSum += info.updateRecordNum;
		insertRecordSum += info.insertRecordNum;

		infoDeque.addLast(info);
	}

	public void onTxCommit() {
		long currentTime = System.nanoTime() / 1000;

		commitQueue.add(currentTime);

		while (commitQueue.size() > 0 && (currentTime - commitQueue.peek()) > windowSizeInUs) {
			commitQueue.remove();
		}
	}

	public void calculate(long currentTime) {
		// Drop the first element if the interval between the current timestamp and
		// the timestamp of the first element in the infoDeque is larger than window
		// size.
		while (infoDeque.size() > 0 && (currentTime - infoDeque.peekFirst().time) > windowSizeInUs) {
			TimeRelatedInfo headinfo = infoDeque.removeFirst();

			readRecordSum -= headinfo.readRecordNum;
			readRecordExcludingCacheSum -= headinfo.readRecordExcludingCacheNum;
			updateRecordSum -= headinfo.updateRecordNum;
			insertRecordSum -= headinfo.insertRecordNum;
		}
	}

	public int getReadRecordSum() {
		return readRecordSum;
	}

	public int getReadRecordExcludingCacheSum() {
		return readRecordExcludingCacheSum;
	}

	public int getUpdateRecordSum() {
		return updateRecordSum;
	}

	public int getInsertRecordSum() {
		return insertRecordSum;
	}

	public int getRecentCommitSum() {
		return commitQueue.size();
	}
}

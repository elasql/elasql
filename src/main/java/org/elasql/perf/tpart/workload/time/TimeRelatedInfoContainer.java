package org.elasql.perf.tpart.workload.time;

import java.util.ArrayDeque;

public class TimeRelatedInfoContainer {
	private int windowSizeInUs;
	private ArrayDeque<TimeRelatedInfo> deque = new ArrayDeque<TimeRelatedInfo>(1000);
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

		deque.addLast(info);
	}
	
	public void calculate(long currentTime) {
		// Drop the first element if the interval between the current timestamp and
		// the timestamp of the first element in the deque is larger than window size.
		while (deque.size() > 0 && (currentTime - deque.peekFirst().time) > windowSizeInUs) {
			TimeRelatedInfo headinfo = deque.removeFirst();
			
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
}

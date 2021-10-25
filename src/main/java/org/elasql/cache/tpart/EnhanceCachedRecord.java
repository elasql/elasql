package org.elasql.cache.tpart;

import org.elasql.cache.CachedRecord;

public class EnhanceCachedRecord {
	
	private CachedRecord cachedRecord;
	private boolean isRemote;
	
	public EnhanceCachedRecord(CachedRecord cachedRecord, boolean isRemote) {
		this.cachedRecord = cachedRecord;
		this.isRemote = isRemote;
	}
	
	public CachedRecord getCachedRecord() {
		return cachedRecord;
	}
	
	public boolean isRemote() {
		return isRemote;
	}
}

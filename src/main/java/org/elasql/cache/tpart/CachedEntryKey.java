package org.elasql.cache.tpart;

import org.elasql.sql.RecordKey;

public class CachedEntryKey {
	private final RecordKey recKey;
	private final long source;
	private final long dest;

	public CachedEntryKey(RecordKey key, long src, long dest) {
		recKey = key;
		source = src;
		this.dest = dest;
	}

	public RecordKey getRecordKey() {
		return recKey;
	}

	public long getSource() {
		return source;
	}

	public long getDestination() {
		return dest;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null)
			return false;
		if (obj.getClass() != CachedEntryKey.class)
			return false;
		CachedEntryKey key = (CachedEntryKey) obj;
		return key.recKey.equals(recKey) && key.source == this.source && key.dest == this.dest;
	}

	@Override
	public int hashCode() {
		int hashCode = 17;
		hashCode = 31 * hashCode + recKey.hashCode();
		hashCode = 31 * hashCode + (int) (dest ^ (dest >>> 32));
		hashCode = 31 * hashCode + (int) (source ^ (source >>> 32));
		return hashCode;
	}

	@Override
	public String toString() {
		return "[" + recKey.toString() + ", src:" + source + ", dest:" + dest + "]";
	}
}

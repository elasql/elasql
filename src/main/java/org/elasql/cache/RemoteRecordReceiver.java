package org.elasql.cache;

import org.elasql.sql.RecordKey;

public interface RemoteRecordReceiver {

	void cacheRemoteRecord(RecordKey key, CachedRecord rec);
	
}

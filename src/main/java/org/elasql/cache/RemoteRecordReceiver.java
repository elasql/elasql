package org.elasql.cache;

import org.elasql.remote.groupcomm.Tuple;
import org.elasql.sql.RecordKey;

public interface RemoteRecordReceiver {
	
	void cacheRemoteRecord(Tuple t);
	
}

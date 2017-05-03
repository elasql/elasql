package org.elasql.cache;

import org.elasql.remote.groupcomm.Tuple;

public interface RemoteRecordReceiver {
	
	void cacheRemoteRecord(Tuple t);
	
}

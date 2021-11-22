package org.elasql.remote.groupcomm;

import java.io.Serializable;


public class SyncRequest implements Serializable {
	int serverId;
	TimeSync timeSync;

	public SyncRequest(int s_id, TimeSync sync) {
		this.serverId = s_id;
		this.timeSync = sync;
	}

	public int getServerID() {
		return this.serverId;
	}

	public TimeSync getTimeSync() {
		return this.timeSync;
	}

	public boolean isRequest() {
		return this.timeSync.isRequest();
	}
}
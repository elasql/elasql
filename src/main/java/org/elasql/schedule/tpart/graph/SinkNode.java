package org.elasql.schedule.tpart.graph;

import org.elasql.cache.tpart.TPartCacheMgr;

public class SinkNode extends Node {
	
	public SinkNode(int partId) {
		setPartId(partId);
	}

	@Override
	public double getWeight() {
		// TODO: Should be the loading of the server
		// but it is not used for now. So it's fine.
		return 1;
	}

	@Override
	public boolean isSinkNode() {
		return true;
	}

	@Override
	public long getTxNum() {
		return TPartCacheMgr.toSinkId(getPartId());
	}
}

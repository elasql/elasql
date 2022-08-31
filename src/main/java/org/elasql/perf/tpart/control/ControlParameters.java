package org.elasql.perf.tpart.control;

import java.util.Arrays;

import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

public class ControlParameters {
	
	public static final double INITIAL_ALPHA;
	
	static {
		INITIAL_ALPHA = ElasqlProperties.getLoader().getPropertyAsDouble(
				ControlParamUpdater.class.getName() + ".INITIAL_ALPHA", 1.0);
	}
	
	private double[] alpha;
	
	public ControlParameters() {
		this.alpha = new double[PartitionMetaMgr.NUM_PARTITIONS];
		Arrays.fill(alpha, INITIAL_ALPHA);
	}
	
	public ControlParameters(double[] alpha) {
		this.alpha = alpha;
	}
	
	public double alpha(int partId) {
		return alpha[partId];
	}
	
	@Override
	public String toString() {
		return String.format("[alpha: %s]", Arrays.toString(alpha));
	}
}

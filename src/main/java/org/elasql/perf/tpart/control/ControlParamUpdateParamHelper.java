package org.elasql.perf.tpart.control;

import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

/**
 * A helper that carries the latest parameters controlled by {@code RoutingControlActuator}.
 * 
 * @author Yu-Shan Lin
 */
public class ControlParamUpdateParamHelper extends StoredProcedureParamHelper {
	
	// Check {@code RoutingControlActuator} for the meaning of these variables.
	private double[] alpha;
	private double[] beta;
	private double[] gamma;

	@Override
	public void prepareParameters(Object... pars) {
		
		alpha = new double[PartitionMetaMgr.NUM_PARTITIONS];
		beta = new double[PartitionMetaMgr.NUM_PARTITIONS];
		gamma = new double[PartitionMetaMgr.NUM_PARTITIONS];
		
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			alpha[nodeId] = (Double) pars[nodeId];
			beta[nodeId] = (Double) pars[PartitionMetaMgr.NUM_PARTITIONS + nodeId];
			gamma[nodeId] = (Double) pars[PartitionMetaMgr.NUM_PARTITIONS * 2 + nodeId];
		}
	}
	
	public double getAlpha(int nodeId) {
		return alpha[nodeId];
	}
	
	public double getBeta(int nodeId) {
		return beta[nodeId];
	}
	
	public double getGamma(int nodeId) {
		return gamma[nodeId];
	}

	@Override
	public Schema getResultSetSchema() {
		return new Schema();
	}

	@Override
	public SpResultRecord newResultSetRecord() {
		return new SpResultRecord();
	}
}

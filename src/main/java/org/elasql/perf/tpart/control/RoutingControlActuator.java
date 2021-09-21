package org.elasql.perf.tpart.control;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.server.Elasql;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.server.task.Task;

/**
 * The actuator controls the parameters of {@code ControlBasedRouter} based
 * on the current system metrics.
 * 
 * @author Yu-Shan Lin
 */
public class RoutingControlActuator extends Task {
	
	private static final long UPDATE_PERIOD;
	
	static {
		UPDATE_PERIOD = ElasqlProperties.getLoader().getPropertyAsLong(
				RoutingControlActuator.class.getName() + ".UPDATE_PERIOD", 60_000);
	}
	
	private PidController[] alpha;
	private PidController[] beta;
	private PidController[] gamma;
	
	private TpartMetricWarehouse metricWarehouse;

	public RoutingControlActuator(TpartMetricWarehouse metricWarehouse) {
		this.metricWarehouse = metricWarehouse;
		
		alpha = new PidController[PartitionMetaMgr.NUM_PARTITIONS];
		beta = new PidController[PartitionMetaMgr.NUM_PARTITIONS];
		gamma = new PidController[PartitionMetaMgr.NUM_PARTITIONS];
		
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			alpha[nodeId] = new PidController(1.0);
			beta[nodeId] = new PidController(1.0);
			gamma[nodeId] = new PidController(1.0);
		}
	}
	
	@Override
	public void run() {
		Thread.currentThread().setName("routing-control-aucuator");
		
		long startTime = System.currentTimeMillis();
		
		while (true) {
			// Wait for the next update
			waitForUpdate(startTime);
			startTime = System.currentTimeMillis();
			
			// Get observation values
			acquireObservations();
			
			// Update reference values
			updateReferences();
			
			// Update parameters
			updateParameters();
			
			// Issue an update transaction
			issueUpdateTransaction();
		}
	}
	
	private void waitForUpdate(long startTime) {
		while (System.currentTimeMillis() - startTime < UPDATE_PERIOD) {
			try {
				Thread.sleep(UPDATE_PERIOD / 10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void acquireObservations() {
		// TODO: add disk and network I/O
		// XXX: right observation?
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++)
			alpha[nodeId].setObservation(metricWarehouse.getSystemCpuLoad(nodeId));
	}
	
	private void updateReferences() {
		// TODO: add disk and network I/O
		// XXX: right reference?
		double sum = 0;
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++)
			sum += alpha[nodeId].getObservation();
		double average = sum / PartitionMetaMgr.NUM_PARTITIONS;
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++)
			alpha[nodeId].setReference(average);
	}
	
	private void updateParameters() {
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++)
			alpha[nodeId].updateControlParameters(UPDATE_PERIOD);
	}
	
	private void issueUpdateTransaction() {
		// Prepare the parameters
		Object[] params = new Object[PartitionMetaMgr.NUM_PARTITIONS * 3];
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			params[nodeId] = 
					alpha[nodeId].getControlParameter();
			params[PartitionMetaMgr.NUM_PARTITIONS + nodeId] = 
					beta[nodeId].getControlParameter();
			params[PartitionMetaMgr.NUM_PARTITIONS * 2 + nodeId] = 
					gamma[nodeId].getControlParameter();
		}
		
		// Send a store procedure call
		Elasql.connectionMgr().sendStoredProcedureCall(false, 
				ControlStoredProcedureFactory.SP_CONTROL_PARAM_UPDATE, params);
	}
}

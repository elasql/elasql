package org.elasql.perf.tpart.ai;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.estimator.Constants;
import org.elasql.estimator.model.SingleServerMasterModel;
import org.elasql.estimator.model.SumMaxSequentialModel;
import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

import smile.data.Tuple;
import smile.data.type.StructType;

public class SumMaxEstimator implements Estimator {
	private static Logger logger = Logger.getLogger(SumMaxEstimator.class.getName());
	
	private static final String MODEL_DIR;
	
	private static final StructType FEATURE_SCHEMA = Constants.FEATURE_SCHEMA;
	private static final String[] NON_ARRAY_FEATURES = Constants.NON_ARRAY_FEATURES;
	private static final String[] ARRAY_FEATURES = Constants.ARRAY_FEATURES;
	
	private static final int SERVER_COUNT = PartitionMetaMgr.NUM_PARTITIONS;
	
	static {
		MODEL_DIR = ElasqlProperties.getLoader()
				.getPropertyAsString(SumMaxEstimator.class.getName() + ".MODEL_DIR",
						null);
	}
	
	private SumMaxSequentialModel model;
	
	public SumMaxEstimator() {
		model = new SumMaxSequentialModel(loadModels());
	}

	@Override
	public TransactionEstimation estimate(TransactionFeatures features) {
		TransactionEstimation.Builder builder = new TransactionEstimation.Builder();

		// Get latency estimation
		double[] latencies = estimateLatency(features);
		for (int masterId = 0; masterId < SERVER_COUNT; masterId++)
			builder.setLatency(masterId, latencies[masterId]);

		for (int masterId = 0; masterId < SERVER_COUNT; masterId++)
			builder.setMasterCpuCost(masterId, estimateMasterCpuCost(features, masterId));

		for (int slaveId = 0; slaveId < SERVER_COUNT; slaveId++)
			builder.setSlaveCpuCost(slaveId, estimateSlaveCpuCost(features, slaveId));

		return builder.build();
	}

	@Override
	public void notifyTransactionRoute(long txNum, int masterId) {
		model.decideLastTxnDest(txNum, masterId);
	}

	private double[] estimateLatency(TransactionFeatures features) {
		Tuple[] serverFeatures = new Tuple[SERVER_COUNT];
		
		for (int serverId = 0; serverId < SERVER_COUNT; serverId++) {
			serverFeatures[serverId] = preprocessFeatures(features, serverId);
		}
		
		return model.predictNextTxnLatency(
			features.getTxNum(),
			features.getDependencies(),
			(Long) features.getFeature("Start Time"),
			serverFeatures
		);
	}

	private long estimateMasterCpuCost(TransactionFeatures features, int masterId) {
		return 190;
	}

	private long estimateSlaveCpuCost(TransactionFeatures features, int slaveId) {
		return 90;
	}
	
	private Tuple preprocessFeatures(TransactionFeatures features, int masterId) {
		Object[] row = new Object[FEATURE_SCHEMA.length()];
		
		for (int i = 0; i < NON_ARRAY_FEATURES.length; i++) {
			String fieldName = NON_ARRAY_FEATURES[i];
			Object valObj = features.getFeature(fieldName);
			
			double value = 1.0;
			if (valObj.getClass().equals(Integer.class)) {
				Integer intVal = (Integer) valObj;
				value = intVal.doubleValue();
			} else if (valObj.getClass().equals(Long.class)) {
				Long longVal = (Long) valObj;
				value = longVal.doubleValue();
			} else if (valObj.getClass().equals(Double.class)) {
				Double doubleVal = (Double) valObj;
				value = doubleVal.doubleValue();
			}

			row[FEATURE_SCHEMA.fieldIndex(fieldName)] = value;
		}
		
		for (int i = 0; i < ARRAY_FEATURES.length; i++) {
			String fieldName = ARRAY_FEATURES[i];
			Object valObj = features.getFeature(fieldName);
			
			double value = 1.0;
			if (valObj.getClass().equals(Integer[].class)) {
				Integer[] intArray = (Integer[]) valObj;
				value = intArray[masterId].doubleValue();
			} else if (valObj.getClass().equals(Long[].class)) {
				Long[] longArray = (Long[]) valObj;
				value = longArray[masterId].doubleValue();
			} else if (valObj.getClass().equals(Double[].class)) {
				Double[] doubleArray = (Double[]) valObj;
				value = doubleArray[masterId].doubleValue();
			}

			row[FEATURE_SCHEMA.fieldIndex(fieldName)] = value;
		}
		
		return Tuple.of(row, FEATURE_SCHEMA);
	}
	
	private List<SingleServerMasterModel> loadModels() {
		// Check if the directory exists
		if (MODEL_DIR == null)
			throw new RuntimeException("please sepecify the path to "
					+ "the estimator models in properties");
		
		File modelDir = new File(MODEL_DIR);
		if (!modelDir.exists() || !modelDir.isDirectory())
			throw new RuntimeException(String.format("%s is not a directory",
					MODEL_DIR));
		
		// Load the models
		List<SingleServerMasterModel> models = new ArrayList<SingleServerMasterModel>();
		try {
			for (int serverId = 0; serverId < PartitionMetaMgr.NUM_PARTITIONS; serverId++) {
				File modelFilePath = new File(modelDir, "model-" + serverId + ".bin");
				SingleServerMasterModel model = SingleServerMasterModel.loadFromFile(modelFilePath);
				models.add(model);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		if (logger.isLoggable(Level.INFO))
			logger.info("The pretrained models are loaded.");
		
		return models;
	}
}

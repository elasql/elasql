package org.elasql.perf.tpart.ai;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.estimator.model.SingleServerMasterModel;
import org.elasql.estimator.model.SumMaxSequentialModel;
import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.util.TransactionProfiler;

import smile.data.Tuple;
import smile.data.type.StructField;
import smile.data.type.StructType;

public class SumMaxEstimator implements Estimator {
    private static Logger logger = Logger.getLogger(SumMaxEstimator.class.getName());
    
    private static final String MODEL_DIR;
    
    private static final int SERVER_COUNT = PartitionMetaMgr.NUM_PARTITIONS;
    
    static {
        MODEL_DIR = ElasqlProperties.getLoader()
                .getPropertyAsString(SumMaxEstimator.class.getName() + ".MODEL_DIR",
                        null);
    }
    
    private SumMaxSequentialModel model;
    private StructType featureSchema;
    
    public SumMaxEstimator() {
        model = new SumMaxSequentialModel(loadModels());
        featureSchema = model.schema();
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

    public void notifyTransactionRoute(long txNum, int masterId) {
        model.decideLastTxnDest(txNum, masterId);
    }

    private double[] estimateLatency(TransactionFeatures features) {
        Tuple[] serverFeatures = new Tuple[SERVER_COUNT];
        
        TransactionProfiler profiler = TransactionProfiler.getLocalProfiler();
        
        profiler.startComponentProfiler("- preprocess");
        for (int serverId = 0; serverId < SERVER_COUNT; serverId++) {
            serverFeatures[serverId] = preprocessFeatures(features, serverId);
        }
        profiler.stopComponentProfiler("- preprocess");

        profiler.startComponentProfiler("- prediction");
        double[] result = model.predictNextTxnLatency(
            features.getTxNum(),
            features.getDependencies(),
            (Long) features.getFeature("Start Time"),
            serverFeatures
        );
        profiler.stopComponentProfiler("- prediction");
        model.decideLastTxnDest(features.getTxNum(), 0); // Run all transactions on one machine
        
        return result;
    }

    private long estimateMasterCpuCost(TransactionFeatures features, int masterId) {
        return 190;
    }

    private long estimateSlaveCpuCost(TransactionFeatures features, int slaveId) {
        return 90;
    }
    
    private Tuple preprocessFeatures(TransactionFeatures features, int masterId) {
        Object[] row = new Object[featureSchema.length()];
        
        for (StructField field : featureSchema.fields()) {
            Object valObj = features.getFeature(field.name);
            
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
            } else if (valObj.getClass().equals(Integer[].class)) {
                Integer[] intArray = (Integer[]) valObj;
                value = intArray[masterId].doubleValue();
            } else if (valObj.getClass().equals(Long[].class)) {
                Long[] longArray = (Long[]) valObj;
                value = longArray[masterId].doubleValue();
            } else if (valObj.getClass().equals(Double[].class)) {
                Double[] doubleArray = (Double[]) valObj;
                value = doubleArray[masterId].doubleValue();
            }

            row[featureSchema.fieldIndex(field.name)] = value;
        }
        
        return Tuple.of(row, featureSchema);
    }
    
    private List<SingleServerMasterModel> loadModels() {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("Loading pretrained models...");
        }
        // Check if the directory exists
        if (MODEL_DIR == null)
            throw new RuntimeException("please specify the path to "
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
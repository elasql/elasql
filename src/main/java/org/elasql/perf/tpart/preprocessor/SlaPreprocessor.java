package org.elasql.perf.tpart.preprocessor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.elasql.perf.tpart.TPartPerformanceManager;
import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.ai.TransactionEstimation;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.workload.CriticalTransactionRecorder;
import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.client.BatchSpcSender;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.TPartScheduler;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;

/**
 * A preprocessor optimized for latency SLA
 * 
 * @author wilbertharriman
 *
 */
public class SlaPreprocessor extends SpCallPreprocessor {
    private List<StoredProcedureCall> txQueue = new ArrayList<>();
    public SlaPreprocessor(TPartStoredProcedureFactory factory,
            BatchNodeInserter inserter, TGraph graph,
            boolean isBatching, TpartMetricWarehouse metricWarehouse,
            Estimator performanceEstimator) {
        super(factory, inserter, graph, isBatching, metricWarehouse, performanceEstimator);
    }
    
    @Override
    public void run() {
        List <TPartStoredProcedureTask> batchedTasks = new ArrayList<>();
        List<Serializable> sendingList = new ArrayList<>();
        
        Thread.currentThread().setName("sla-preprocessor");

        while (true) {
            try {
                StoredProcedureCall spc = spcQueue.take();

                // Get the read-/write-set by creating StoredProcedureTask
                TPartStoredProcedureTask task = createSpTask(spc);
                
                if (task.getProcedureType() == ProcedureType.NORMAL ||
                        task.getProcedureType() == ProcedureType.CONTROL) {
                    // Pre-process the transaction
                    preprocess(spc, task);
                    // Get critical transactions
                    double latencyEstimate = task.getEstimation().getAvgLatency();
                    if (latencyEstimate > TPartPerformanceManager.TRANSACTION_DEADLINE - TPartPerformanceManager.ESTIMATION_ERROR &&
                            latencyEstimate < TPartPerformanceManager.TRANSACTION_DEADLINE + TPartPerformanceManager.ESTIMATION_ERROR) {
                        sendingList.add(spc);
                    } else {
                        txQueue.add(spc);
                    }
                    // Add to the schedule batch
                    batchedTasks.add(task);
                } else {
                    txQueue.add(spc);
                }

                if (sendingList.size() + txQueue.size() >= BatchSpcSender.COMM_BATCH_SIZE) {
                    // Send the SP call batch to total ordering
                    sendingList.addAll(txQueue);
                    Elasql.connectionMgr().sendTotalOrderRequest(sendingList);
                    sendingList = new ArrayList<>(); // clear sending list
                    txQueue = new ArrayList<>();
                }
                
                // Process the batch as TPartScheduler does
                if (isBatching && batchedTasks.size() >= TPartScheduler.SCHEDULE_BATCH_SIZE
                        || !isBatching) {
                    // Route the task batch
                    routeBatch(batchedTasks);
                    batchedTasks.clear();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void preprocess(StoredProcedureCall spc, TPartStoredProcedureTask task) {
        TransactionFeatures features = featureExtractor.extractFeatures(task, graph, keyHasBeenRead, lastTxRoutingDest);
        featureExtractor.addDependency(task);

        bookKeepKeys(task);

        if (performanceEstimator == null) {
            throw new RuntimeException("Performance estimator is not loaded.");
        }

        TransactionEstimation estimation = performanceEstimator.estimate(features);

        // Record the feature if necessary
        if (TPartPerformanceManager.ENABLE_COLLECTING_DATA &&
                task.getProcedureType() != ProcedureType.CONTROL) {
            featureRecorder.record(features);
            dependencyRecorder.record(features);
            criticalTransactionRecorder.record(task, estimation);
        }

        // Save the estimation
        spc.setMetadata(estimation.toBytes());
        task.setEstimation(estimation);
    }
}

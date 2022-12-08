package org.elasql.perf.tpart;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.ai.TransactionEstimation;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.workload.CriticalTransactionRecorder;
import org.elasql.perf.tpart.workload.TransactionDependencyRecorder;
import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.perf.tpart.workload.TransactionFeaturesRecorder;
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
    private final double DEADLINE = 12500; // TODO: different deadline for each transaction type
    private final double THRESHOLD = 2500; // us

    private List<StoredProcedureCall> txQueue = new ArrayList<>();
    CriticalTransactionRecorder criticalTransactionRecorder = new CriticalTransactionRecorder();

    private class StoredProcedureCallOrder implements Comparable<StoredProcedureCallOrder> {
        
        private final StoredProcedureCall spc;
        private final TPartStoredProcedureTask task;
        private TransactionEstimation est;
        private final Comparator<TransactionEstimation> latencyOrder = Comparator.comparingDouble(
                TransactionEstimation::getAvgLatency
        );
        
        private StoredProcedureCallOrder(StoredProcedureCall spc) {
            this.spc = spc;
            this.task = createSpTask(spc);
        }
        
        private StoredProcedureCall getStoredProcedureCall() {
            return spc;
        }
        
        private TPartStoredProcedureTask getStoredProcedureTask() {
            return task;
        }
        
        private void setEstimation(TransactionEstimation est) {
            this.est = est;
        }
        
        private void setMetadata(Serializable metadata) {
            this.spc.setMetadata(metadata);
        }

        @Override
        public int compareTo(StoredProcedureCallOrder spc2) {
            return latencyOrder.compare(est, spc2.est);
        }
    }
    
    public SlaPreprocessor(TPartStoredProcedureFactory factory,
            BatchNodeInserter inserter, TGraph graph,
            boolean isBatching, TpartMetricWarehouse metricWarehouse,
            Estimator performanceEstimator) {
        super(factory, inserter, graph, isBatching, metricWarehouse, performanceEstimator);
        if (TPartPerformanceManager.ENABLE_COLLECTING_DATA) {
            criticalTransactionRecorder = new CriticalTransactionRecorder();
            criticalTransactionRecorder.startRecording();
        }
    }
    
    @Override
    public void run() {
        List <TPartStoredProcedureTask> batchedTasks = new ArrayList<>();
        List<Serializable> sendingList = new ArrayList<>();
        
        Thread.currentThread().setName("sla-preprocessor");

        while (true) {
            try {
                StoredProcedureCallOrder spc = new StoredProcedureCallOrder(spcQueue.take());

                // Get the read-/write-set by creating StoredProcedureTask
                TPartStoredProcedureTask task = spc.getStoredProcedureTask();
                
                if (task.getProcedureType() == ProcedureType.NORMAL ||
                        task.getProcedureType() == ProcedureType.CONTROL) {
                    // Pre-process the transaction
                    preprocess(spc, task);
                    // Get critical transactions
                    if (spc.est.getAvgLatency() > DEADLINE - THRESHOLD &&
                            spc.est.getAvgLatency() < DEADLINE + THRESHOLD) {
                        sendingList.add(spc.getStoredProcedureCall());
                    } else {
                        txQueue.add(spc.getStoredProcedureCall());
                    }
                    // Add to the schedule batch
                    batchedTasks.add(task);
                } else {
                    txQueue.add(spc.getStoredProcedureCall());
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

    private void preprocess(StoredProcedureCallOrder spc, TPartStoredProcedureTask task) {
        TransactionFeatures features = featureExtractor.extractFeatures(task, graph, keyHasBeenRead, lastTxRoutingDest);
        
        // records must be read from disk if they are never read
        bookKeepKeys(task);

        if (performanceEstimator == null) {
            throw new RuntimeException("Performance estimator is not loaded.");
        }
        TransactionEstimation estimation = performanceEstimator.estimate(features);

        // Record the feature to CSV if necessary
        if (TPartPerformanceManager.ENABLE_COLLECTING_DATA) {
            featureRecorder.record(features);
            dependencyRecorder.record(features);
            criticalTransactionRecorder.record(task, estimation);
        }

        // Save the estimation
        spc.setMetadata(estimation.toBytes());
        spc.setEstimation(estimation);
        task.setEstimation(estimation);
    }
}

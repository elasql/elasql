package org.elasql.perf.tpart.preprocessor;

import org.elasql.perf.tpart.TPartPerformanceManager;
import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.ai.TransactionEstimation;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.client.BatchSpcSender;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.TPartScheduler;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A preprocessor that is allowed to drop transactions that violate the deadline SLA
 */
public class DropPreprocessor extends SpCallPreprocessor {

    public DropPreprocessor(TPartStoredProcedureFactory factory, BatchNodeInserter inserter, TGraph graph, boolean isBatching, TpartMetricWarehouse metricWarehouse, Estimator performanceEstimator) {
        super(factory, inserter, graph, isBatching, metricWarehouse, performanceEstimator);
    }

    @Override
    public void run() {
        List<TPartStoredProcedureTask> batchedTasks = new ArrayList<>();
        List<Serializable> sendingList = new ArrayList<>();

        while (true) {
            try {
                StoredProcedureCall spc = spcQueue.take();
                TPartStoredProcedureTask task = createSpTask(spc);
                if (task.getProcedureType() == ProcedureType.NORMAL) {
                    preprocess(spc, task);
                    double latencyEstimate = task.getEstimation().getAvgLatency();
                    // Only execute transactions that do not violate the SLA
                    if (latencyEstimate <= TPartPerformanceManager.TRANSACTION_DEADLINE + TPartPerformanceManager.ESTIMATION_ERROR) {
                        featureExtractor.addDependency(task);
                        sendingList.add(spc);
                        batchedTasks.add(task);
                    } else {
                        if (task.clientId != -1) {
                            // abort transaction
                            Elasql.connectionMgr().sendClientResponse(task.clientId, task.connectionId, task.getTxNum(), new SpResultSet(false, null));
                        }
                    }
                } else {
                    sendingList.add(spc);
                }

                if (sendingList.size() >= BatchSpcSender.COMM_BATCH_SIZE) {
                    Elasql.connectionMgr().sendTotalOrderRequest(sendingList);
                    sendingList = new ArrayList<>();
                }

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

        bookKeepKeys(task);

        if (performanceEstimator == null) {
            throw new RuntimeException("Performance estimator is not loaded.");
        }

        TransactionEstimation estimation = performanceEstimator.estimate(features);

        // Record the feature if necessary
        if (TPartPerformanceManager.ENABLE_COLLECTING_DATA) {
            featureRecorder.record(features);
            dependencyRecorder.record(features);
            criticalTransactionRecorder.record(task, estimation);
        }

        // Save the estimation
        spc.setMetadata(estimation.toBytes());
        task.setEstimation(estimation);
    }
}

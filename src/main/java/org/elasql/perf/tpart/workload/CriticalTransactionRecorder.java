package org.elasql.perf.tpart.workload;

import org.elasql.perf.tpart.TPartPerformanceManager;
import org.elasql.perf.tpart.ai.TransactionEstimation;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.server.Elasql;
import org.vanilladb.core.server.task.Task;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CriticalTransactionRecorder extends Task {
    private static Logger logger = Logger.getLogger(CriticalTransactionRecorder.class.getName());
    private static final String FILENAME = "critical-transactions.csv";
    private static final long TIME_TO_FLUSH = 10; // in seconds
    private static class CriticalTransactionRow {
        long txNum;
        double txType;
        double estimatedLatency;
        boolean isCritical;
        public CriticalTransactionRow(long txNum, double txType, double estimatedLatency, boolean isCritical) {
            this.txNum = txNum;
            this.txType = txType;
            this.estimatedLatency = estimatedLatency;
            this.isCritical = isCritical;
        }
    }

    private BlockingQueue<CriticalTransactionRow> queue = new LinkedBlockingQueue<>();
    private AtomicBoolean isRecording = new AtomicBoolean(false);

    public void startRecording() {
        if (!isRecording.getAndSet(true)) {
            Elasql.taskMgr().runTask(this);
        }
    }

    public void record(TPartStoredProcedureTask task, TransactionEstimation estimation) {
        double estimatedLatency = estimation.getAvgLatency();
        boolean isCritical = estimatedLatency > TPartPerformanceManager.TRANSACTION_DEADLINE - TPartPerformanceManager.ESTIMATION_ERROR &&
                estimatedLatency < TPartPerformanceManager.TRANSACTION_DEADLINE + TPartPerformanceManager.ESTIMATION_ERROR;
        queue.add(new CriticalTransactionRow(task.getTxNum(), task.getWeight(), estimatedLatency, isCritical));
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Critical Transaction Recorder");

        try {
            CriticalTransactionRow row = queue.take();

            if (logger.isLoggable(Level.INFO)) {
                logger.info("Critical transaction recorder starts");
            }

            saveToFile(row);


        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void saveToFile(CriticalTransactionRow firstRow) throws InterruptedException {
        try (PrintWriter writer = new PrintWriter(FILENAME)) {
            writeHeader(writer);
            writeRow(writer, firstRow);
            CriticalTransactionRow row;

            while ((row = queue.poll(TIME_TO_FLUSH, TimeUnit.SECONDS)) != null) {
                writeRow(writer, row);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        if (logger.isLoggable(Level.INFO)) {
            String log = String.format("No more features coming in last %d seconds. Start generating a report.",
                    TIME_TO_FLUSH);
            logger.info(log);
        }

        if (logger.isLoggable(Level.INFO)) {
            String log = String.format("A feature log is generated at \"%s\"", FILENAME);
            logger.info(log);
        }
    }

    private void writeHeader(PrintWriter writer) {
        writer.format("%s,%s,%s,%s", "Transaction ID", "Type", "Estimated latency", "Is critical");
        writer.println();
    }

    private void writeRow(PrintWriter writer, CriticalTransactionRow row) {
        writer.format("%s,%s,%s,%s", row.txNum, row.txType, row.estimatedLatency, row.isCritical);
        writer.println();
    }
}

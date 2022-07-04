package org.elasql.perf.tpart.ai;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.elasql.cache.tpart.TPartCacheMgr;
import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class PythonSubProcessEstimator implements Estimator {
	private static Logger logger = Logger.getLogger(TPartCacheMgr.class.getName());

	// XXX: for quick testing
	private static final String PYTHON_RUNTIME = "/usr/local/bin/python3.8";
	private static final String SCRIPT_PATH = "/home/db-team/cost-estimator/serve.py";

	private static double[] parseDoubleArray(String arrayStr) {
		arrayStr = arrayStr.trim();
		arrayStr = arrayStr.substring(1, arrayStr.length() - 1);
		String[] numStrs = arrayStr.split(",");
		double[] nums = new double[numStrs.length];
		for (int i = 0; i < numStrs.length; i++) {
			nums[i] = Double.parseDouble(numStrs[i].trim());
		}
		return nums;
	}

	private Process process;
	private BufferedWriter output;
	private BufferedReader input;
	private BufferedReader error;

	public PythonSubProcessEstimator() {
		List<String> cmd = new ArrayList<String>();
		cmd.add(PYTHON_RUNTIME);
		cmd.add(SCRIPT_PATH);

		try {
			ProcessBuilder builder = new ProcessBuilder();
			logger.info("Pyhon SubProcess Estimator command is: " + cmd);

			builder.command(cmd);
			builder.redirectError(Redirect.INHERIT);
			process = builder.start();

			output = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), "UTF-8"));
			input = new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));
//			error = new BufferedReader(new InputStreamReader(process.getErrorStream(), "UTF-8"));

			waitForStart();
		} catch (IOException e) {
			throw new RuntimeException(
					String.format("%s got an error: %s", this.getClass().getSimpleName(), e.getMessage()));
		}
	}

	private void waitForStart() throws IOException {
		logger.info("waiting for a python cost-estimator starting signal");

		String response;
		while (true) {
			response = input.readLine();
			logger.info("received a signal: " + response);
			if (response.contains("PYTHON-INFO: start serving")) {
				logger.info("received a STARTING signal");
				break;
			}
		}
	}

	@Override
	public TransactionEstimation estimate(TransactionFeatures features) {
		TransactionEstimation.Builder builder = new TransactionEstimation.Builder();

		// Get latency estimation
		double[] latencies = estimateLatency(features);
		for (int masterId = 0; masterId < PartitionMetaMgr.NUM_PARTITIONS; masterId++)
			builder.setLatency(masterId, latencies[masterId]);

		for (int masterId = 0; masterId < PartitionMetaMgr.NUM_PARTITIONS; masterId++)
			builder.setMasterCpuCost(masterId, estimateMasterCpuCost(features, masterId));

		for (int slaveId = 0; slaveId < PartitionMetaMgr.NUM_PARTITIONS; slaveId++)
			builder.setSlaveCpuCost(slaveId, estimateSlaveCpuCost(features, slaveId));

		return builder.build();
	}

	@Override
	public void notifyTransactionRoute(long txNum, int masterId) {
		// Do nothing
	}

	private double[] estimateLatency(TransactionFeatures features) {
		String response = callPythonEstimator(features.toString());
		return parseDoubleArray(response);
	}

	private String callPythonEstimator(String featureString) {
		try {
			logger.fine("feature string to python: " + featureString);
			// Send the request
			output.append(featureString);
			output.newLine();
			output.flush();

			// Read the response
			String response;
			String errorResponse;
			while (true) {
				response = input.readLine();

				if (response != null) {
					logger.fine("python cost-estimator response: " + response);

					/*
					 * What we want is an array filled with double values. Ignore those irrelevant
					 * responses.
					 */
					if (response.charAt(0) == '[' && response.charAt(response.length() - 1) == ']') {
						break;
					}
				} else {
					/* There might be an error if response == null */
					errorResponse = error.readLine();

					if (errorResponse != null) {
						logger.info("python errors: " + errorResponse);
					}
				}
			}

			return response;
		} catch (IOException e) {
			throw new RuntimeException(
					String.format("%s got an error: %s", this.getClass().getSimpleName(), e.getMessage()));
		}
	}

	private long estimateMasterCpuCost(TransactionFeatures features, int masterId) {
		return 190;
	}

	private long estimateSlaveCpuCost(TransactionFeatures features, int slaveId) {
		return 90;
	}
}

package org.elasql.perf.tpart.ai;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class PythonSubProcessEstimator implements Estimator {
	
	// XXX: for quick testing
	private static final String PYTHON_RUNTIME = "python";
	private static final String SCRIPT_PATH = "~/cost-estimator/serve.py";
	
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
	
	public PythonSubProcessEstimator() {
		List<String> cmd = new ArrayList<String>();
		cmd.add(PYTHON_RUNTIME);
		cmd.add(SCRIPT_PATH);
		
		try {
			ProcessBuilder builder = new ProcessBuilder();
			builder.command(cmd);
			process = builder.start();
			
			output = new BufferedWriter(new OutputStreamWriter(
					process.getOutputStream(), "UTF-8"));
			input = new BufferedReader(new InputStreamReader(
					process.getInputStream(), "UTF-8"));
		} catch (IOException e) {
			throw new RuntimeException(String.format("%s got an error: %s",
					this.getClass().getSimpleName(), e.getMessage()));
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
	
	private double[] estimateLatency(TransactionFeatures features) {
		String response = callPythonEstimator(features.toString());
		return parseDoubleArray(response);
	}
	
	private String callPythonEstimator(String featureString) {
		try {
			// Send the request
			output.append(featureString);
			output.newLine();
			output.flush();
			
			// Read the response
			String response = input.readLine();
			return response;
		} catch (IOException e) {
			throw new RuntimeException(String.format("%s got an error: %s",
					this.getClass().getSimpleName(), e.getMessage()));
		}
	}
	
	private long estimateMasterCpuCost(TransactionFeatures features, int masterId) {
		return 190;
	}
	
	private long estimateSlaveCpuCost(TransactionFeatures features, int slaveId) {
		return 90;
	}
}

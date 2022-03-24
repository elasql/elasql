package org.elasql.perf.tpart.workload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An object to store the features for a transaction request.
 * 
 * @author Yu-Xuan Lin, Yu-Shan Lin, Ping-Yu Wang
 */
public class TransactionFeatures {

	// Defines a read-only list for feature keys
	public static final List<String> FEATURE_KEYS;

	static {
		List<String> featureKeys = new ArrayList<String>();

		// Transaction Features:
		// (Modify this part to add/remove features)
		// - Transaction start time (the time entering the system)
		featureKeys.add("Start Time");
		
		// - Transaction Type Related
		featureKeys.add("Tx Type");
		
		// Transaction Dependency
		featureKeys.add("Dependency - Max Depth");
		featureKeys.add("Dependency - First Layer Tx Count");
		featureKeys.add("Dependency - Total Tx Count");
		
		// - Number of records
//		featureKeys.add("Number of Read Records");
//		featureKeys.add("Number of Update Records");
		featureKeys.add("Number of Insert Records");
//		featureKeys.add("Number of Fully Replicated Records");
//		// - Data distribution
		featureKeys.add("Read Data Distribution");
//		featureKeys.add("Read Data Distribution in Bytes");
		featureKeys.add("Read Data in Cache Distribution");
		featureKeys.add("Read Data with IO Distribution");
		featureKeys.add("Update Data Distribution");
//
//		// Fusion Table
		featureKeys.add("Number of Overflows in Fusion Table");

//		featureKeys.add("Buffer Hit Rate");
//		featureKeys.add("Avg Pin Count");
//		featureKeys.add("Pinned Buffer Count");
//
//		featureKeys.add("Buffer RL Wait Count");
//		featureKeys.add("Buffer WL Wait Count");
//		featureKeys.add("Block Lock Release Count");
//		featureKeys.add("Block Lock Wait Count");
//		featureKeys.add("File Header Page Release Count");
//		featureKeys.add("File Header Page Wait Count");
//		featureKeys.add("Page GetVal Wait Count");
//		featureKeys.add("Page SetVal Wait Count");
//		featureKeys.add("Page GetVal Release Count");
//		featureKeys.add("Page SetVal Release Count");
//
		featureKeys.add("System CPU Load");
		featureKeys.add("Process CPU Load");
		featureKeys.add("System Load Average");
		featureKeys.add("Thread Active Count");
//
		featureKeys.add("I/O Read Bytes");
		featureKeys.add("I/O Write Bytes");
		featureKeys.add("I/O Queue Length");
		
		featureKeys.add("Number of Read Record in Last 100 us");
		featureKeys.add("Number of Read Record Excluding Cache in Last 100 us");
		featureKeys.add("Number of Update Record in Last 100 us");
		featureKeys.add("Number of Insert Record in Last 100 us");
		featureKeys.add("Number of Commit Tx in Last 100 us");
		
		featureKeys.add("Number of Read Record in Last 500 us");
		featureKeys.add("Number of Read Record Excluding Cache in Last 500 us");
		featureKeys.add("Number of Update Record in Last 500 us");
		featureKeys.add("Number of Insert Record in Last 500 us");
		featureKeys.add("Number of Commit Tx in Last 500 us");
		
		featureKeys.add("Number of Read Record in Last 1000 us");
		featureKeys.add("Number of Read Record Excluding Cache in Last 1000 us");
		featureKeys.add("Number of Update Record in Last 1000 us");
		featureKeys.add("Number of Insert Record in Last 1000 us");
		featureKeys.add("Number of Commit Tx in Last 1000 us");
//
//		featureKeys.add("Latch Features");

//		featureKeys.add("Latch Features");

		// Convert the list to a read-only list
		FEATURE_KEYS = Collections.unmodifiableList(featureKeys);
	}

	// Builder Pattern
	// - avoids passing Map and List from outside
	// - creates immutable TransactionFeatures objects
	// - checks the correctness before building an object
	public static class Builder {
		private long txNum;
		private int lastTxRoutingDest;
		private Map<String, Object> features;
		private List<Long> dependentTxns;

		public Builder(long txNum, int lastTxRoutingDest) {
			this.txNum = txNum;
			this.lastTxRoutingDest = lastTxRoutingDest;
			this.features = new HashMap<String, Object>();
			this.dependentTxns = new ArrayList<Long>();
		}

		public void addFeature(String key, Object value) {
			if (!FEATURE_KEYS.contains(key))
				throw new RuntimeException("Unexpected feature: " + key);

			features.put(key, value);
		}

		public void addDependency(Long dependentTxNum) {
			if (dependentTxNum >= txNum)
				throw new RuntimeException(String.format("Tx.%d should not depend to tx.%d", txNum, dependentTxNum));

			dependentTxns.add(dependentTxNum);
		}

		public TransactionFeatures build() {
			// Check the integrity of the features
			for (String key : FEATURE_KEYS)
				if (!features.containsKey(key))
					throw new RuntimeException(String.format("Feature '%s' is missing for tx.%d", key, txNum));

			// Sort the dependencies
			Collections.sort(dependentTxns);

			return new TransactionFeatures(txNum, features, dependentTxns, lastTxRoutingDest);
		}
	}

	private long txNum;
	private int lastTxRoutingDest;
	private Map<String, Object> features;
	// Transaction dependencies are handled separately
	private List<Long> dependentTxns;

	// Builder Pattern: set the constructor to private to avoid creating an object
	// from outside
	private TransactionFeatures(long txNum, Map<String, Object> features, List<Long> dependentTxns, int lastTxRoutingDest) {
		this.txNum = txNum;
		this.lastTxRoutingDest = lastTxRoutingDest;
		this.features = features;
		this.dependentTxns = dependentTxns;
	}

	public long getTxNum() {
		return txNum;
	}

	public Object getFeature(String key) {
		return features.get(key);
	}

	public List<Long> getDependencies() {
		// Use 'unmodifiableList' to avoid the list is modified outside
		return Collections.unmodifiableList(dependentTxns);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		// The starting character
		sb.append('[');
		
		// Transaction ID
		sb.append(txNum);
		sb.append(',');
		
		// Transaction Dependencies
		sb.append(dependentTxns);
		sb.append(',');
		
		// Last tx's routing destination
		sb.append(lastTxRoutingDest);
		sb.append(',');
		
		// Non-array features
		for (String key : FEATURE_KEYS) {
			Object val = features.get(key);
			if (!val.getClass().isArray()) {
				sb.append(val);
				sb.append(',');
			}
		}
		
		// Array features
		for (String key : FEATURE_KEYS) {
			Object val = features.get(key);
			if (val.getClass().isArray()) {
				sb.append(Arrays.toString((Object[]) val));
				sb.append(',');
			}
		}
		
		// The ending character
		sb.setCharAt(sb.length() - 1, ']');
		
		return sb.toString();
	}
}

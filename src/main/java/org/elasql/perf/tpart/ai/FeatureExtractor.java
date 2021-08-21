package org.elasql.perf.tpart.ai;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;

/**
 * A processor to extract features from a transaction request. The transaction
 * must be given in the total order. 
 * 
 * @author Yu-Shan Lin, Yu-Xuan Lin
 */
public class FeatureExtractor {
	
	private long lastProcessedTxNum = -1;
	
	public TransactionFeatures extractFeatures(TPartStoredProcedureTask task) {
		// Check if transaction requests are given in the total order
		if (task.getTxNum() <= lastProcessedTxNum)
			throw new RuntimeException(String.format(
					"Transaction requests are not passed to FeatureExtractor "
					+ "in the total order: %d, last processed tx: %d",
					task.getTxNum(), lastProcessedTxNum));
			
		// Extract the features
		TransactionFeatures.Builder builder = new TransactionFeatures.Builder(task.getTxNum());
		
		// All features in TransactionFeatures.FEATURE_KEYS must be set
		
		// TODO: Get features
		
		// TODO: Get dependencies
		
		return builder.build();
	}
}

package org.elasql.perf.tpart.bandit.data;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.storage.metadata.PartitionMetaMgr;

import java.io.Serializable;
import java.util.Arrays;

public class BanditTransactionContext implements Serializable {
	public static final int NUMBER_OF_CONTEXT = PartitionMetaMgr.NUM_PARTITIONS;
	private static final long serialVersionUID = 1;
	private final long txNum;
	private final ArrayRealVector context;

	public BanditTransactionContext(long txNum, TransactionFeatures transactionFeatures) {
		double[] readDataDistributions = Arrays.stream((Integer[]) transactionFeatures.getFeature("Remote Reads")).mapToDouble((v) -> v).toArray();
		double[] writeDataDistributions = Arrays.stream((Integer[]) transactionFeatures.getFeature("Remote Writes")).mapToDouble((v) -> v).toArray();
		normalize(readDataDistributions);
		normalize(writeDataDistributions);
		for (int i = 0; i < readDataDistributions.length; i++) {
			readDataDistributions[i] = readDataDistributions[i] * 0.5 + writeDataDistributions[i] * 0.5;
		}
		this.context = new ArrayRealVector(readDataDistributions, false);
		this.txNum = txNum;
	}

	public BanditTransactionContext(long txNum, RealVector context) {
		this.context = new ArrayRealVector(context);
		this.txNum = txNum;
	}

	public ArrayRealVector getContext() {
		return context;
	}

	public long getTransactionNumber() {
		return txNum;
	}

	private void normalize(double[] array) {
		double sum = Arrays.stream(array).sum();
		for (int i = 0; i < array.length; i++) {
			array[i] /= sum;
		}
	}

//	public byte[] toBytes() {
//		try {
//			ByteArrayOutputStream bo = new ByteArrayOutputStream();
//			ObjectOutputStream out = new ObjectOutputStream(bo);
//
//			out.writeLong(txNum);
//			out.writeInt(context.size());
//
//			for (Double feature : context) {
//				out.writeDouble(feature);
//			}
//
//			out.flush();
//
//			return bo.toByteArray();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
//
//	public static BanditTransactionContext fromBytes(byte[] bytes) throws IOException {
//		ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
//		ObjectInputStream in = new ObjectInputStream(bi);
//
//		long txNum = in.readLong();
//		int size = in.readInt();
//
//		ArrayList<Double> features = new ArrayList<>(size);
//		for (int i = 0; i < size; i++) {
//			features.set(i, in.readDouble());
//		}
//
//		return new BanditTransactionContext(txNum, features);
//	}
}

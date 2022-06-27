package org.elasql.perf.tpart.bandit.data;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.elasql.perf.tpart.workload.TransactionFeatures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class BanditTransactionContext implements Serializable {
	private static final long serialVersionUID = 1;

	private final long txNum;
	private final ArrayRealVector context;
	public BanditTransactionContext(long txNum, TransactionFeatures transactionFeatures) {
		Double[] readDataDistributions = Arrays.stream((Integer[]) transactionFeatures.getFeature("Read Data Distribution")).mapToDouble(Double::new).boxed().toArray(Double[]::new);
		Double[] writeDataDistributions = Arrays.stream((Integer[]) transactionFeatures.getFeature("Write Data Distribution")).mapToDouble(Double::new).boxed().toArray(Double[]::new);
		Double[] systemCpuLoads = (Double[]) transactionFeatures.getFeature("System CPU Load");
		ArrayList<Double> context = new ArrayList<>();
		Collections.addAll(context, readDataDistributions);
		Collections.addAll(context, writeDataDistributions);
		Collections.addAll(context, systemCpuLoads);
		this.context = new ArrayRealVector(context.stream().mapToDouble(v -> v).toArray(), false);
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

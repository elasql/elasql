package org.elasql.perf.tpart.mdp.bandit.data;

import java.io.Serializable;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class BanditTransactionContext implements Serializable {
	public static final int NUMBER_OF_CONTEXT = PartitionMetaMgr.NUM_PARTITIONS * 2;
	private static final long serialVersionUID = 1;
	private final long txNum;
	private final ArrayRealVector context;

	public BanditTransactionContext(long txNum, ArrayRealVector context) {
		this.context = context;
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

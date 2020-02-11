package org.elasql.remote.groupcomm;

import java.io.Serializable;

import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.metadata.index.IndexInfo;

public class Bytes implements Serializable, Comparable<Bytes>{
	/**
	 * 
	 */
	private static final long serialVersionUID = -606284893049245719L;
	public String serial_nm;
	public int part;
	public int totalPart;
	public byte[] rec;
	public int byte_lenth;
	public IndexInfo ii;
	public SearchKeyType sktype;
	

	public Bytes(String serial_nm, int part, int totalPart, byte[] rec,
			int byte_lenth, IndexInfo ii, SearchKeyType sktype) {
		this.serial_nm = serial_nm;
		this.rec = rec;
		this.part = part;
		this.totalPart = totalPart;
		this.byte_lenth = byte_lenth;
		this.ii = ii;
		this.sktype = sktype;
	}

	@Override
	public int compareTo(Bytes o) {
		int i = serial_nm.compareTo(o.serial_nm);
		if(i == 0) {
			if(part > o.part)
				return 1;
			else if(part == o.part)
				return 0;
			else
				return -1;
		} else {
			return i;
		}
	}

	/*
	 * @Override
	 * public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("[Tuple: ");
		sb.append(key);
		sb.append(" sent form tx.");
		sb.append(srcTxNum);
		sb.append(" to tx.");
		sb.append(destTxNum);
		sb.append("]");

		return sb.toString();
	}
	 */
	
}

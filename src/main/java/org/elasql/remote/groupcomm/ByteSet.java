package org.elasql.remote.groupcomm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.metadata.index.IndexInfo;


public class ByteSet implements Serializable {
	private List<Bytes> DataSet;

	public ByteSet() {
		this.DataSet = new ArrayList<Bytes>();
	}

	public List<Bytes> getByteSet() {
		return DataSet;
	}

	public void addByte(String serial_nm, int part, int totalPart, byte[] rec,
			int byte_lenth, IndexInfo ii, SearchKeyType sktype) {
		DataSet.add(new Bytes(serial_nm, part, totalPart, rec, byte_lenth, ii, sktype));
	}
	
	public void addByte(Bytes b) {
		DataSet.add(b);
	}
	
	public void sort()
    {
		Collections.sort(DataSet);
    }
	
	public int size() {
		return DataSet.size();
	}
	public void clear() {
		DataSet.clear();
	}
	
}

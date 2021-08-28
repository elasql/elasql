package org.elasql.perf.tpart.metric;

import java.util.ArrayList;
import java.util.List;

import org.elasql.util.CsvRow;
import org.vanilladb.core.server.task.Task;

public abstract class TransactionMetricRow extends Task {
	
	// Data
	protected List<LatencyRow> latencyRows = new ArrayList<LatencyRow>();
	protected List<CpuTimeRow> cpuTimeRows = new ArrayList<CpuTimeRow>();	
	protected List<DiskioCountRow> diskioCountRows = new ArrayList<DiskioCountRow>();
	
	protected static class LatencyRow implements CsvRow, Comparable<LatencyRow> {
		long txNum;
		boolean isMaster;
		long[] values;
		
		LatencyRow(long txNum, boolean isMaster, long[] values) {
			this.txNum = txNum;
			this.isMaster = isMaster;
			this.values = values;
		}

		@Override
		public String getVal(int index) {
			if (index == 0) {
				return Long.toString(txNum);
			} else if (index == 1) {
				return Boolean.toString(isMaster);
			} else {
				if (index - 2 < values.length) {
					return Long.toString(values[index - 2]);
				} else {
					return "";
				}
			}
		}

		@Override
		public int compareTo(LatencyRow row) {
			return Long.compare(txNum, row.txNum);
		}
	}
	
	protected static class CpuTimeRow implements CsvRow, Comparable<CpuTimeRow> {
		long txNum;
		boolean isMaster;
		long[] values;
		
		CpuTimeRow(long txNum, boolean isMaster, long[] values) {
			this.txNum = txNum;
			this.isMaster = isMaster;
			this.values = values;
		}

		@Override
		public String getVal(int index) {
			if (index == 0) {
				return Long.toString(txNum);
			} else if (index == 1) {
				return Boolean.toString(isMaster);
			} else {
				if (index - 2 < values.length) {
					return Long.toString(values[index - 2]);
				} else {
					return "";
				}
			}
		}

		@Override
		public int compareTo(CpuTimeRow row) {
			return Long.compare(txNum, row.txNum);
		}
	}
	
	protected static class DiskioCountRow implements CsvRow, Comparable<DiskioCountRow> {
		long txNum;
		boolean isMaster;
		long[] values;
		
		DiskioCountRow(long txNum, boolean isMaster, long[] values) {
			this.txNum = txNum;
			this.isMaster = isMaster;
			this.values = values;
		}

		@Override
		public String getVal(int index) {
			if (index == 0) {
				return Long.toString(txNum);
			} else if (index == 1) {
				return Boolean.toString(isMaster);
			} else {
				if (index - 2 < values.length) {
					return Long.toString(values[index - 2]);
				} else {
					return "";
				}
			}
		}

		@Override
		public int compareTo(DiskioCountRow row) {
			return Long.compare(txNum, row.txNum);
		}
	}
}

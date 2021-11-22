package org.elasql.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class CsvSaver<R extends CsvRow> {
	
	// Set 'true' to use the same filename for the report.
	// This is used to avoid create too many files in a series of experiments.
	private static final boolean USE_SAME_FILENAME = true;
	
	private String filenamePrefix;
	
	public CsvSaver(String filenamePrefix) {
		this.filenamePrefix = filenamePrefix;
	}
	
	public String generateOutputFile(List<String> header, List<R> rows) {
		int columnCount = header.size();
		String fileName = generateOutputFileName();
		try (BufferedWriter writer = createOutputFile(fileName)) {
			writeHeader(writer, header);
			for (R row : rows)
				writeRecord(writer, row, columnCount);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fileName;
	}
	
	private String generateOutputFileName() {
		String filename;
		
		if (USE_SAME_FILENAME) {
			filename = String.format("%s.csv", filenamePrefix);
		} else {
			LocalDateTime datetime = LocalDateTime.now();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
			String datetimeStr = datetime.format(formatter);
			filename = String.format("%s-%s.csv", filenamePrefix, datetimeStr);
		}
		
		return filename;
	}
	
	private BufferedWriter createOutputFile(String fileName) throws IOException {
		return new BufferedWriter(new FileWriter(fileName));
	}
	
	private void writeHeader(BufferedWriter writer, List<String> header) throws IOException {
		StringBuilder sb = new StringBuilder();
		
		for (String column : header) {
			sb.append(column);
			sb.append(',');
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append('\n');
		
		writer.append(sb.toString());
	}
	
	private void writeRecord(BufferedWriter writer, R row, int columnCount) throws IOException {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 0; i < columnCount; i++) {
			sb.append(row.getVal(i));
			sb.append(',');
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append('\n');
		
		writer.append(sb.toString());
	}
}

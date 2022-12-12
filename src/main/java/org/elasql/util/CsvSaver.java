package org.elasql.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class CsvSaver<R extends CsvRow> {
	
	private String filenamePrefix;

	// Set 'true' to use the same filename for the report.
	// This is used to avoid create too many files in a series of experiments.
	private boolean useSameFileName;
	
	public CsvSaver(String filenamePrefix) {
		this(filenamePrefix, true);
	}

	public CsvSaver(String filenamePrefix, boolean useSameFileName) {
		this.filenamePrefix = filenamePrefix;
		this.useSameFileName = useSameFileName;
	}

	private String generateOutputFileName() {
		String filename;

		if (useSameFileName) {
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

	public String fileName() {
		return generateOutputFileName();
	}

	public BufferedWriter createOutputFile() throws IOException {
		String fileName = generateOutputFileName();
		return createOutputFile(fileName);
	}

	public void writeHeader(BufferedWriter writer, List<String> header) throws IOException {
		StringBuilder sb = new StringBuilder();

		for (String column : header) {
			sb.append(column);
			sb.append(',');
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append('\n');
		
		writer.append(sb.toString());
	}

	public void writeRecord(BufferedWriter writer, R row, int columnCount) throws IOException {
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

package org.elasql.perf.tpart.rl.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class MemoryCsvLoader {
	public static final String FIELD_NAME_ID = "txId";
	public static final String FIELD_NAME_STATE = "state";
	public static final String FIELD_NAME_NEXT_STATE = "nextState";
	public static final String FIELD_NAME_ACTION = "action";
	public static final String FIELD_NAME_REWARD = "reward";

	private static final String FILENAME_PREFIX = "memory";
	private static File file = new File(String.format("%s.csv", FILENAME_PREFIX));

	public static Memory loadCsvAsDataFrame() {
		Path path = file.toPath();

		Memory memory;
		List<CSVRecord> csvRecords = null;
		try (CSVParser csv = newCsvParser(path)) {
			csvRecords = csv.getRecords();
		} catch (IOException e) {
			e.printStackTrace();
		}

		memory = new Memory(csvRecords.size() - 1);
		csvRecords.remove(0);
		for (int rowId = 0; rowId < csvRecords.size() - 1; rowId++) {
			loadCsvRecord(csvRecords.get(rowId), memory);
		}
		return memory;
	}
	
	private static void loadCsvRecord(CSVRecord record, Memory memory) {
		long txNum = 0;
		float[] state = null;
		int action = 0;
		float reward = 0.0f;
		
		for (int colId = 0; colId < record.size(); colId++) {
			String valStr = record.get(colId).trim();
			
			switch (colId) {
			case 0:
				txNum = Long.parseLong(valStr);
				break;
			case 1:
				state = parseFloatArray(valStr);
				break;
			case 2:
				action = Integer.parseInt(valStr);
				break;
			case 3:
				reward = Float.parseFloat(valStr);
				break;
			}
		}
		memory.setStep(txNum, state, action, reward, false);
	}

	private static float[] parseFloatArray(String s) {
		// strip surrounding []
		if (s.length() == 2)
			return null;
		String[] elements = s.substring(1, s.length() - 1).split(",");
		float[] array = new float[elements.length];
		for (int ei = 0; ei < elements.length; ei++) {
			array[ei] = Float.parseFloat(elements[ei]);
		}
		return array;
	}

	private static CSVParser newCsvParser(Path path) throws IOException {
		return CSVParser.parse(path, StandardCharsets.UTF_8, CSVFormat.DEFAULT.withIgnoreSurroundingSpaces());
	}
}

package org.elasql.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.elasql.perf.tpart.rl.util.Memory;
import org.elasql.perf.tpart.rl.util.Transition;

import smile.data.type.DataType;
import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;

public class CsvLoader {
	public static final String FIELD_NAME_ID = "txId";
	public static final String FIELD_NAME_STATE = "state";
	public static final String FIELD_NAME_NEXT_STATE = "nextState";
	public static final String FIELD_NAME_ACTION = "action";
	public static final String FIELD_NAME_REWARD = "reward";

	private static final String FILENAME_PREFIX = "memory";
	private static File file = new File(String.format("%s.csv", FILENAME_PREFIX));

	private static List<String> header = null;

	public static Memory loadCsvAsDataFrame() {
		Path path = file.toPath();
		StructType schema = inferSchema(path);
		List<Function<String, Object>> valParsers = schema.parser();

		
		Memory memory;
		List<CSVRecord> csvRecords = null;
		try (CSVParser csv = newCsvParser(path)) {
			csvRecords = csv.getRecords();
		} catch (IOException e) {
			e.printStackTrace();
		}

		memory = new Memory(csvRecords.size() - 1);
		Transition transition = null;
		csvRecords.remove(0);
		for (CSVRecord record : csvRecords) {
			transition = parseCsvRecord(record, schema, valParsers);
			memory.add(transition);
		}
		return memory;
	}

	private static Transition parseCsvRecord(CSVRecord record, StructType schema,
			List<Function<String, Object>> valParsers) {
		StructField[] fields = schema.fields();
		int txId = 0;
		float[] state = null;
		float[] state_next = null;
		int action = 0;
		float reward = 0.0f;

		for (int i = 0; i < fields.length; i++) {
			String valStr = null;

			// Get the value
			if (i < record.size()) {
				valStr = record.get(i).trim();
			}

			if (i == 0) {
				txId = ((Float) valParsers.get(i).apply(valStr)).intValue();
			} else if (i == 1) {
				state = parseFloatArray(valStr);
			} else if (i == 2) {
				state_next = parseFloatArray(valStr);
			} else if (i == 3) {
				action = ((Float) valParsers.get(i).apply(valStr)).intValue();
			} else if (i == 4) {
				reward = (float) valParsers.get(i).apply(valStr);
			}
		}
		return new Transition(state, state_next, action, reward, false);
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

	private static StructType inferSchema(Path path) {
		// Find the header and the first row of values
		try {
			header = newCsvParser(path).getRecords().get(0).toList();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Infer the schema
		DataType[] types = new DataType[header.size()];
		for (int i = 0; i < header.size(); i++) {
			String fieldName = header.get(i);
			if (fieldName.equals(FIELD_NAME_ID))
				types[i] = DataTypes.FloatType;
			else if (fieldName.equals(FIELD_NAME_STATE))
				types[i] = DataTypes.FloatArrayType;
			else if (fieldName.equals(FIELD_NAME_NEXT_STATE))
				types[i] = DataTypes.FloatArrayType;
			else if (fieldName.equals(FIELD_NAME_ACTION))
				types[i] = DataTypes.FloatType;
			else
				types[i] = DataTypes.FloatType;
		}

		// Build the schema
		StructField[] fields = new StructField[header.size()];
		for (int i = 0; i < fields.length; i++)
			fields[i] = new StructField(header.get(i), types[i]);
		return DataTypes.struct(fields);
	}
}

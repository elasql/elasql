package org.elasql.perf.tpart.mdp.rl.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.elasql.util.CsvRow;
import org.elasql.util.CsvSaver;

import ai.djl.ndarray.NDManager;

public final class Memory {

	private static final String FILENAME_PREFIX = "memory";
	
	private class MemoryCsvRow implements CsvRow {
		int rowId;
		
		MemoryCsvRow(int rowId) {
			this.rowId = rowId;
		}
		
		public String getVal(int index) {
			switch (index) {
			case 0:
				return Long.toString(txNums[rowId]);
			case 1:
				return String.format("\"%s\"", Arrays.toString(states[rowId]));
			case 2:
				return Integer.toString(actions[rowId]);
			case 3:
				return Float.toString(rewards[rowId]);
			default:
				return "";
			}
		}
	}
	
	private final Random random;
	private final int capacity;
	
	private long[] txNums;
	private float[][] states;
	private int[] actions;
	private float[] rewards;
	private boolean[] masks;
	private int head;
	private int size;

	public Memory(int capacity) {
		this(capacity, 0);
	}

	public Memory(int capacity, int seed) {
		this.capacity = capacity;
		this.random = new Random(seed);
		this.txNums = new long[capacity];
		this.states = new float[capacity][];
		this.actions = new int[capacity];
		this.rewards = new float[capacity];
		this.masks = new boolean[capacity];
		this.size = capacity;
		System.out.print("size = ");
		System.out.println(size);
	}
	
	public void setStep(long txNum, float[] state, int action, float reward,
			boolean mask) {
		int index = (int) ((txNum - 1) % capacity);
		txNums[index] = txNum;
		states[index] = state;
		actions[index] = action;
		rewards[index] = reward;
		masks[index] = mask;
	}

	public Transition[] sample(int sample_size) {
		Transition[] chunk = new Transition[sample_size];
		int i = 0;
		while (i < sample_size) {
			Transition transition = get(random.nextInt(size));
			if (transition != null) {
				chunk[i] = transition;
				i++;
			}
		}

		return chunk;
	}

	public MemoryBatch sampleBatch(int sample_size, NDManager manager) {
		return getBatch(sample(sample_size), manager, sample_size);
	}

	// Returns null if the transition on the given index is unavailable
	public Transition get(int index) {
		if (index < 0 || index >= size) {
			throw new ArrayIndexOutOfBoundsException("Index out of bound " + index);
		}
		
		// Check if the state is available
		if (states[index] == null)
			return null;
		
		// Check if the next state is available by checking the tx number
		int nextIndex = (index + 1) % capacity;
		if (txNums[index] + 1 != txNums[nextIndex])
			return null;
		
		return new Transition(states[index], states[nextIndex], actions[index], rewards[index], masks[index]);
	}

	public int size() {
		return size;
	}

	public void reset() {
		this.txNums = new long[capacity];
		this.states = new float[capacity][];
		this.actions = new int[capacity];
		this.rewards = new float[capacity];
		this.masks = new boolean[capacity];
		head = -1;
		size = 0;
	}

	@Override
	public String toString() {
		// TODO
		return "Memory";
	}

	private MemoryBatch getBatch(Transition[] transitions, NDManager manager, int batch_size) {
		float[][] states = new float[batch_size][];
		float[][] next_states = new float[batch_size][];
		int[] actions = new int[batch_size];
		float[] rewards = new float[batch_size];
		boolean[] masks = new boolean[batch_size];

		int index = head;
		for (int i = 0; i < batch_size; i++) {
			index++;
			if (index >= batch_size) {
				index = 0;
			}
			states[i] = transitions[index].getState();
			float[] next_state = transitions[index].getNextState();
			next_states[i] = next_state != null ? next_state : new float[states[i].length];
			actions[i] = transitions[index].getAction();
			rewards[i] = transitions[index].getReward();
			masks[i] = transitions[index].isMasked();
		}

		return new MemoryBatch(manager.create(states), manager.create(next_states), manager.create(actions),
				manager.create(rewards), manager.create(masks));
	}

	public void saveFile() {
		List<String> header = initHeader();
		int columnCount = header.size();

		// Save to CSV
		CsvSaver<MemoryCsvRow> csvSaver = new CsvSaver<MemoryCsvRow>(FILENAME_PREFIX);

		try (BufferedWriter writer = csvSaver.createOutputFile()) {
			csvSaver.writeHeader(writer, header);

			// write the rows
			for (int rowId = 0; rowId < capacity; rowId++) {
				try {
					csvSaver.writeRecord(writer, new MemoryCsvRow(rowId), columnCount);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private List<String> initHeader() {
		List<String> header = new ArrayList<String>();
		
		header.add("txId");
		header.add("state");
		header.add("action");
		header.add("reward");
		
		return header;
	}
}

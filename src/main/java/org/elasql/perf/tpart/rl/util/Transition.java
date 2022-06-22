package org.elasql.perf.tpart.rl.util;

import java.util.Arrays;
import java.util.Map;

import org.elasql.util.CsvRow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class Transition extends Snapshot implements CsvRow {

	private final float[] state_next;
	private final int action;

	public Transition(float[] state, float[] state_next, int action, float reward, boolean mask) {
		super(state, reward, mask);
		this.state_next = state_next != null ? state_next.clone() : null;
		this.action = action;
	}

	public final float[] getNextState() {
		return state_next;
	}

	public final int getAction() {
		return action;
	}

	@Override
	public String toString() {
//		try {
//			return new ObjectMapper().writeValueAsString(Map.of("state", getState(), "state_next", state_next==null?"Not exist":state_next, "action",
//					action, "reward", getReward(), "mask", isMasked()));
//		} catch (JsonProcessingException e) {
//			throw new RuntimeException(e);
//		}
		return null;
	}

	@Override
	public String getVal(int index) {
		
		if (index == 0) {
			if (getState() == null)
				return "";
			else
				return quoteString(Arrays.toString(getState()));
		} else if (index == 1) {
			if (getNextState() == null) {
				return "[]";
			}
			else {
				return quoteString(Arrays.toString(getNextState()));
			}
		} else if (index == 2) {
			return Integer.toString(getAction());
		} else if (index == 3) {
			return Float.toString(getReward());
		} else {
			throw new RuntimeException();
		}
	}
	
	private String quoteString(String str) {
		return "\"" + str + "\"";
	}
}

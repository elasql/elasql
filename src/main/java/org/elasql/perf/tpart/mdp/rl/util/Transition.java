package org.elasql.perf.tpart.mdp.rl.util;

public final class Transition extends Snapshot {

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
		// TODO
//		try {
//			return new ObjectMapper().writeValueAsString(Map.of("state", getState(), "state_next", state_next==null?"Not exist":state_next, "action",
//					action, "reward", getReward(), "mask", isMasked()));
//		} catch (JsonProcessingException e) {
//			throw new RuntimeException(e);
//		}
		return "Transition";
	}
}

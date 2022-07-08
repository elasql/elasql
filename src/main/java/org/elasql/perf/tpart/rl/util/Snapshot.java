package org.elasql.perf.tpart.rl.util;

public class Snapshot {
    private final float[] state;
    private final float reward;
    private final boolean mask;

    public Snapshot(float[] state, float reward, boolean mask) {
        this.state = state.clone();
        this.reward = reward;
        this.mask = mask;
    }

    public final float[] getState() {
        return state;
    }

    public final float getReward() {
        return reward;
    }

    public final boolean isMasked() {
        return mask;
    }

    @Override
    public String toString() {
//        try {
//            return new ObjectMapper().writeValueAsString(Map.of("state", state, "reward", reward, "mask", mask));
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException(e);
//        }
    	return null;
    }
}

package org.elasql.perf.tpart.mdp;

public class State {
	
	private int[] localReadCounts;
	private int[] machineLoads;
	
	public State(int[] localReadCounts, int[] machineLoads) {
		this.localReadCounts = localReadCounts;
		this.machineLoads = machineLoads;
	}
	
	public int getLocalRead(int partId) {
		return localReadCounts[partId];
	}
	
	public int getMachineLoad(int partId) {
		return machineLoads[partId];
	}
	
	public float[] toFloatArray() {
		float[] state = new float[localReadCounts.length + machineLoads.length];
		
		for (int i = 0; i < localReadCounts.length; i++)
			state[i] = (float) localReadCounts[i];
		
		int offset = localReadCounts.length;
		for (int i = 0; i < machineLoads.length; i++)
			state[offset + i] = (float) machineLoads[i];
		
		return state;
	}
}

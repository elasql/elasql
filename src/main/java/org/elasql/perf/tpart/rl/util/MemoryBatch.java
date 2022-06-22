package org.elasql.perf.tpart.rl.util;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;

public final class MemoryBatch extends NDList {
	private static final long serialVersionUID = 1L;

    public MemoryBatch(NDArray... arrays) {
        super(arrays);
    }

    public NDArray getActions() {
        return get(2);
    }

    public NDArray getRewards() {
        return get(3);
    }

    public NDArray getMasks() {
        return get(4);
    }

    public NDArray getStates() {
        return get(0);
    }

    public NDArray getNextStates() {
        return get(1);
    }
}

package org.elasql.perf.tpart.mdp.rl.model;

import ai.djl.ndarray.NDManager;
import ai.djl.nn.AbstractBlock;

public abstract class BaseModel extends AbstractBlock {
    private static final byte VERSION = 2;
    private final NDManager manager;

    public BaseModel(NDManager manager) {
        super(VERSION);
        this.manager = manager;
    }

    public NDManager getManager() {
        return manager;
    }

}

package org.elasql.perf.tpart.rl.model;

import ai.djl.inference.Predictor;
import ai.djl.ndarray.NDList;

public abstract class TrainedAgent {

    public abstract void setPredictor(Predictor<NDList, NDList> target_predictor, Predictor<NDList, NDList> imitation_predictor);
	
    public abstract void setPredictor(Predictor<NDList, NDList> target_predictor);

    public abstract int react(float[] state);
    
}

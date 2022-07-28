package org.elasql.perf.tpart.rl.model;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import org.elasql.perf.tpart.rl.util.ActionSampler;

import ai.djl.inference.Predictor;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.translate.TranslateException;

public class TrainedBCQ extends TrainedAgent{

	protected final Random random = new Random(0);
	
	protected NDManager manager;
	protected Predictor<NDList, NDList> target_predictor;
	protected Predictor<NDList, NDList> imitation_predictor;

    private HashMap<Integer, Integer> stateActionMap = new HashMap<Integer, Integer>();

    public TrainedBCQ() {
    	manager = NDManager.newBaseManager();
    }
    
    public final void setPredictor(Predictor<NDList, NDList> target_predictor, Predictor<NDList, NDList> imitation_predictor) {
    	this.target_predictor = target_predictor;
        this.imitation_predictor = imitation_predictor;
        stateActionMap = new HashMap<Integer, Integer>();
    }

    public final int react(float[] state) {
    	// only evaluation
        Integer action = stateActionMap.get(Arrays.hashCode(state));
        if (action == null) {
	        try (NDManager submanager = manager.newSubManager()) {
	            action = getAction(submanager, state);
	            stateActionMap.put(Arrays.hashCode(state), action);
	        } catch (TranslateException e) {
	            throw new IllegalStateException(e);
	        }
        }
        return action;
    }
    
    private int getAction(NDManager manager, float[] state) throws TranslateException { 
    	NDArray imitation = imitation_predictor.predict(new NDList(manager.create(state))).singletonOrThrow();
    	imitation = imitation.softmax(-1);
    	NDArray score = target_predictor.predict(new NDList(manager.create(state))).singletonOrThrow();
    	int action = ActionSampler.greedy(score, imitation);
        return action;
    }

	@Override
	public void setPredictor(Predictor<NDList, NDList> target_predictor) {
		throw new UnsupportedOperationException();
	}
}

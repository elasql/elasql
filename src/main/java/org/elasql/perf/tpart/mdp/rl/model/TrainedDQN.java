package org.elasql.perf.tpart.mdp.rl.model;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import org.elasql.perf.tpart.mdp.rl.util.ActionSampler;
import org.vanilladb.core.util.TransactionProfiler;

import ai.djl.inference.Predictor;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.translate.TranslateException;

public class TrainedDQN extends TrainedAgent{

	protected final Random random = new Random(0);
	
	protected NDManager manager;
	protected Predictor<NDList, NDList> target_predictor;

    private HashMap<Integer, Integer> stateActionMap = new HashMap<Integer, Integer>();
    private float epsilon = 0.1f;
    
    public TrainedDQN() {
    	manager = NDManager.newBaseManager();
    }
    
    public final void setPredictor(Predictor<NDList, NDList> target_predictor, Predictor<NDList, NDList> imitation_predictor) {
    	this.target_predictor = target_predictor;
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
		TransactionProfiler.getLocalProfiler().startComponentProfiler("inference");
    	NDArray score = target_predictor.predict(new NDList(manager.create(state))).singletonOrThrow();
    	TransactionProfiler.getLocalProfiler().stopComponentProfiler("inference");
    	TransactionProfiler.getLocalProfiler().startComponentProfiler("get action");
    	// do some exploration
    	int action = ActionSampler.epsilonGreedy(score, random, epsilon);
    	TransactionProfiler.getLocalProfiler().stopComponentProfiler("get action");
        return action;
    }

}

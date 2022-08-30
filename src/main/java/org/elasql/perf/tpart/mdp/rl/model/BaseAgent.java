package org.elasql.perf.tpart.mdp.rl.model;

import ai.djl.inference.Predictor;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.translate.TranslateException;

public abstract class BaseAgent {
    private boolean is_eval = false;
    private boolean offline_training = false;

    /**
     * Calculate the action to the input state
     * 
     * @param state
     * @return action
     */
    public abstract int react(float[] state);

    /**
     * Collect the result of the previous action
     * 
     * @param reward
     * @param done
     */
    public abstract void collect(float reward, boolean done);

    public abstract void saveFile();
    
    public abstract Predictor<NDList, NDList> takeoutPredictor();
    /**
     * Reset the agent.
     */
    public abstract void reset();

    /**
     * Switch to the training mode.
     */
    public final void train() {
        this.is_eval = false;
    }

    /**
     * Switch to the inference mode.
     */
    public final void eval() {
        this.is_eval = true;
    }
    
    public final void offline() {
        this.offline_training = true;
    }

    /**
     * Check if the agent is in the inference mode.
     * 
     * @return true if the agent is in the inference mode.
     */
    public final boolean isEval() {
        return is_eval;
    }
    
    public final boolean isOffline() {
        return offline_training;
    }

	public abstract NDArray updateModel(NDManager submanager) throws TranslateException;
    
}

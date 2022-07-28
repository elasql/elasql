package org.elasql.perf.tpart.rl.model;

import org.elasql.perf.tpart.rl.util.ActionSampler;
import org.elasql.perf.tpart.rl.util.Helper;
import org.elasql.perf.tpart.rl.util.Memory;
import org.elasql.perf.tpart.rl.util.MemoryBatch;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.training.loss.L2Loss;
import ai.djl.translate.TranslateException;

public class DQN extends BaseDQN {
    private final L2Loss loss_func = new L2Loss();

    public DQN(int hidden_size, int batch_size, int sync_net_interval,
            float gamma, float learning_rate) {
        super(hidden_size, batch_size, sync_net_interval, gamma, learning_rate);
    }
    
    public DQN(int hidden_size, int batch_size, int sync_net_interval,
            float gamma, float learning_rate, Memory memory) {
        super(hidden_size, batch_size, sync_net_interval, gamma, learning_rate, memory);
    }

    @Override
    protected int getAction(NDManager manager, float[] state) throws TranslateException {
       // just for action selecting before training
        return ActionSampler.random(random);
    }

    @Override
	public NDArray updateModel(NDManager manager) throws TranslateException {
        MemoryBatch batch = memory.sampleBatch(batch_size, manager);

        NDArray policy = policy_predictor.predict(new NDList(batch.getStates())).singletonOrThrow();
        NDArray target = target_predictor.predict(new NDList(batch.getNextStates())).singletonOrThrow().duplicate();
        NDArray expected_returns = Helper.gather(policy, batch.getActions().toIntArray());
        // Reward from the policy
        NDArray next_returns = batch.getRewards()
                .add(target.max(new int[] { 1 }).mul(batch.getMasks().logicalNot()).mul(gamma));

        NDArray loss = loss_func.evaluate(new NDList(expected_returns), new NDList(next_returns));
        gradientUpdate(loss);     
        return loss;
    }
}
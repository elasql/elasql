package org.elasql.perf.tpart.mdp.rl.model;

import org.elasql.perf.tpart.mdp.rl.util.ActionSampler;
import org.elasql.perf.tpart.mdp.rl.util.Helper;
import org.elasql.perf.tpart.mdp.rl.util.Memory;
import org.elasql.perf.tpart.mdp.rl.util.MemoryBatch;
import org.elasql.server.Elasql;

import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.training.loss.L2Loss;
import ai.djl.translate.NoopTranslator;
import ai.djl.translate.TranslateException;

public class OfflineBCQ extends OfflineBaseBCQ {
    private final L2Loss loss_func = new L2Loss();

    public OfflineBCQ(int hidden_size, int batch_size, int sync_net_interval,
            float gamma, float learning_rate, Memory memory) {
        super(hidden_size, batch_size, sync_net_interval, gamma, learning_rate, memory);
    }

    @Override
    protected int getAction(NDManager manager, float[] state) throws TranslateException {    	
    	NDArray imitation = imitation_predictor.predict(new NDList(manager.create(state))).singletonOrThrow();
    	imitation = imitation.softmax(-1);
    	NDArray score = target_predictor.predict(new NDList(manager.create(state))).singletonOrThrow();
        return ActionSampler.epsilonGreedy(score, imitation, random, 0);
    }
    

    @Override
	public NDArray updateModel(NDManager manager) throws TranslateException {
        MemoryBatch batch = memory.sampleBatch(batch_size, manager);
        
        NDArray policy = policy_predictor.predict(new NDList(batch.getStates())).singletonOrThrow();
        NDArray target = target_predictor.predict(new NDList(batch.getNextStates())).singletonOrThrow().duplicate();
        NDArray expected_returns = Helper.gather(policy, batch.getActions().toIntArray());
        // Reward from our current policy
        NDArray next_returns = batch.getRewards()
                .add(target.max(new int[] { 1 }).mul(batch.getMasks().logicalNot()).mul(gamma));        
        
        //q_loss = F.smooth_l1_loss(current_Q, target_Q)
		//i_loss = F.nll_loss(imt, action.reshape(-1))
		//Q_loss = q_loss + i_loss + 1e-2 * i.pow(2).mean()
        NDArray q_loss = loss_func.evaluate(new NDList(expected_returns), new NDList(next_returns));
        gradientUpdate(q_loss);
        
        if(!Elasql.HERMES_ROUTING_STRATEGY.equals(Elasql.HermesRoutingStrategy.ONLINE_RL)) {
        	imitation_net = updateImitationNet(manager);
        }
        
        return q_loss;
    }
    
    private Model updateImitationNet(NDManager submanager) throws TranslateException{
    	imitation_model.updateModel(submanager);
    	return imitation_model.getImitationNet();
    }

}

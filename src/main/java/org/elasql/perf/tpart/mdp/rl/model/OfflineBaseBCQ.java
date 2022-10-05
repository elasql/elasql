package org.elasql.perf.tpart.mdp.rl.model;

import java.util.Random;

import org.elasql.perf.tpart.TPartPerformanceManager;
import org.elasql.perf.tpart.mdp.TransactionRoutingEnvironment;
import org.elasql.perf.tpart.mdp.rl.util.Memory;
import org.elasql.server.Elasql;

import ai.djl.Model;
import ai.djl.engine.Engine;
import ai.djl.inference.Predictor;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Parameter;
import ai.djl.training.GradientCollector;
import ai.djl.training.optimizer.Optimizer;
import ai.djl.training.tracker.Tracker;
import ai.djl.translate.NoopTranslator;
import ai.djl.translate.TranslateException;
import ai.djl.util.Pair;

public abstract class OfflineBaseBCQ extends BaseAgent {
    protected static final float MIN_EXPLORE_RATE = 0.1f;
    protected static final float DECAY_EXPLORE_RATE = 0.99f;

    protected final Random random = new Random(0);
    protected Memory memory;

    private final int dim_of_state_space = TransactionRoutingEnvironment.STATE_DIM;
    private final int num_of_actions = TransactionRoutingEnvironment.ACTION_DIM;
    private final int hidden_size;
    private final float learning_rate;

    protected final int batch_size;
    protected final int sync_net_interval;
    protected final float gamma;

    private Optimizer optimizer;
    private Model policy_net;
    private Model target_net;
    protected Model imitation_net;
    private Model final_net;

    protected NDManager manager;
    protected Predictor<NDList, NDList> policy_predictor;
    protected Predictor<NDList, NDList> target_predictor;
    protected Predictor<NDList, NDList> imitation_predictor;

    private int iteration = 0;
    protected float epsilon = 1.0f;

    protected OfflineBaseBCQ(int hidden_size, int batch_size,
            int sync_net_interval, float gamma, float learning_rate, Memory memory) {
        this.hidden_size = hidden_size;
        this.batch_size = batch_size;
        this.sync_net_interval = sync_net_interval;
        this.gamma = gamma;
        this.learning_rate = learning_rate;
        this.memory = memory;
        
        reset();
    }

    @Override
    public final int react(float[] state) {
    	// only evaluation
        int action;
        try (NDManager submanager = manager.newSubManager()) {
            action = getAction(submanager, state);
        } catch (TranslateException e) {
            throw new IllegalStateException(e);
        }
        return action;
    }

    @Override
    public final void collect(float reward, boolean done) {
        // do nothing
    }
    
    @Override
    public final Predictor<NDList, NDList> takeoutPredictor() {
    	for (Pair<String, Parameter> params : target_net.getBlock().getParameters()) {
            final_net.getBlock().getParameters().get(params.getKey()).close();
            final_net.getBlock().getParameters().get(params.getKey()).setShape(null);
            final_net.getBlock().getParameters().get(params.getKey()).setArray(params.getValue().getArray().duplicate());
        }

        return final_net.newPredictor(new NoopTranslator());
    }

    public final Predictor<NDList, NDList> takeoutImitationPredictor() {
    	if(imitation_net == null)
    		return null;
    	return imitation_net.newPredictor(new NoopTranslator());
    }

    @Override
    public final void reset() {
        optimizer = Optimizer.adam().optLearningRateTracker(Tracker.fixed(learning_rate)).build();

        if (manager != null) {
            manager.close();
        }
        manager = NDManager.newBaseManager();
        policy_net = ScoreModel.newModel(manager, dim_of_state_space, hidden_size, num_of_actions);
        target_net = ScoreModel.newModel(manager, dim_of_state_space, hidden_size, num_of_actions);
        final_net = ScoreModel.newModel(manager, dim_of_state_space, hidden_size, num_of_actions);
        if(!Elasql.HERMES_ROUTING_STRATEGY.equals(Elasql.HermesRoutingStrategy.ONLINE_RL)) {
        	imitation_net = prepareImitationNet();
        	imitation_predictor = imitation_net.newPredictor(new NoopTranslator());
        }
        policy_predictor = policy_net.newPredictor(new NoopTranslator());
        syncNets();
    }
    
    @Override
    public void saveFile() {
    	// do nothing
    }

    protected final void syncNets() {
        for (Pair<String, Parameter> params : policy_net.getBlock().getParameters()) {
            target_net.getBlock().getParameters().get(params.getKey()).close();
            target_net.getBlock().getParameters().get(params.getKey()).setShape(null);
            target_net.getBlock().getParameters().get(params.getKey()).setArray(params.getValue().getArray().duplicate());
        }

        target_predictor = target_net.newPredictor(new NoopTranslator());
    }

    protected final void gradientUpdate(NDArray q_loss) {
        try (GradientCollector collector = Engine.getInstance().newGradientCollector()) {
            collector.backward(q_loss);
            for (Pair<String, Parameter> params : policy_net.getBlock().getParameters()) {
                NDArray params_arr = params.getValue().getArray();
                optimizer.update(params.getKey(), params_arr, params_arr.getGradient());
            }
        }

        if (iteration++ % sync_net_interval == 0) {
            epsilon *= DECAY_EXPLORE_RATE;
            syncNets();
        }
    }
    
    protected abstract int getAction(NDManager manager, float[] state) throws TranslateException;
   
	protected Imitation imitation_model;

    private Model prepareImitationNet() {
    	imitation_model = new Imitation(dim_of_state_space, num_of_actions, hidden_size, batch_size, 0.001f, memory);
    	int episode = 0;
		while (episode < 500) {
			episode++;
			try (NDManager submanager = NDManager.newBaseManager().newSubManager()) {
				imitation_model.updateModel(submanager);
				if (episode % 100 == 0) {
					System.out.print("Imitation model loss: ");
					System.out.println(imitation_model.updateModel(submanager).toString());
				}
			} catch (TranslateException e) {
				e.printStackTrace();
			}
		}
    	return imitation_model.getImitationNet();
    }
}

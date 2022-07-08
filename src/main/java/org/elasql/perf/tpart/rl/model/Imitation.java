package org.elasql.perf.tpart.rl.model;

import java.util.Random;

import org.elasql.perf.tpart.rl.util.Memory;
import org.elasql.perf.tpart.rl.util.MemoryBatch;

import ai.djl.Model;
import ai.djl.engine.Engine;
import ai.djl.inference.Predictor;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Parameter;
import ai.djl.training.GradientCollector;
import ai.djl.training.loss.SoftmaxCrossEntropyLoss;
import ai.djl.training.optimizer.Optimizer;
import ai.djl.training.tracker.Tracker;
import ai.djl.translate.NoopTranslator;
import ai.djl.translate.TranslateException;
import ai.djl.util.Pair;

public class Imitation {
	protected static final float MIN_EXPLORE_RATE = 0.1f;
	protected static final float DECAY_EXPLORE_RATE = 0.99f;

	protected final Random random = new Random(0);
	protected Memory memory;
	protected Model imitation_net;

	protected final int dim_of_state_space;
	protected final int num_of_actions;
	protected final int hidden_size;
	protected final float learning_rate;

	protected final int batch_size;

	private final SoftmaxCrossEntropyLoss loss_func = new SoftmaxCrossEntropyLoss();
	private Optimizer optimizer;

	protected NDManager manager;
	protected Predictor<NDList, NDList> imitation_predictor;

	protected Imitation(int dim_of_state_space, int num_of_actions, int hidden_size, int batch_size,
			float learning_rate) {
		this.dim_of_state_space = dim_of_state_space;
		this.num_of_actions = num_of_actions;
		this.hidden_size = hidden_size;
		this.batch_size = batch_size;
		this.learning_rate = learning_rate;
		
		reset();

	}

	protected Imitation(int dim_of_state_space, int num_of_actions, int hidden_size, int batch_size,
			float learning_rate, Memory memory) {
		this.dim_of_state_space = dim_of_state_space;
		this.num_of_actions = num_of_actions;
		this.hidden_size = hidden_size;
		this.batch_size = batch_size;
		this.learning_rate = learning_rate;
		this.memory = memory;

		reset();
	}

	protected final void reset() {
		optimizer = Optimizer.adam().optLearningRateTracker(Tracker.fixed(learning_rate)).build();

		if (manager != null) {
			manager.close();
		}
		manager = NDManager.newBaseManager();
		imitation_net = ImitationModel.newModel(manager, dim_of_state_space, hidden_size, num_of_actions);

		imitation_predictor = imitation_net.newPredictor(new NoopTranslator());
	}

	protected final void gradientUpdate(NDArray i_loss) {
		try (GradientCollector collector = Engine.getInstance().newGradientCollector()) {
			collector.backward(i_loss);
			for (Pair<String, Parameter> params : imitation_net.getBlock().getParameters()) {
				NDArray params_arr = params.getValue().getArray();
				optimizer.update(params.getKey(), params_arr, params_arr.getGradient());
			}
		}
	}

	public void updateModel(NDManager manager) throws TranslateException {
		MemoryBatch batch = memory.sampleBatch(batch_size, manager);
		NDArray action = batch.getActions();

		// imitation part
		NDArray imitation = imitation_predictor.predict(new NDList(batch.getStates())).singletonOrThrow();
        
		NDArray i_loss = loss_func.evaluate(new NDList(action), new NDList(imitation));
        i_loss.setRequiresGradient(true);
//        System.out.println(i_loss.toString());
		gradientUpdate(i_loss);

	}
	
	public Model getImitationNet() {
		return imitation_net;
	}
}

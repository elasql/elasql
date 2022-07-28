package org.elasql.perf.tpart.rl.model;

import ai.djl.Model;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Activation;
import ai.djl.nn.Block;
import ai.djl.nn.Parameter.Type;
import ai.djl.nn.core.Linear;
import ai.djl.training.ParameterStore;
import ai.djl.training.initializer.XavierInitializer;
import ai.djl.util.PairList;

public class ScoreModel extends BaseModel {
    private final Block linear_input;
    private final Block linear_output;

    private final int hidden_size;
    private final int output_size;
    
//    private final Block hidden;

    protected ScoreModel(NDManager manager, int hidden_size, int output_size) {
        super(manager);
        this.linear_input = addChildBlock("linear_input", Linear.builder().setUnits(hidden_size).build());
//        this.hidden = addChildBlock("hidden", Linear.builder().setUnits(hidden_size).build());
        this.linear_output = addChildBlock("linear_output", Linear.builder().setUnits(output_size).build());

        this.hidden_size = hidden_size;
        this.output_size = output_size;
    }

    public static Model newModel(NDManager manager, int input_size, int hidden_size, int output_size) {
        Model model = Model.newInstance("ScoreModel");
        BaseModel net = new ScoreModel(manager, hidden_size, output_size);
        net.initialize(net.getManager(), DataType.FLOAT32, new Shape(input_size));
        model.setBlock(net);

        return model;
    }
    
    @Override
    public void initializeChildBlocks(NDManager manager, DataType data_type, Shape... input_shapes) {
        setInitializer(new XavierInitializer(), Type.WEIGHT);
        linear_input.initialize(manager, data_type, input_shapes[0]);
//        hidden.initialize(manager, data_type, new Shape(hidden_size));
        linear_output.initialize(manager, data_type, new Shape(hidden_size));
    }

	@Override
	public Shape[] getOutputShapes(Shape[] input_shape) {
		return new Shape[] { new Shape(output_size) };
	}
	
	@Override
	protected NDList forwardInternal(ParameterStore parameter_store, NDList inputs, boolean training, PairList<String, Object> params) {
		NDList current = inputs;
		current = linear_input.forward(parameter_store, current, training);
        current = new NDList(Activation.relu(current.singletonOrThrow()));
//        current = hidden.forward(parameter_store, current, training);
//        current = new NDList(Activation.relu(current.singletonOrThrow()));
        current = linear_output.forward(parameter_store, current, training);
        return current;
	}

}

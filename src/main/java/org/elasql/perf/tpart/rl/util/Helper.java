package org.elasql.perf.tpart.rl.util;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.types.Shape;

public final class Helper {
    public static NDArray gather(NDArray arr, int[] indexes) {
        boolean[][] mask = new boolean[(int) arr.size(0)][(int) arr.size(1)];
        for (int i = 0; i < indexes.length; i++) {
            mask[i][indexes[i]] = true;
        }
        NDArray boolean_mask = arr.getManager().create(mask);
        for (int i = (int) boolean_mask.getShape().dimension(); i < arr.getShape().dimension(); i++) {
            boolean_mask = boolean_mask.expandDims(i);
        }

        return arr.get(tile(boolean_mask, arr.getShape())).reshape(Shape.update(arr.getShape(), 1, 1)).squeeze();
    }

    public static NDArray tile(NDArray arr, Shape shape) {
        for (int i = (int) arr.getShape().dimension(); i < shape.dimension(); i++) {
            arr = arr.expandDims(i);
        }
        return arr.broadcast(shape);

    }
}

package org.elasql.perf.tpart.rl.util;

import java.util.Random;

import org.elasql.perf.tpart.rl.agent.Agent;
import org.elasql.util.ElasqlProperties;

import ai.djl.ndarray.NDArray;

public final class ActionSampler {

	private final static double THRESHOLD;
	static {
		THRESHOLD = ElasqlProperties.getLoader().getPropertyAsDouble(ActionSampler.class.getName() + ".THRESHOLD", 0.5);
	}

	public static int random(Random random) {
		return random.nextInt(Agent.ACTION_DIM);
	}

	public static int epsilonGreedy(NDArray distribution, Random random, float epsilon) {
		if (random.nextFloat() < epsilon) {
			return random.nextInt((int) distribution.size());
		} else {
			return greedy(distribution);
		}
	}

	public static int greedy(NDArray distribution) {
		return (int) distribution.argMax().getLong();
	}

	public static int epsilonGreedy(NDArray distribution, NDArray imitation, Random random, float epsilon) {
		if (random.nextFloat() < epsilon) {
			return random.nextInt((int) distribution.size());

		} else {
			return greedy(distribution, imitation, THRESHOLD);
		}
	}

	private static int greedy(NDArray distribution, NDArray imitation, double threshold) {
		NDArray result = imitation.gte(imitation.max().toFloatArray()[0] * threshold);
		distribution = result.mul(distribution);
		return (int) distribution.argMax().getLong();
	}

	public static int greedy(NDArray distribution, NDArray imitation) {
		return greedy(distribution, imitation, THRESHOLD);
	}

	public static int sampleMultinomial(NDArray distribution, Random random) {
		int value = 0;
		long size = distribution.size();
		float rnd = random.nextFloat();
		for (int i = 0; i < size; i++) {
			float cut = distribution.getFloat(value);
			if (rnd > cut) {
				value++;
			} else {
				return value;
			}
			rnd -= cut;
		}

		throw new IllegalArgumentException("Invalid multinomial distribution");
	}
}

package org.elasql.perf.tpart.rl.util;

import java.util.Random;

import ai.djl.ndarray.NDArray;

public final class ActionSampler {
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
			return greedy(distribution, imitation, 0.5f);
		}
	}

	private static int greedy(NDArray distribution, NDArray imitation, float threshold) {
		// Select the actions > threshold
		// imt = (imt/imt.max(1, keepdim=True)[0] > self.threshold).float()
		// Choose the max
		// int((imt * q + (1. - imt) * -1e8).argmax(1))
		NDArray result = imitation.gte(imitation.max().toFloatArray()[0] * threshold);
		distribution = result.mul(distribution);
		return (int) distribution.argMax().getLong();
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

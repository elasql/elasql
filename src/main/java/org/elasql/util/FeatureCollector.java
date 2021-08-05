/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.util;

import java.util.HashMap;
import java.util.Map;

public class FeatureCollector {

	private static final ThreadLocal<FeatureCollector> LOCAL_FEATURE_COLLECTOR = new ThreadLocal<FeatureCollector>() {
		@Override
		protected FeatureCollector initialValue() {
			return new FeatureCollector();
		}
	};
	
	private FeatureCollector() {
		for(String key : keys) {
			features.put(key, null);
		}
	}

	/**
	 * Get the feature collector local to this thread.
	 * 
	 * @return the local feature collector
	 */
	public static FeatureCollector getLocalFeatureCollector() {
		return LOCAL_FEATURE_COLLECTOR.get();
	}
	
	public static String[] keys = {"Start time", "Read PK", "Write PK"};
	
	// Record features and it's value (may be list)
	private Map<String, Object> features = new HashMap<String, Object>();

	public void reset() {
		features.clear();
		for(String key : keys) {
			features.put(key, null);
		}
	}
	
	public void setFeatureValue(String feature, Object value) {
		features.replace(feature, value);
	}

	public Object getFeatureValue(int featureIndex) {
		Object v = features.get(keys[featureIndex]);
		if (v == null)
			return "";
		return v;
	}

	public int getKeyCount() {
		return keys.length;
	}

	public String[] getKeys() {
		return keys;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("==============================\n");
		for (Object key : keys) {
			sb.append(String.format("%-40s: %d us, with %d counts\n", key, features.get(key).toString()));
		}
		sb.append("==============================\n");

		return sb.toString();
	}
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.stratosphere.mitos;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

final class SeqNumAtomicBools {

	private final ArrayList<AtomicBoolean> as;

	SeqNumAtomicBools(int s) {
		as = new ArrayList<>(s);
		for (int j = 0; j < s; j++) {
			as.add(new AtomicBoolean(false));
		}
	}

	boolean getAndSet(int i) {
		ensureCapacity(i);
		AtomicBoolean a = as.get(i);
		return a.getAndSet(true);
	}

	synchronized void clear() {
	    for (AtomicBoolean a: as) {
	        a.set(false);
        }
    }

	private void ensureCapacity(int i) {
		if (i >= as.size()) {
			synchronized (this) {
				if (i >= as.size()) {
					int toAdd = as.size(); // Double the size
					for (int j = 0; j < toAdd; j++) {
						as.add(new AtomicBoolean(false));
					}
				}
			}
		}
	}
}

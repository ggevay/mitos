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

public final class BagIdToObjectMap<T> {

    private final AutoGrowArrayList<AutoGrowArrayList<T>> map = new AutoGrowArrayList<>(3000);

    T get(BagID bid) {
        AutoGrowArrayList<T> innerMap = map.get(bid.cflSize);
        if (innerMap == null) {
            return null;
        } else {
            return innerMap.get(bid.opID);
        }
    }

    void put(BagID bid, T v) {
        AutoGrowArrayList<T> innerMap = map.get(bid.cflSize);
        if (innerMap == null) {
			innerMap = new AutoGrowArrayList<>(50);
            map.put(bid.cflSize, innerMap);
        }
        innerMap.put(bid.opID, v);
    }

    void clear() {
        map.clear();
    }
}

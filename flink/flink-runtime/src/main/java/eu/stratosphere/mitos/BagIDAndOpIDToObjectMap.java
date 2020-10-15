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

public class BagIDAndOpIDToObjectMap<T> {

    private final AutoGrowArrayList<AutoGrowArrayList<AutoGrowArrayList<T>>> map = new AutoGrowArrayList<>(3000);

    T get(BagIDAndOpID bidoid) {
        int key1 = bidoid.bagID.cflSize;
        int key2 = bidoid.bagID.opID;
        int key3 = bidoid.opID;

        AutoGrowArrayList<AutoGrowArrayList<T>> innerMap = map.get(key1);
        if (innerMap == null) {
            return null;
        } else {
            AutoGrowArrayList<T> innerMap2 = innerMap.get(key2);
            if (innerMap2 == null) {
                return null;
            } else {
                return innerMap2.get(key3);
            }
        }
    }

    void put(BagIDAndOpID bidoid, T v) {
        int key1 = bidoid.bagID.cflSize;
        int key2 = bidoid.bagID.opID;
        int key3 = bidoid.opID;

        AutoGrowArrayList<AutoGrowArrayList<T>> innerMap = map.get(key1);
        if (innerMap == null) {
            innerMap = new AutoGrowArrayList<>(50);
            map.put(key1, innerMap);
        }

        AutoGrowArrayList<T> innerMap2 = innerMap.get(key2);
        if (innerMap2 == null) {
            innerMap2 = new AutoGrowArrayList<>(50);
            innerMap.put(key2, innerMap2);
        }

        innerMap2.put(key3, v);
    }

    void clear() {
        map.clear();
    }

}

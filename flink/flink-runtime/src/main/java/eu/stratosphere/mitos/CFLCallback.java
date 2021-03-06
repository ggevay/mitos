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

import java.util.List;

/**
 *
 */
public interface CFLCallback {

	void notifyCFLElement(int cflElement, boolean checkpoint);

	void notifyTerminalBB();

	void notifyCloseInput(BagID bagID, int opID); // todo: az implementacio-kor majd figyelni kell, hogy a ket input lehet ugyanaz a bag is

	void notifyBarrierAllReached(int cflSize);

	void startFromSnapshot(int checkpointId, List<Integer> cfl);

	void startNormally();


	int getOpID();
}

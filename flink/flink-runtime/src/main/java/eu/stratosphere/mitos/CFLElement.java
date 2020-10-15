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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 *
 */
public final class CFLElement {

	public int seqNum;
	public int bbId;

	public void serialize(DataOutputView target) throws IOException {
		target.writeInt(seqNum);
		target.writeInt(bbId);
	}

	public static void deserialize(CFLElement r, DataInputView src) throws IOException {
		r.seqNum = src.readInt();
		r.bbId = src.readInt();
	}

	public CFLElement() {}

	public CFLElement(int seqNum, int bbId) {
		this.seqNum = seqNum;
		this.bbId = bbId;
	}

	@Override
	public String toString() {
		return "CFLElement{" +
				"seqNum=" + seqNum +
				", bbId=" + bbId +
				'}';
	}
}

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

/**
 *
 */
public class BagIDAndOpID {

	public BagID bagID;
	public int opID;

	public BagIDAndOpID(BagID bagID, int opID) {
		this.bagID = bagID;
		this.opID = opID;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {return true;}
		if (o == null || getClass() != o.getClass()) {return false;}

		BagIDAndOpID that = (BagIDAndOpID) o;

		if (opID != that.opID) {return false;}
		return bagID.equals(that.bagID);
	}

	@Override
	public int hashCode() {
		int result = bagID.hashCode();
		result = 31 * result + opID;
		return result;
	}
}

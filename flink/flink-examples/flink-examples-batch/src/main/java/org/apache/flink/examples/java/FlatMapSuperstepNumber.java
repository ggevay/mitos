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

package org.apache.flink.examples.java;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.graph.util.ConnectedComponentsData;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * The transitive closure of a graph contains an edge for each pair of vertices
 * which are endpoints of at least one path in the graph.
 *
 * <p>This algorithm is implemented using a delta iteration. The transitive
 * closure solution set is grown in each step by joining the workset of newly
 * discovered path endpoints with the original graph edges and discarding
 * previously discovered path endpoints (already in the solution set).
 */
@SuppressWarnings("serial")
public class FlatMapSuperstepNumber {

	public static void main (String... args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Integer> init = env.fromElements(Integer.class, 0,1,2);

		IterativeDataSet<Integer> iteration = init.iterate(5);

		DataSet<Integer> next = iteration.getSuperstepNumberAsDataSet().flatMap(new FlatMapFunction<Integer, Integer>() {
			@Override
			public void flatMap(Integer stepNum, Collector<Integer> out) throws Exception {
				out.collect(stepNum);
			}
		});

		DataSet<Integer> result = iteration.closeWith(next, next);

		result.output(new DiscardingOutputFormat<>());

		result.print();

		//System.out.println(env.getExecutionPlan());
	}
}

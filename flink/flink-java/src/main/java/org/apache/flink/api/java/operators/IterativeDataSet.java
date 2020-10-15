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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.AggregatorRegistry;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.function.Function;

/**
 * The IterativeDataSet represents the start of an iteration. It is created from the DataSet that
 * represents the initial solution set via the {@link DataSet#iterate(int)} method.
 *
 * @param <T> The data type of set that is the input and feedback of the iteration.
 *
 * @see DataSet#iterate(int)
 */
@Public
public class IterativeDataSet<T> extends SingleInputOperator<T, T, IterativeDataSet<T>> {

	private final AggregatorRegistry aggregators = new AggregatorRegistry();

	private int maxIterations;

	public IterativeDataSet(ExecutionEnvironment context, TypeInformation<T> type, DataSet<T> input, int maxIterations) {
		super(input, type);
		this.maxIterations = maxIterations;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Closes the iteration. This method defines the end of the iterative program part.
	 *
	 * @param iterationResult The data set that will be fed back to the next iteration.
	 * @return The DataSet that represents the result of the iteration, after the computation has terminated.
	 *
	 * @see DataSet#iterate(int)
	 */
	public DataSet<T> closeWith(DataSet<T> iterationResult) {
		return new BulkIterationResultSet<T>(getExecutionEnvironment(), getType(), this, iterationResult);
	}

	/**
	 * Closes the iteration and specifies a termination criterion. This method defines the end of
	 * the iterative program part.
	 *
	 * <p>The termination criterion is a means of dynamically signaling the iteration to halt. It is expressed via a data
	 * set that will trigger to halt the loop as soon as the data set is empty. A typical way of using the termination
	 * criterion is to have a filter that filters out all elements that are considered non-converged. As soon as no more
	 * such elements exist, the iteration finishes.
	 *
	 * @param iterationResult The data set that will be fed back to the next iteration.
	 * @param terminationCriterion The data set that being used to trigger halt on operation once it is empty.
	 * @return The DataSet that represents the result of the iteration, after the computation has terminated.
	 *
	 * @see DataSet#iterate(int)
	 */
	public DataSet<T> closeWith(DataSet<T> iterationResult, DataSet<?> terminationCriterion) {
		return new BulkIterationResultSet<T>(getExecutionEnvironment(), getType(), this, iterationResult, terminationCriterion);
	}

	/**
	 * Gets the maximum number of iterations.
	 *
	 * @return The maximum number of iterations.
	 */
	public int getMaxIterations() {
		return maxIterations;
	}

	/**
	 * Registers an {@link Aggregator} for the iteration. Aggregators can be used to maintain simple statistics during the
	 * iteration, such as number of elements processed. The aggregators compute global aggregates: After each iteration step,
	 * the values are globally aggregated to produce one aggregate that represents statistics across all parallel instances.
	 * The value of an aggregator can be accessed in the next iteration.
	 *
	 * <p>Aggregators can be accessed inside a function via the
	 * {@link org.apache.flink.api.common.functions.AbstractRichFunction#getIterationRuntimeContext()} method.
	 *
	 * @param name The name under which the aggregator is registered.
	 * @param aggregator The aggregator class.
	 *
	 * @return The IterativeDataSet itself, to allow chaining function calls.
	 */
	@PublicEvolving
	public IterativeDataSet<T> registerAggregator(String name, Aggregator<?> aggregator) {
		this.aggregators.registerAggregator(name, aggregator);
		return this;
	}

	/**
	 * Registers an {@link Aggregator} for the iteration together with a {@link ConvergenceCriterion}. For a general description
	 * of aggregators, see {@link #registerAggregator(String, Aggregator)} and {@link Aggregator}.
	 * At the end of each iteration, the convergence criterion takes the aggregator's global aggregate value and decided whether
	 * the iteration should terminate. A typical use case is to have an aggregator that sums up the total error of change
	 * in an iteration step and have to have a convergence criterion that signals termination as soon as the aggregate value
	 * is below a certain threshold.
	 *
	 * @param name The name under which the aggregator is registered.
	 * @param aggregator The aggregator class.
	 * @param convergenceCheck The convergence criterion.
	 *
	 * @return The IterativeDataSet itself, to allow chaining function calls.
	 */
	@PublicEvolving
	public <X extends Value> IterativeDataSet<T> registerAggregationConvergenceCriterion(
			String name, Aggregator<X> aggregator, ConvergenceCriterion<X> convergenceCheck) {
		this.aggregators.registerAggregationConvergenceCriterion(name, aggregator, convergenceCheck);
		return this;
	}

	/**
	 * Gets the registry for aggregators. On the registry, one can add {@link Aggregator}s and an aggregator-based
	 * {@link ConvergenceCriterion}. This method offers an alternative way to registering the aggregators via
	 * {@link #registerAggregator(String, Aggregator)} and {@link #registerAggregationConvergenceCriterion(String, Aggregator, ConvergenceCriterion)}.
	 *
	 * @return The registry for aggregators.
	 */
	@PublicEvolving
	public AggregatorRegistry getAggregators() {
		return aggregators;
	}

	/**
	 * Returns the current superstep number as a 1-element DataSet.
	 *
	 * Note: this relies on a hacked FlatMapDriver that recognizes the DummyPerStepFlatMap marker interface.
	 */
	public DataSet<Integer> getSuperstepNumberAsDataSet() {
		return flatMap(new SuperstepNumberFlatMapper<>());
	}

	private static class SuperstepNumberFlatMapper<T> extends RichFlatMapFunction<T, Integer> implements DummyPerStepFlatMap {
		@Override
		public void flatMap(T value, Collector<Integer> out) throws Exception {
			out.collect(getIterationRuntimeContext().getSuperstepNumber());
		}
	}

	public <OT, IS extends InputSplit> DataSet<OT> readFile(MapFunction<Integer, InputFormat<OT, IS>> inputFormatFromSuperstepNumber, TypeInformation<OT> typeInfo) {
		int para = getExecutionEnvironment().getParallelism();
		TypeSerializer<OT> serializer = typeInfo.createSerializer(getExecutionEnvironment().getConfig());

		DataSet<InputFormatWithInputSplit<OT, IS>> inputSplits = getSuperstepNumberAsDataSet().flatMap(new FlatMapFunction<Integer, InputFormatWithInputSplit<OT, IS>>() {
			@Override
			public void flatMap(Integer stepNum, Collector<InputFormatWithInputSplit<OT, IS>> out) throws Exception {
				InputFormat<OT, IS> inputFormat = inputFormatFromSuperstepNumber.map(stepNum);
				IS[] splits = inputFormat.createInputSplits(para);
				for(IS inputSplit: splits) {
					out.collect(new InputFormatWithInputSplit<>(inputFormat, inputSplit));
				}
			}
		});

		return inputSplits.rebalance().flatMap(new FlatMapFunction<InputFormatWithInputSplit<OT, IS>, OT>() {
			@Override
			public void flatMap(InputFormatWithInputSplit<OT, IS> ifwis, Collector<OT> out) throws Exception {
				InputFormat<OT, IS> format = ifwis.inputFormat;
				IS inputSplit = ifwis.inputSplit;

				if (format instanceof RichInputFormat) {
					((RichInputFormat<OT, IS>) format).openInputFormat();
				}

				format.open(inputSplit);

				while (!format.reachedEnd()) {
					OT nextElement = format.nextRecord(serializer.createInstance());
					if (nextElement != null) {
						out.collect(nextElement);
					} else {
						break;
					}
				}

				format.close();

				if (format instanceof RichInputFormat) {
					((RichInputFormat<OT, IS>) format).closeInputFormat();
				}
			}
		}).returns(typeInfo);
	}

	private static class InputFormatWithInputSplit<OT, IS extends InputSplit> {

		public InputFormat<OT, IS> inputFormat;
		public IS inputSplit;

		public InputFormatWithInputSplit(InputFormat<OT, IS> inputFormat, IS inputSplit) {
			this.inputFormat = inputFormat;
			this.inputSplit = inputSplit;
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	protected org.apache.flink.api.common.operators.SingleInputOperator<T, T, ?> translateToDataFlow(Operator<T> input) {
		// All the translation magic happens when the iteration end is encountered.
		throw new InvalidProgramException("A data set that is part of an iteration was used as a sink or action."
				+ " Did you forget to close the iteration?");
	}
}

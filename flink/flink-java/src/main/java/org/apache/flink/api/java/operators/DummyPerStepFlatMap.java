package org.apache.flink.api.java.operators;

/**
 * Calling a flatMap on an IterativeDataSet with a FlatMapFunction that is marked with this marker interface will
 * cause the flatMap UDF to be called at every iteration step with one element (null).
 */
public interface DummyPerStepFlatMap {}

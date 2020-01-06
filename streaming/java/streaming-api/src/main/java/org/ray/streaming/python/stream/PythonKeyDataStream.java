package org.ray.streaming.python.stream;

import org.ray.streaming.api.stream.Stream;
import org.ray.streaming.operator.StreamOperator;
import org.ray.streaming.python.PythonOperator;
import org.ray.streaming.python.descriptor.DescriptorFunction;
import org.ray.streaming.python.descriptor.DescriptorFunction.PythonFunctionInterface;
import org.ray.streaming.python.descriptor.DescriptorPartition;

/**
 * Represents a python DataStream returned by a key-by operation.
 */
public class PythonKeyDataStream extends Stream implements PythonStream  {

  public PythonKeyDataStream(PythonDataStream input, StreamOperator streamOperator) {
    super(input, streamOperator);
    this.partition = DescriptorPartition.KeyPartition;
  }

  /**
   * Apply a reduce function to this stream.
   *
   * @param func The reduce function.
   * @return A new DataStream.
   */
  public PythonDataStream reduce(DescriptorFunction func) {
    func.setPythonFunctionInterface(PythonFunctionInterface.REDUCE_FUNCTION);
    return new PythonDataStream(this, new PythonOperator(func));
  }

  public PythonKeyDataStream setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

}

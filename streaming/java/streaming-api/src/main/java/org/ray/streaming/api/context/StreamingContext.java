package org.ray.streaming.api.context;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import org.ray.streaming.api.stream.StreamSink;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.jobgraph.JobGraphBuilder;
import org.ray.streaming.driver.IJobDriver;

/**
 * Encapsulate the context information of a streaming Job.
 */
public class StreamingContext implements Serializable {

  private transient AtomicInteger idGenerator;

  /**
   * The sinks of this streaming job.
   */
  private List<StreamSink> streamSinks;
  private Map<String, String> jobConfig;

  /**
   * The logic plan.
   */
  private JobGraph jobGraph;

  private StreamingContext() {
    this.idGenerator = new AtomicInteger(0);
    this.streamSinks = new ArrayList<>();
    this.jobConfig = new HashMap<>();
  }

  public static StreamingContext buildContext() {
    return new StreamingContext();
  }

  /**
   * Construct job DAG, and execute the job.
   */
  public void execute(String jobName) {
    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(streamSinks, jobName, jobConfig);
    this.jobGraph = jobGraphBuilder.build();
    jobGraph.printPlan();

    ServiceLoader<IJobDriver> serviceLoader = ServiceLoader.load(IJobDriver.class);
    Iterator<IJobDriver> iterator = serviceLoader.iterator();
    Preconditions.checkArgument(iterator.hasNext());
//    IJobDriver jobDriver = new JobDriverImpl(jobConfig);
    IJobDriver jobDriver = iterator.next();
    jobDriver.submit(jobGraph, jobConfig);
  }

  public int generateId() {
    return this.idGenerator.incrementAndGet();
  }

  public void addSink(StreamSink streamSink) {
    streamSinks.add(streamSink);
  }

  public void withConfig(Map<String, String> jobConfig) {
    this.jobConfig.putAll(jobConfig);
  }

  public Map<String, String> getJobConfig() {
    return jobConfig;
  }
}

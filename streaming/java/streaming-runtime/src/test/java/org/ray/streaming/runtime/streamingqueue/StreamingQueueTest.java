package org.ray.streaming.runtime.streamingqueue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.ActorCreationOptions.Builder;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.function.impl.FlatMapFunction;
import org.ray.streaming.api.function.impl.ReduceFunction;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.runtime.queue.QueueUtils;
import org.ray.streaming.util.ConfigKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;


public class StreamingQueueTest implements Serializable {

  private static Logger LOGGER = LoggerFactory.getLogger(StreamingQueueTest.class);
  private String runModeBackup = null;

  @org.testng.annotations.BeforeSuite
  public void suiteSetUp() throws Exception {
    LOGGER.info("Do set up");
    String management = ManagementFactory.getRuntimeMXBean().getName();
    String pid = management.split("@")[0];

    LOGGER.info("StreamingQueueTest pid: {}", pid);
    LOGGER.info("java.library.path = {}", System.getProperty("java.library.path"));
  }

  @org.testng.annotations.AfterSuite
  public void suiteTearDown() throws Exception {
    LOGGER.warn("Do tear down");
  }

  @BeforeClass
  public void setUp() {
  }

  @BeforeMethod
  void beforeMethod() {

    LOGGER.info("beforeTest");
    Ray.shutdown();
    System.setProperty("ray.resources", "CPU:4,RES-A:4");
    System.setProperty("ray.raylet.config.num_workers_per_process_java", "1");

    runModeBackup = System.getProperty("ray.run-mode");
    LOGGER.info("runModeBackup: {}", runModeBackup);
    System.setProperty("ray.run-mode", "CLUSTER");
    System.setProperty("ray.redirect-output", "true");
    // ray init
    Ray.init();
  }

  @AfterMethod
  void afterMethod() {
    LOGGER.info("afterTest");
    Ray.shutdown();
    System.setProperty("ray.run-mode", "SINGLE_PROCESS");
  }

  @Test(timeOut = 3000000)
  public void testProducerConsumer() {
    LOGGER.info("StreamingQueueTest.testProducerConsumer");
    Ray.shutdown();
    System.setProperty("ray.resources", "CPU:4,RES-A:4");
    System.setProperty("ray.raylet.config.num_workers_per_process_java", "1");

    runModeBackup = System.getProperty("ray.run-mode");
    LOGGER.info("runModeBackup: {}", runModeBackup);
    System.setProperty("ray.run-mode", "CLUSTER");
    System.setProperty("ray.redirect-output", "true");
    // ray init
    Ray.init();

    ActorCreationOptions.Builder builder = new Builder(); // .setUseDirectCall(true).setMaxReconstructions(1000);

    RayActor<WriterWorker> writerActor = Ray.createActor(WriterWorker::new, "writer",
        builder.createActorCreationOptions());
    RayActor<ReaderWorker> readerActor = Ray.createActor(ReaderWorker::new, "reader",
        builder.createActorCreationOptions());

    LOGGER.info("call getName on writerActor: {}", Ray.call(WriterWorker::getName, writerActor).get());
    LOGGER.info("call getName on readerActor: {}", Ray.call(ReaderWorker::getName, readerActor).get());

//    LOGGER.info(Ray.call(WriterWorker::testCallReader, writerActor, readerActor).get());
    List<String> outputQueueList = new ArrayList<>();
    List<String> inputQueueList = new ArrayList<>();
    int queueNum = 2;
    for (int i = 0; i < queueNum; ++i) {
      String qid = QueueUtils.getRandomQueueId();
      LOGGER.info("getRandomQueueId: {}", qid);
      inputQueueList.add(qid);
      outputQueueList.add(qid);
      readerActor.getId();
    }

    final int msgCount = 100;
    Ray.call(ReaderWorker::init, readerActor, inputQueueList, writerActor, msgCount);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Ray.call(WriterWorker::init, writerActor, outputQueueList, readerActor, msgCount);


    try {
      Thread.sleep(20*1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Assert.assertEquals(
        Ray.call(ReaderWorker::getTotalMsg, readerActor).get().intValue(),
        msgCount*queueNum);
  }

  @Test(timeOut = 60000)
  public void testWordCount() {
    String resultFile = "/tmp/org.ray.streaming.runtime.streamingqueue.testWordCount.txt";
    deleteResultFile(resultFile);

    Map<String, Integer> wordCount = new ConcurrentHashMap<>();
    StreamingContext streamingContext = StreamingContext.buildContext();
    Map<String, Object> config = new HashMap<>();
    config.put(ConfigKey.STREAMING_BATCH_MAX_COUNT, 1);
    config.put(ConfigKey.STREAMING_QUEUE_TYPE, ConfigKey.STREAMING_QUEUE);
    config.put(ConfigKey.QUEUE_SIZE, "100000");
    streamingContext.withConfig(config);
    List<String> text = new ArrayList<>();
    text.add("hello world eagle eagle eagle");
    StreamSource<String> streamSource = StreamSource.buildSource(streamingContext, text);
    streamSource
        .flatMap((FlatMapFunction<String, WordAndCount>) (value, collector) -> {
          String[] records = value.split(" ");
          for (String record : records) {
            collector.collect(new WordAndCount(record, 1));
          }
        })
        .keyBy(pair -> pair.word)
        .reduce((ReduceFunction<WordAndCount>) (oldValue, newValue) -> {
            LOGGER.info("reduce: {} {}", oldValue, newValue);
            return new WordAndCount(oldValue.word, oldValue.count + newValue.count); })
        .sink(s -> {
          LOGGER.info("sink {} {}", s.word, s.count);
          wordCount.put(s.word, s.count);
          serilizeResultToFile(resultFile, wordCount);
        });

    streamingContext.execute();

    Map<String, Integer> checkWordCount = (Map<String, Integer>)deserilizeResultFromFile(resultFile);
    // Sleep until the count for every word is computed.
    while (checkWordCount == null || checkWordCount.size() < 3) {
      LOGGER.info("sleep");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOGGER.warn("Got an exception while sleeping.", e);
      }
      checkWordCount = (Map<String, Integer>)deserilizeResultFromFile(resultFile);
    }
    LOGGER.info("check");
    Assert.assertEquals(checkWordCount, ImmutableMap.of("eagle", 3, "hello", 1, "world", 1));
  }

  private void serilizeResultToFile(String fileName, Object obj) {
    try {
      ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(fileName));
      out.writeObject(obj);
    } catch (Exception e) {
      LOGGER.error(String.valueOf(e));
    }
  }

  private Object deserilizeResultFromFile(String fileName) {
    Map<String, Integer> checkWordCount = null;
    try {
      ObjectInputStream in = new ObjectInputStream(new FileInputStream(fileName));
      checkWordCount = (Map<String, Integer>)in.readObject();
      Assert.assertEquals(checkWordCount, ImmutableMap.of("eagle", 3, "hello", 1, "world", 1));
    } catch (Exception e) {
      LOGGER.error(String.valueOf(e));
    }
    return checkWordCount;
  }

  private static class WordAndCount implements Serializable {

    public final String word;
    public final Integer count;

    public WordAndCount(String key, Integer count) {
      this.word = key;
      this.count = count;
    }
  }

  private void deleteResultFile(String path) {
    File file = new File(path);
    file.deleteOnExit();
  }
}
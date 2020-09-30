/*
 * Copyright (c) 2020-2020 Splunk, Inc. All rights reserved.
 */
package com.splunk.streaming.flink.test;

import com.splunk.streaming.data.Record;
import com.splunk.streaming.data.Tuple;

import com.google.common.base.Stopwatch;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class AsyncTestContext implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(AsyncTestContext.class);
  private BlockingQueue<Tuple> queue;
  private AsyncLocalStreamEnvironment.MiniClusterWithConfigAndClientResource cluster;

  AsyncTestContext(BlockingQueue<Tuple> queue, AsyncLocalStreamEnvironment.MiniClusterWithConfigAndClientResource cluster) {
    this.queue = queue;
    this.cluster = cluster;
  }

  public BlockingQueue<Tuple> getQueue() {
    return queue;
  }

  public void asyncCheckExpected(List<Record> expected) throws Exception {
    for (Record record : expected) {
      Tuple actual = queue.poll(2, TimeUnit.SECONDS);
      Assert.assertEquals("Expected results do not match actual.", record.getTuple(), actual);
    }
  }

  public void asyncCheckExpectedUnordered(List<Record> expected) throws Exception {
    Tuple[] tuples = new Tuple[expected.size()];
    long maxWaitTimeSeconds = 60L;

    Stopwatch timer = Stopwatch.createStarted();
    int i = 0;
    while (timer.elapsed(TimeUnit.SECONDS) < maxWaitTimeSeconds && i < expected.size()) {
      Tuple tuple = queue.poll(1, TimeUnit.SECONDS);
      if (tuple != null) {
        tuples[i++] = tuple;
        logger.info("received a new data from job output i: {}, tuple: {}", i, tuple);
      }
    }
    logger.info("Received {} records: {}", i, tuples);

    Assert.assertEquals(String.format("Received less than expected records in timeout: %s", maxWaitTimeSeconds),
      expected.size(), i);

    Arrays.sort(tuples, Comparator.comparing(o -> o.get(0).toString()));
    expected.sort(Comparator.comparing(o -> o.getTuple().get(0).toString()));

    i = 0;
    for (Record record : expected) {
      Assert.assertEquals("Expected results do not match actual.", record.getTuple(), tuples[i++]);
    }
  }

  public AsyncLocalStreamEnvironment.MiniClusterWithConfigAndClientResource getCluster() {
    return cluster;
  }

  @Override
  public void close() throws Exception {
    try {
      cluster.getMiniCluster().cancelJob(cluster.getJobID());
      cluster.getMiniCluster().close();
    } finally {
      cluster.after();
    }
  }
}
/*
 * Copyright (c) 2020-2020 Splunk, Inc. All rights reserved.
 */
package com.splunk.streaming.flink.test;

import com.splunk.spl.SplProgramParser;
import com.splunk.streaming.data.Record;
import com.splunk.streaming.data.RecordDescriptor;
import com.splunk.streaming.data.Tuple;
import com.splunk.streaming.flink.streams.core.SinkFunction;
import com.splunk.streaming.flink.streams.core.StreamingFunction;
import com.splunk.streaming.flink.streams.planner.AggregationFunctionPlanner;
import com.splunk.streaming.flink.streams.planner.FlinkDataStreamsPlanner;
import com.splunk.streaming.flink.streams.planner.FunctionPlanner;
import com.splunk.streaming.flink.streams.planner.PlannedVariableTable;
import com.splunk.streaming.flink.streams.planner.PlannerConfiguration;
import com.splunk.streaming.flink.streams.planner.PlannerRegistry;
import com.splunk.streaming.flink.streams.planner.ScalarFunctionPlanner;
import com.splunk.streaming.flink.streams.planner.SinkFunctionPlanner;
import com.splunk.streaming.flink.streams.planner.StreamingFunctionPlanner;
import com.splunk.streaming.language.PipelineCompiler;
import com.splunk.streaming.upl3.core.AggregationFunction;
import com.splunk.streaming.upl3.core.RegistryVersion;
import com.splunk.streaming.upl3.core.ScalarFunction;
import com.splunk.streaming.upl3.core.UserDefinedFunction;
import com.splunk.streaming.upl3.exception.FunctionConflictException;
import com.splunk.streaming.upl3.language.FunctionDefinition;
import com.splunk.streaming.upl3.language.Node;
import com.splunk.streaming.upl3.language.TypeChecker;
import com.splunk.streaming.upl3.language.TypeCheckingConfig;
import com.splunk.streaming.upl3.language.UplSchemaToRecordDescriptor;
import com.splunk.streaming.upl3.language.VersionedFunctionRegistry;
import com.splunk.streaming.upl3.node.Program;
import com.splunk.streaming.upl3.type.SchemaType;
import com.splunk.streaming.util.TypeUtils;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * TestRunner for running tests on pipeline nodes in an embedded Flink context.
 * It can be run with StreamingFunctions or SinkFunctions and make use of DSP scalar functions.
 *
 * You build a pipeline with SPL, run it in Flink using either a blocking
 * (synchronous) call or a non-blocking (asynchronous) call, and verify the results.
 *
 * StreamingFunction tests that only have a Source should use the async call, since a Flink
 * Pipeline with only a source will run until you kill it.
 */
public class TestRunner {

  private static final Logger logger = LoggerFactory.getLogger(TestRunner.class);
  private final AsyncLocalStreamEnvironment environment;
  private final PlannerRegistry plannerRegistry;
  private final VersionedFunctionRegistry functionRegistry;
  private final PlannerConfiguration plannerConfiguration;
  private TestingServer zkTestServer;
  private CuratorFramework zkClient;
  private TypeCheckingConfig typeCheckingConfig;

  /**
   * Create a TestRunner that uses a given VersionedFunctionRegistry and additional UserDefined functions.
   * @param registry
   * @param functions
   * @throws FunctionConflictException
   */
  public TestRunner(VersionedFunctionRegistry registry, UserDefinedFunction... functions) throws FunctionConflictException {
    this.environment = new AsyncLocalStreamEnvironment();
    environment.getStreamExecutionEnvironment().setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    environment.getStreamExecutionEnvironment().getConfig().enableForceAvro();
    environment.getStreamExecutionEnvironment().getConfig().enableObjectReuse();
    environment.getStreamExecutionEnvironment().getConfig().setParallelism(1);

    plannerRegistry = new PlannerRegistry();
    functionRegistry = registry;

    typeCheckingConfig = new TypeCheckingConfig.Builder().build();

    /*
     * Register helper functions provided by the test framework.
     * This saves end users from having to record these in a registry config file.
     */
    registerFunction(new TestSource());
    registerFunction(new TestDelayedSource());
    registerFunction(new ParallelUnevenTestSource());

    // register built-in scalar functions
    for (ScalarFunction scalarFunction : ServiceLoader.load(ScalarFunction.class)) {
      UserDefinedFunction function = TypeUtils.coerce(scalarFunction);
      if (function.getClass().isAnnotationPresent(RegistryVersion.class)) {
        RegistryVersion registryVersion = function.getClass().getAnnotation(RegistryVersion.class);
        if (registryVersion.maxMajorVersion() < 3) {
          continue;
        }
      }
      registerFunction(function);
    }

    // register any additional functions provided by the user
    for (UserDefinedFunction function : functions) {
      registerFunction(function);
    }

    // create planner so it can be configured if necessary
    plannerConfiguration = createPlannerConfigurationForTest();
  }

  /**
   * <p>
   * This method will compile the SPL into a pipeline, run it in Flink, collect the resulting Records
   * into a collector sink, wait for the pipeline to finish running and then validate that
   * the actual Records returned matches the expected List of Records passed in.
   * This method is blocking and will not return until all of the above are done.
   * </p>
   * <p>
   * Hint: This is what "function" (non-source) StreamingFunction tests should use.
   * </p>
   * @param pipeline pipeline defined with SPL to compile into a program
   * @param expected Expected Records at the end of the pipeline once it is finished running.
   * @throws Exception
   */
  public void runSynchronousTest(String pipeline, List<Record> expected) throws Exception {
    // All of the record should have the same descriptor/schema
    runSynchronousTest(pipeline, expected, expected.get(0).getDescriptor());
  }

  /**
   * <p>
   * This method will compile SPL into a pipeline, run it in Flink, collect the resulting Records
   * into a collector sink, wait for the pipeline to finish running and then validate that
   * the actual Records returned matches the expected List of Records passed in.
   * This method is blocking and will not return until all of the above are done.
   * </p>
   * <p>
   * Hint: This is what "function" (non-source) StreamingFunction tests should use, or the other runSynchronousTest
   * method.
   * </p>
   * @param pipeline pipeline defined with SPL to compile into a program
   * @throws Exception
   */
  public BlockingQueue<Tuple> runSynchronousTest(String pipeline) throws Exception {
    BlockingQueue<Tuple> queue = planTest(pipeline);
    environment.getStreamExecutionEnvironment().execute("test");
    return queue;
  }

  /**
   * This is what SinkFunction tests should use. This method will compile the SPL into a pipeline,
   * run it in Flink, wait for the pipeline to finish running and then return. Since a Sink does
   * not output data (as part of a Flink Stream), nothing is returned from this method.
   * This method is blocking and will not return until the Flink job has finished running.
   * @param pipeline pipeline defined with SPL to compile into a program
   */
  public void runSynchronousTestWithSink(String pipeline) throws Exception {
    planTestWithSink(pipeline);
    environment.getStreamExecutionEnvironment().execute("test");
  }

  /**
   * <p>
   * This method will compile SPL into a pipeline, start the pipeline job in Flink and return
   * while the Flink job is still running. The {@link AsyncTestContext} returned has a reference
   * to a BlockingQueue that will collect the output from the Flink pipeline job.
   * To stop the Flink job and shut down the mini-cluster, the caller should call {@link AsyncTestContext#close()}
   * or, better, use a try-with-resources block.
   * </p>
   * <p>
   * Hint: This is what "source" StreamingFunction tests should use, as those Flink jobs
   * will not terminate on their own.
   * </p>
   *
   * @param pipeline pipeline defined with SPL to compile into a program and run in Flink
   */
  public AsyncTestContext runAsynchronousTest(String pipeline) throws Exception {
    BlockingQueue<Tuple> queue = planTest(pipeline);

    AsyncLocalStreamEnvironment.MiniClusterWithConfigAndClientResource cluster = environment.executeDetached("test");

    while (getRunningJobs(cluster.getClusterClient()).isEmpty()) {
      try {
        logger.debug("Sleeping 5 seconds waiting for the job to start");
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
      } catch (InterruptedException ex) {
        Assert.fail("Thread interrupted while waiting for the job to start");
      }
    }
    return new AsyncTestContext(queue, cluster);
  }

  /**
   * This is the run method to use when you have a complete pipeline - a source (thus it must
   * run asynchronously) and a sink (no BlockingQueue with data is returned).
   * To stop the Flink job and shut down the mini-cluster, the caller should call {@link AsyncTestContext#close()}
   * or, better, use a try-with-resources block.
   *
   * @param pipeline pipeline defined with SPL to compile into a program and run in Flink
   * @return AsyncTestContext with a null BlockingQueue. Use this to stop the Flink job.
   */
  public AsyncTestContext runAsynchronousTestWithSink(String pipeline) throws Exception {
    planTestWithSink(pipeline);

    AsyncLocalStreamEnvironment.MiniClusterWithConfigAndClientResource cluster = environment.executeDetached("test");

    while (getRunningJobs(cluster.getClusterClient()).isEmpty()) {
      try {
        logger.debug("Sleeping 5 seconds waiting for the job to start");
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
      } catch (InterruptedException ex) {
        Assert.fail("Thread interrupted while waiting for the job to start");
      }
    }

    return new AsyncTestContext(null, cluster);
  }

  /**
   * Wait until the queue is at least qsize large or 30 seconds elapses, whichever comes first.
   * If the timer goes off before the expected queue size is hit, an IllegalStateException
   * will be thrown.
   *
   * @param q BlockingQueue to check
   * @param qsize Min size of the queue before returning
   */
  public void waitUntilQueueHasSize(BlockingQueue<Tuple> q, final int qsize) {
    Predicate<BlockingQueue<Tuple>> sizeMatch = new Predicate<BlockingQueue<Tuple>>() {
      @Override
      public boolean apply(@Nullable BlockingQueue<Tuple> input) {
        return input.size() >= qsize;
      }
    };
    try {
      waitUntilTrue(30, 250, sizeMatch, q);
    } catch (IllegalStateException e) {
      throw new IllegalStateException("Min expected queue size of " + qsize
        + " was not reached. Final queue size was: " + q.size());
    }
  }

  /**
   * Utility method to return when Predicate you specify returns true or when a timeout of "nseconds"
   * has passed. If the timeout is hit before Predicate returns true an {@link IllegalStateException}
   * will be thrown.
   *
   * @param nseconds Max time to wait in seconds for Predicate to return true
   * @param sleepBetweenMs Amount of time (in millis) to wait between invocations of the Predicate
   * @param predicate Predicate to call to check state
   * @param input the input for the predicate to check (can be null)
   * @param <T> input type of the Predicate
   */
  public <T> void waitUntilTrue(int nseconds, int sleepBetweenMs, Predicate<T> predicate, T input) {
    long start = System.currentTimeMillis();
    while (!predicate.apply(input)) {
      try {
        Thread.sleep(sleepBetweenMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      long timeElapsedInSeconds = (System.currentTimeMillis() - start) / 1000;
      if (timeElapsedInSeconds >= nseconds) {
        throw new IllegalStateException("Predicate did not return true in allotted time");
      }
    }
  }

  public AsyncLocalStreamEnvironment getEnvironment() {
    return environment;
  }

  public PlannerConfiguration getPlannerConfiguration() {
    return plannerConfiguration;
  }

  public void setParallelism(int parallelism) {
    environment.getStreamExecutionEnvironment().getConfig().setParallelism(parallelism);
  }

  public void setCheckpointingInterval(Long intervalInMilliseconds) throws Exception {
    environment.getStreamExecutionEnvironment().enableCheckpointing(intervalInMilliseconds);
  }

  public void startZKServer(int port) throws Exception {
    zkTestServer = new TestingServer(port);
    zkClient = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(), new RetryOneTime(2000));
    zkClient.start();
  }

  public void startZKServer() throws Exception {
    startZKServer(2181);
  }

  public void stopZKServer() throws Exception {
    if (zkClient != null) {
      zkClient.close();
    }
    if (zkTestServer != null) {
      zkTestServer.stop();
    }
  }

  private void runSynchronousTest(String pipeline, List<Record> expected, RecordDescriptor descriptor) throws Exception {
    BlockingQueue<Tuple> queue = runSynchronousTest(pipeline);

    List<Tuple> results = Lists.newArrayList();
    queue.drainTo(results);

    List<Record> actual = Lists.transform(results, new TupleToRecord(descriptor));

    Assert.assertEquals(expected, actual);
  }

  private void planTestWithSink(String pipeline) {
    plan(pipeline);
  }

  private BlockingQueue<Tuple> planTest(String pipeline) {
    Optional<Object> plannerOutput = plan(pipeline);
    Assert.assertTrue("Expected a planner result", plannerOutput.isPresent());

    Object plannerOutputObj = plannerOutput.get();
    DataStream dataStream = null;
    if (plannerOutputObj instanceof List) {
      dataStream = getDataStreamFromPlanOutputList((List<?>) plannerOutputObj);
    } else if (plannerOutputObj instanceof DataStream) {
      dataStream = (DataStream) plannerOutputObj;
    } else {
      // this will fail and return a useful error message
      TypeUtils.expectClass(plannerOutput.get(), DataStream.class);
    }
    DataStream<Tuple> result = TypeUtils.coerce(dataStream);

    CollectObjects<Tuple> sink = new CollectObjects<>();
    BlockingQueue<Tuple> queue = sink.queue();
    result.addSink(sink);

    return queue;
  }

  private Optional<Object> plan(String pipeline) {
    // Compile and type check the pipeline
    PipelineCompiler pipelineCompiler = new PipelineCompiler(functionRegistry);
    SplProgramParser parser = new SplProgramParser(functionRegistry);
    Program program = pipelineCompiler.compilePipeline(parser.toPipeline(pipeline));

    Node checkedProgram = new TypeChecker(functionRegistry, typeCheckingConfig).typeCheck(program);

    FlinkDataStreamsPlanner planner = new FlinkDataStreamsPlanner(
      environment.getStreamExecutionEnvironment(), plannerRegistry, new PlannedVariableTable(), plannerConfiguration);

    return planner.plan(checkedProgram);
  }

  private DataStream getDataStreamFromPlanOutputList(List<?> plannerOutputObj) {
    List<?> plannerOutputList = plannerOutputObj;
    int numStreamsInList = 0;
    for (Object entry : plannerOutputList) {
      if (entry instanceof DataStream) {
        numStreamsInList++;
      }
    }
    if (numStreamsInList > 1) {
      throw new IllegalStateException(
        "Your pipeline has more than out 'endpoint' likely due to branching. This is not yet supported by this test framework."
      );
    } else if (numStreamsInList == 0) {
      throw new IllegalArgumentException("No DataStream found in the plan output. "
        + "Be sure that the final node of your pipeline is not captured by an assignment to a variable.");
    }

    Object finalEntry = plannerOutputList.get(plannerOutputList.size() - 1);
    if (finalEntry instanceof DataStream) {
      return (DataStream) finalEntry;
    } else {
      throw new IllegalStateException("Your pipeline is malformed for this test."
        + "The last statement should be the 'endpoint' that you want to collect data from for testing.");
    }
  }

  private PlannerConfiguration createPlannerConfigurationForTest() {
    PlannerConfiguration plannerConfiguration = new PlannerConfiguration();
    // set planner configuration as needed
    return plannerConfiguration;
  }

  private static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
    Collection<JobStatusMessage> statusMessages = client.listJobs().get();
    return statusMessages.stream()
      .filter(status -> !status.getJobState().isGloballyTerminalState())
      .map(JobStatusMessage::getJobId)
      .collect(Collectors.toList());
  }

  private void registerFunction(UserDefinedFunction function) throws FunctionConflictException {
    FunctionPlanner planner;
    if (function instanceof ScalarFunction) {
      planner = new ScalarFunctionPlanner(((ScalarFunction) function));
    } else if (function instanceof StreamingFunction) {
      planner = new StreamingFunctionPlanner(((StreamingFunction) function));
    } else if (function instanceof SinkFunction) {
      planner = new SinkFunctionPlanner(((SinkFunction) function));
    } else if (function instanceof AggregationFunction) {
      planner = new AggregationFunctionPlanner(((AggregationFunction) function));
    } else {
      throw new IllegalArgumentException(
        String.format("Unknown type of function: %s", function.getClass().getSimpleName())
      );
    }
    // register function first so that it can throw an error if there is a conflict
    FunctionDefinition functionDefinition = new FunctionDefinition(
      function.getName(),
      function.getFunctionType(),
      function.getCategories(),
      function.getAttributes()
    );

    functionRegistry.registerFunction(functionDefinition);
    plannerRegistry.registerFunctionPlanner(function.getName(), functionDefinition, planner);
  }

  private static class TupleToRecord implements Function<Tuple, Record> {

    private final RecordDescriptor descriptor;

    public TupleToRecord(RecordDescriptor descriptor) {
      this.descriptor = descriptor;
    }

    @Nullable
    @Override
    public Record apply(@Nullable Tuple input) {
      return new Record(descriptor, input);
    }
  }

  private static class CollectRecords implements org.apache.flink.streaming.api.functions.sink.SinkFunction<Tuple> {
    private static final long serialVersionUID = 1L;
    private static final Map<UUID, BlockingQueue<Record>> QUEUES = Maps.newHashMap();
    private final UUID queueId;
    private final RecordDescriptor descriptor;

    public CollectRecords(SchemaType schema) {
      this.descriptor = UplSchemaToRecordDescriptor.convert(schema);
      this.queueId = UUID.randomUUID();
      QUEUES.put(queueId, Queues.newLinkedBlockingDeque());
    }

    @Override
    public void invoke(Tuple value, Context context) throws Exception {
      queue().add(new Record(descriptor, value));
    }

    public BlockingQueue<Record> queue() {
      return QUEUES.get(queueId);
    }
  }
}

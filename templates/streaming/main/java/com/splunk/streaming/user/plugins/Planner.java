/*
 * ${SDK_COPYRIGHT_NOTICE}
 */

package com.splunk.streaming.user.plugins;

import com.google.common.base.Preconditions;
import com.splunk.streaming.data.Tuple;
import com.splunk.streaming.flink.streams.planner.FunctionPlanner;
import com.splunk.streaming.flink.streams.planner.PlannerRegistry;
import com.splunk.streaming.flink.streams.plugin.SplunkStreamsPlugin;
import com.splunk.streaming.upl3.node.FunctionCall;
import com.splunk.streaming.user.functions.${SDK_CLASS_NAME}Function;
import com.splunk.streaming.util.TypeUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ${SDK_CLASS_NAME}Planner implements FunctionPlanner, SplunkStreamsPlugin {
  private static final Logger logger = LoggerFactory.getLogger(${SDK_CLASS_NAME}Planner.class);
  private static final String PLANNER_NAME = "${SDK_FUNCTION_NAME}-planner";

  @Override
  public Object plan(FunctionCall functionCall, List<Object> plannedArguments, StreamExecutionEnvironment context) {
    PlannerArguments arguments = getArguments(plannedArguments);
    return arguments.stream;
  }

  // This class is only meant to pass arguments since this code needs to be reentrant!
  public static class PlannerArguments {
    private DataStream<Tuple> stream;
  }

  private PlannerArguments getArguments(List<Object> children) {
    final int expectedMinNumArguments = 1;
    Preconditions.checkArgument(children.size() >= expectedMinNumArguments,
            "Incorrect number of arguments. Expected at least: %s was: %s",
            expectedMinNumArguments, children.size());

    TypeUtils.expectClass(children.get(0), DataStream.class);

    PlannerArguments arguments = new PlannerArguments();

    try {
      arguments.stream = TypeUtils.coerce(children.get(0));
    } catch (Exception e) {
      logger.error("Hello-World planner failed to validate arguments: {}", e.getMessage());
      logger.debug("Stack trace:", e);
      throw new IllegalArgumentException("Unexpected validation error while parsing arguments", e);
    }

    return arguments;
  }

  @Override
  public String getName() {
    return PLANNER_NAME;
  }

  @Override
  public PlannerRegistry registerPlugin(PlannerRegistry plannerRegistry) {
    ${SDK_CLASS_NAME}Function function = new ${SDK_CLASS_NAME}Function();
    return plannerRegistry.registerFunctionPlanner(function.name(), function.getFunctionType(), this);
  }


  public DataStream<Tuple> createOutputStream(PlannerArguments arguments) {
    return arguments.stream;
  }
}

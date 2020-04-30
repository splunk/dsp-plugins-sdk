/*
 * Copyright (c) 2019-2020 Splunk, Inc. All rights reserved.
 */
package com.splunk.streaming.user.functions;

import com.splunk.streaming.data.Record;
import com.splunk.streaming.data.RecordDescriptor;
import com.splunk.streaming.data.Tuple;
import com.splunk.streaming.flink.streams.core.SinkFunction;
import com.splunk.streaming.flink.streams.planner.PlannerContext;
import com.splunk.streaming.flink.streams.planner.ScalarFunctionWrapper;
import com.splunk.streaming.flink.streams.serialization.SerializableRecordDescriptor;
import com.splunk.streaming.upl3.language.Category;
import com.splunk.streaming.upl3.node.FunctionCall;
import com.splunk.streaming.upl3.plugins.Attributes;
import com.splunk.streaming.upl3.plugins.Categories;
import com.splunk.streaming.upl3.type.CollectionType;
import com.splunk.streaming.upl3.type.ExpressionType;
import com.splunk.streaming.upl3.type.FunctionType;
import com.splunk.streaming.upl3.type.RecordType;
import com.splunk.streaming.upl3.type.StringType;
import com.splunk.streaming.upl3.type.TypeVariable;
import com.splunk.streaming.upl3.type.VoidType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Sink function example that writes records to the Flink Task Manager log.
 * Sink functions are functions that accept a collection of records as input and do not output anything.
 *
 * Example SPL usage (assumes input schema has a field called "level", which contains
 * a string with the desired log level):
 *
 * ... | into variable_write_log("logger name", level);
 */
public class VariableWriteLogFunction implements SinkFunction {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory.getLogger(VariableWriteLogFunction.class);

  /* Function name */
  private static final String NAME = "variable_write_log";
  /* Name as displayed in the DSP UI */
  private static final String UI_NAME = "Variable Write Records";
  /* Description as displayed in the DSP UI */
  private static final String DESCRIPTION = "Write Records to the Flink Taskmanager Log, based on logging level";

  /* Argument names */
  private static final String INPUT_ARG = "input";
  private static final String LOGGER_NAME_ARG = "logger_name";
  private static final String LEVEL_ARG = "level";

  private static final Set<String> VALID_LOGGER_LEVELS = ImmutableSet.of(
    "trace", "debug", "info", "warn", "error"
  );

  /**
   * This method defines the function for "variable_write_log",
   * which takes three arguments:
   *
   * input: collection of records, parameterized by type variable "R"
   * logger_name: string literal
   * level: an expression that evaluates to a string
   *
   * The function has "void" return type because it is a sink function.
   *
   * Note: the first argument for functions that accept a collection of records must always be named "input"
   *
   * @return FunctionType which defines the function signature
   */
  @Override
  public FunctionType getFunctionType() {
    final RecordType streamType = new RecordType(
      new TypeVariable("R")
    );

    return FunctionType.newSinkFunctionBuilder(this)
      .returns(VoidType.INSTANCE)
      .addArgument(INPUT_ARG, new CollectionType(streamType))
      .addArgument(LOGGER_NAME_ARG, StringType.INSTANCE)
      .addArgument(LEVEL_ARG, new ExpressionType(StringType.INSTANCE))
      .build();
  }

  /**
   * Called at "plan time" (during pipeline activation). Defines the Flink operator to be run during
   * job execution.
   * @param functionCall The FunctionCall node from the AST which defines the Program to be executed
   * @param context PlannerContext used to access the function arguments and record schema
   * @param environment Flink class that encapsulates the stream execution environment
   */
  @Override
  public void plan(FunctionCall functionCall, PlannerContext context, StreamExecutionEnvironment environment) {
    DataStream<Tuple> input = context.getArgument(INPUT_ARG);
    String loggerName = context.getConstantArgument(LOGGER_NAME_ARG);
    ScalarFunctionWrapper<String> levelExpression = context.getArgument(LEVEL_ARG);

    RecordDescriptor inputDescriptor = context.getDescriptorForArgument(INPUT_ARG);

    LoggingSink loggingSink = new LoggingSink(loggerName, levelExpression, inputDescriptor);

    input.addSink(loggingSink)
      .uid(createUid(functionCall))
      .name(String.format("%s(%s: %s, %s: %s)", NAME, LOGGER_NAME_ARG, loggerName, LEVEL_ARG, levelExpression));
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public List<Category> getCategories() {
    // this is a sink function
    return ImmutableList.of(Categories.SINK.getCategory());
  }

  @Override
  public Map<String, Object> getAttributes() {
    Map<String, Object> attributes = Maps.newHashMap();
    attributes.put(Attributes.NAME.toString(), UI_NAME);
    attributes.put(Attributes.DESCRIPTION.toString(), DESCRIPTION);
    return attributes;
  }

  private static class LoggingSink extends RichSinkFunction<Tuple> {

    private String loggerName;
    private ScalarFunctionWrapper<String> levelExpression;
    private SerializableRecordDescriptor inputDescriptor;

    public LoggingSink(String loggerName, ScalarFunctionWrapper<String> levelExpression, RecordDescriptor inputDescriptor) {
      this.loggerName = loggerName;
      this.levelExpression = levelExpression;
      this.inputDescriptor = new SerializableRecordDescriptor(inputDescriptor);
    }

    /**
     * Called by Flink at runtime. Defines actual sink logic to be executed
     * @param tuple Tuple being processed
     * @param context Context
     * @throws Exception
     */
    @Override
    public void invoke(Tuple tuple, Context context) throws Exception {
      Record record = new Record(inputDescriptor.toRecordDescriptor(), tuple);
      final String msgFormat = "{}: {}";

      levelExpression.setRecord(record);
      String level = levelExpression.call();
      if (level != null) {
        level = level.toLowerCase();
      }

      if ("trace".equals(level)) {
        logger.trace(msgFormat, loggerName, record);
      } else if ("debug".equals(level)) {
        logger.debug(msgFormat, loggerName, record);
      } else if ("info".equals(level)) {
        logger.info(msgFormat, loggerName, record);
      } else if ("warn".equals(level)) {
        logger.warn(msgFormat, loggerName, record);
      } else if ("error".equals(level)) {
        logger.error(msgFormat, loggerName, record);
      } else {
        // no output
      }
    }
  }
}

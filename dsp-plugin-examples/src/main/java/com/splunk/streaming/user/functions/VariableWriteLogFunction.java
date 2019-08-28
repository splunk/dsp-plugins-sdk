/*
 * Copyright (c) 2019-2019 Splunk, Inc. All rights reserved.
 */
package com.splunk.streaming.user.functions;

import com.splunk.streaming.data.Record;
import com.splunk.streaming.data.RecordDescriptor;
import com.splunk.streaming.data.Tuple;
import com.splunk.streaming.flink.streams.core.SinkFunction;
import com.splunk.streaming.flink.streams.planner.PlannerContext;
import com.splunk.streaming.flink.streams.serialization.SerializableRecordDescriptor;
import com.splunk.streaming.upl3.core.ArgumentValidator;
import com.splunk.streaming.upl3.core.Types;
import com.splunk.streaming.upl3.language.Category;
import com.splunk.streaming.upl3.language.Type;
import com.splunk.streaming.upl3.node.FunctionCall;
import com.splunk.streaming.upl3.node.NamedNode;
import com.splunk.streaming.upl3.node.StringValue;
import com.splunk.streaming.upl3.plugins.Attributes;
import com.splunk.streaming.upl3.plugins.Categories;
import com.splunk.streaming.upl3.type.CollectionType;
import com.splunk.streaming.upl3.type.FunctionType;
import com.splunk.streaming.upl3.type.RecordType;
import com.splunk.streaming.upl3.type.SchemaType;
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

public class VariableWriteLogFunction implements SinkFunction, ArgumentValidator {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory.getLogger(VariableWriteLogFunction.class);
  private static final String NAME = "variable_write_log";
  private static final String UI_NAME = "Variable Write Records";
  private static final String DESCRIPTION = "Write Records to the Flink Taskmanager Log, based on logging level defined in a field";

  private static final Set<String> VALID_LOGGER_LEVELS = ImmutableSet.of(
    "trace", "debug", "info", "warn", "error"
  );

  @Override
  public void plan(FunctionCall functionCall, PlannerContext context, StreamExecutionEnvironment environment) {
    DataStream<Tuple> input = context.getArgument("input");
    String loggerName = context.getConstantArgument("logger-name");
    String levelField = context.getConstantArgument("level-field");

    RecordDescriptor inputDescriptor = context.getDescriptorForArgument("input");

    LoggingSink loggingSink = new LoggingSink(loggerName, levelField, inputDescriptor);

    input.addSink(loggingSink)
      .uid(createUid(functionCall))
      .name(String.format("%s(logger-name: %s, logger-level: %s)", NAME, loggerName, levelField));
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
  public FunctionType getFunctionType() {
    final RecordType streamType = new RecordType(
      new TypeVariable("R")
    );

    return FunctionType.newSinkFunctionBuilder(this)
      .returns(VoidType.INSTANCE)
      .addArgument("input", new CollectionType(streamType))
      .addArgument("logger-name", StringType.INSTANCE)
      .addArgument("level-field", StringType.INSTANCE)
      .build();
  }

  @Override
  public List<Category> getCategories() {
    return ImmutableList.of(Categories.SINK.getCategory());
  }

  @Override
  public Map<String, Object> getAttributes() {
    Map<String, Object> attributes = Maps.newHashMap();
    attributes.put(Attributes.NAME.toString(), UI_NAME);
    attributes.put(Attributes.DESCRIPTION.toString(), DESCRIPTION);
    attributes.put(Attributes.IS_SINK.toString(), true);
    return attributes;
  }

  @Override
  public void validateArguments(FunctionCall functionCall) {
    // get field name and ensure that the field is of string type
    List<NamedNode> argumentNodes = functionCall.getCanonicalizedArguments();

    String levelField;
    try {
      levelField = ((StringValue) argumentNodes.get(2).getNode()).getValue();
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("[level-field] argument must be a constant string");
    }

    // If our input stream type is not yet resolved return
    if (!Types.isStream(argumentNodes.get(0).getResultType())) {
      return;
    }

    SchemaType inputSchema = Types.getSchema(argumentNodes.get(0).getResultType());
    Type levelType = inputSchema.getFields().get(levelField);

    if (levelType instanceof TypeVariable) {
      return;
    } else if (!(levelType instanceof StringType)) {
      throw new IllegalArgumentException(String.format("%s field must be of string type.  Found: %s", levelField, levelType.getName()));
    }
  }

  private static class LoggingSink extends RichSinkFunction<Tuple> {

    private String loggerName;
    private String levelField;
    private SerializableRecordDescriptor inputDescriptor;

    public LoggingSink(String loggerName, String levelField, RecordDescriptor inputDescriptor) {
      this.loggerName = loggerName;
      this.levelField = levelField.toLowerCase();
      this.inputDescriptor = new SerializableRecordDescriptor(inputDescriptor);
    }

    @Override
    public void invoke(Tuple tuple, Context context) throws Exception {
      Record record = new Record(inputDescriptor.toRecordDescriptor(), tuple);
      final String msgFormat = "{}: {}";

      String level = record.get(levelField);

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

/*
 * Copyright (c) 2019-2020 Splunk, Inc. All rights reserved.
 */

package com.splunk.streaming.user.functions;

import com.splunk.streaming.data.Record;
import com.splunk.streaming.data.Tuple;
import com.splunk.streaming.flink.streams.core.StreamingFunction;
import com.splunk.streaming.flink.streams.planner.PlannerContext;
import com.splunk.streaming.flink.streams.planner.ScalarFunctionWrapper;
import com.splunk.streaming.flink.streams.serialization.SerializableRecordDescriptor;
import com.splunk.streaming.upl3.core.FunctionCallContext;
import com.splunk.streaming.upl3.core.ReturnTypeAdviser;
import com.splunk.streaming.upl3.core.Types;
import com.splunk.streaming.upl3.language.Category;
import com.splunk.streaming.upl3.language.Type;
import com.splunk.streaming.upl3.language.TypeRegistry;
import com.splunk.streaming.upl3.node.FunctionCall;
import com.splunk.streaming.upl3.node.NamedNode;
import com.splunk.streaming.upl3.node.StringValue;
import com.splunk.streaming.upl3.plugins.Attributes;
import com.splunk.streaming.upl3.plugins.Categories;
import com.splunk.streaming.upl3.type.CollectionType;
import com.splunk.streaming.upl3.type.ExpressionType;
import com.splunk.streaming.upl3.type.FunctionType;
import com.splunk.streaming.upl3.type.MapType;
import com.splunk.streaming.upl3.type.RecordType;
import com.splunk.streaming.upl3.type.SchemaType;
import com.splunk.streaming.upl3.type.StringType;
import com.splunk.streaming.upl3.type.TypeVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Streaming function example that operates on a record with a map and outputs one record for each key-value
 * pair in the map, inserting the key and value as new fields in the record schema.
 *
 * Streaming functions are functions that accept a collection of records as input and output
 * a collection of records.
 *
 * Example SPL usage (assumes input schema has a field called "mymap", which contains a map):
 *
 * ... | map_expand map=mymap key_field="foo" value_field="bar" | ...
 */
public class MapExpandFunction implements StreamingFunction, ReturnTypeAdviser {
  private static final long serialVersionUID = 1L;

  /* Function name */
  private static final String NAME = "map_expand";
  /* Name to be displayed in the DSP UI */
  private static final String UI_NAME = "Map Expand";
  /* Description to be displayed in the DSP UI */
  private static final String UI_DESCRIPTION = "Takes a map and creates a new record for each key and value pair";

  /* Argument names */
  private static final String INPUT_ARG = "input";
  private static final String MAP_ARG = "map";
  private static final String KEY_FIELD_ARG = "key_field";
  private static final String VALUE_FIELD_ARG = "value_field";

  /**
   * This method defines the function signature for "map_expand", which takes
   * five arguments:
   *
   * input: collection of records with an unknown schema, parameterized by type variable "R"
   * map: an expression that evaluates to a map<string, M>
   * key_field: string literal
   * value_field: string literal
   *
   * and returns a collection of records (a stream) with some schema "S"
   *
   * Note: the first argument for functions that accept a collection of records must always be named "input".
   *
   * @return FunctionType which defines the function signature
   */
  @Override
  public FunctionType getFunctionType() {
    return FunctionType.newStreamingFunctionBuilder(this)
      // unknown output stream
      .returns(new CollectionType(new RecordType(new TypeVariable("S"))))
      // unknown input stream
      .addArgument(INPUT_ARG, new CollectionType(new RecordType(new TypeVariable("R"))))
      // map to expand on
      .addArgument(MAP_ARG, new ExpressionType(new MapType(StringType.INSTANCE, new TypeVariable("M"))))
      // field names
      .addArgument(KEY_FIELD_ARG, StringType.INSTANCE)
      .addArgument(VALUE_FIELD_ARG, StringType.INSTANCE)
      .build();
  }

  /**
   * Called during type checking (pipeline validation) to get the concrete output schema of this function,
   * since the output schema may depend on the input arguments.
   * @param functionCall The FunctionCall node from the AST which defines the Program to be executed
   * @param typeRegistry Contains all types registered with the type system
   * @param context Contains contextual information that may be useful to determine the return type (for example
   *                the input schema).
   * @return The return type for this function
   */
  @Override
  public Type getReturnType(FunctionCall functionCall, TypeRegistry typeRegistry, FunctionCallContext context) {
    List<NamedNode> argumentNodes = functionCall.getCanonicalizedArguments();

    String keyField;
    String valueField;
    Type mapType;

    if (argumentNodes.size() != 4) {
      throw new IllegalArgumentException(String.format("Expecting 4 arguments found %d.", argumentNodes.size()));
    }

    // Throw exceptions to fail type checking
    mapType = argumentNodes.get(1).getNode().getResultType();
    // map type must be a map or unresolved.
    if (mapType instanceof TypeVariable) {
      return null;
    } else if (!(mapType instanceof MapType)) {
      throw new IllegalArgumentException(String.format("[%s] argument must be a map", MAP_ARG));
    }

    try {
      keyField = ((StringValue) argumentNodes.get(2).getNode()).getValue();
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(String.format("[%s] argument must be a constant string", KEY_FIELD_ARG));
    }

    try {
      valueField = ((StringValue) argumentNodes.get(3).getNode()).getValue();
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(String.format("[%s] argument must be a constant string", VALUE_FIELD_ARG));
    }

    SchemaType inputSchema = Types.getSchema(argumentNodes.get(0).getResultType());
    SchemaType.Builder schemaBuilder = SchemaType.newBuilder();

    // shallow copy fields map
    Map<String, Type> fields = new HashMap<>(inputSchema.getFields());
    fields.put(keyField, ((MapType) mapType).getKeyType());
    fields.put(valueField, ((MapType) mapType).getValueType());

    for (Map.Entry<String, Type> entry : fields.entrySet()) {
      schemaBuilder.addField(entry.getKey(), entry.getValue());
    }

    return new CollectionType(new RecordType(schemaBuilder.build()));
  }

  /**
   * This method is called at "plan time" (during pipeline activation). Defines the Flink operator to be run during
   * job execution.
   * @param functionCall The FunctionCall node from the AST which defines the Program to be executed
   * @param context PlannerContext used to access the function arguments and record schema
   * @param streamExecutionEnvironment Flink class that encapsulates the stream execution environment
   */
  @Override
  public DataStream<Tuple> plan(FunctionCall functionCall, PlannerContext context, StreamExecutionEnvironment streamExecutionEnvironment) {
    DataStream<Tuple> input = context.getArgument(INPUT_ARG);
    ScalarFunctionWrapper<Map<String, Object>> map = context.getArgument(MAP_ARG);
    String keyField = context.getConstantArgument(KEY_FIELD_ARG);
    String valueField = context.getConstantArgument(VALUE_FIELD_ARG);

    SerializableRecordDescriptor inputDescriptor = new SerializableRecordDescriptor(context.getDescriptorForArgument(INPUT_ARG));
    SerializableRecordDescriptor outputDescriptor = new SerializableRecordDescriptor(context.getDescriptorForReturnType());

    MapExpand mapExpand = new MapExpand(inputDescriptor, outputDescriptor, map, keyField, valueField);

    String name = String.format("%s()", NAME);

    return input.flatMap(mapExpand)
      .name(name)
      .uid(createUid(functionCall));
  }

  private static class MapExpand extends RichFlatMapFunction<Tuple, Tuple> {
    private static final long serialVersionUID = 1L;
    private final SerializableRecordDescriptor inputDescriptor;
    private final SerializableRecordDescriptor outputDescriptor;
    private final ScalarFunctionWrapper<Map<String, Object>> mapFunction;
    private final String keyField;
    private final String valueField;

    public MapExpand(SerializableRecordDescriptor inputDescriptor, SerializableRecordDescriptor outputDescriptor,
      ScalarFunctionWrapper<Map<String, Object>> mapFunction, String keyField, String valueField) {
      this.inputDescriptor = inputDescriptor;
      this.outputDescriptor = outputDescriptor;
      this.mapFunction = mapFunction;
      this.keyField = keyField;
      this.valueField = valueField;
    }

    /**
     * Called by Flink at runtime. Maps an input Tuple to one or more output Tuples
     * @param value Tuple being processed
     * @param out Collector which accepts output records from this function
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple value, Collector<Tuple> out) throws Exception {
      Record inputRecord = new Record(inputDescriptor.toRecordDescriptor(), value);

      mapFunction.setRecord(inputRecord);
      Map<String, Object> mapResult = mapFunction.call();
      if (mapResult == null) {
        return;
      }

      // get fields and remove reused names
      Set<String> inputFields = inputRecord.getFieldNames();
      if (inputFields.contains(keyField)) {
        inputFields.remove(keyField);
      }
      if (inputFields.contains(valueField)) {
        inputFields.remove(valueField);
      }

      for (Map.Entry<String, Object> entry : mapResult.entrySet()) {
        Record outputRecord = new Record(outputDescriptor.toRecordDescriptor());
        for (String field : inputRecord.getFieldNames()) {
          outputRecord.put(field, inputRecord.get(field));
        }
        outputRecord.put(keyField, entry.getKey());
        outputRecord.put(valueField, entry.getValue());
        out.collect(outputRecord.getTuple());
      }
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<Category> getCategories() {
    // this is a streaming function
    return ImmutableList.of(Categories.STREAMING.getCategory());
  }

  @Override
  public Map<String, Object> getAttributes() {
    Map<String, Object> attributes = Maps.newHashMap();
    attributes.put(Attributes.NAME.toString(), UI_NAME);
    attributes.put(Attributes.DESCRIPTION.toString(), UI_DESCRIPTION);
    return attributes;
  }
}

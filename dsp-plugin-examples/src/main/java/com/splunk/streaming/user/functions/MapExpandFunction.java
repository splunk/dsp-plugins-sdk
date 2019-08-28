/*
 * Copyright (c) 2019-2019 Splunk, Inc. All rights reserved.
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

public class MapExpandFunction implements StreamingFunction, ReturnTypeAdviser {
  private static final long serialVersionUID = 1L;

  private static final String NAME = "map_expand";
  private static final String UI_NAME = "Map Expand";
  private static final String UI_DESCRIPTION = "Takes a map and creates a new record for each key and value pair";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.newStreamingFunctionBuilder(this)
      // unknown input stream
      .returns(new CollectionType(new RecordType(new TypeVariable("S"))))
      // unknown output stream
      .addArgument("input", new CollectionType(new RecordType(new TypeVariable("R"))))
      // map to expand on
      .addArgument("map", new MapType(StringType.INSTANCE, new TypeVariable("M")))
      // field names
      .addArgument("key-field", StringType.INSTANCE)
      .addArgument("value-field", StringType.INSTANCE)
      .build();
  }

  @Override
  public List<Category> getCategories() {
    return ImmutableList.of(Categories.FUNCTION.getCategory());
  }

  @Override
  public Map<String, Object> getAttributes() {
    Map<String, Object> attributes = Maps.newHashMap();
    attributes.put(Attributes.NAME.toString(), UI_NAME);
    attributes.put(Attributes.DESCRIPTION.toString(), UI_DESCRIPTION);
    return attributes;
  }

  @Override
  public Type getReturnType(FunctionCall functionCall, TypeRegistry typeRegistry, FunctionCallContext context) {
    List<NamedNode> argumentNodes = functionCall.getCanonicalizedArguments();

    String keyField;
    String valueField;
    Type mapType;

    if (argumentNodes.size() != 4) {
      throw new IllegalArgumentException(String.format("Expecting 4 arguments found %d.", argumentNodes.size()));
    }

    // If our input stream type is not yet resolved, return null
    if (!Types.isStream(argumentNodes.get(0).getResultType())) {
      return null;
    }

    // Throw expections to fail validation
    mapType = argumentNodes.get(1).getNode().getResultType();
    // map type must be a map or unresolved.
    if (mapType instanceof TypeVariable) {
      return null;
    } else if (!(mapType instanceof MapType)) {
      throw new IllegalArgumentException("[map] argument must be a map");
    }

    try {
      keyField = ((StringValue) argumentNodes.get(2).getNode()).getValue();
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("[key-field] argument must be a constant string");
    }

    try {
      valueField = ((StringValue) argumentNodes.get(3).getNode()).getValue();
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("[value-field] argument must be a constant string");
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

  @Override
  public DataStream<Tuple> plan(FunctionCall functionCall, PlannerContext context, StreamExecutionEnvironment streamExecutionEnvironment) {
    DataStream<Tuple> input = context.getArgument("input");
    ScalarFunctionWrapper<Map<String, Object>> map = context.getArgument("map");
    String keyField = context.getConstantArgument("key-field");
    String valueField = context.getConstantArgument("value-field");

    SerializableRecordDescriptor inputDescriptor = new SerializableRecordDescriptor(context.getDescriptorForArgument("input"));
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
}

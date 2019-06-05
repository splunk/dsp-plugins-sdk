/*
 * ${SDK_COPYRIGHT_NOTICE}
 */

package com.splunk.streaming.user.functions;

import com.splunk.streaming.flink.streams.core.SinkFunction;
import com.splunk.streaming.flink.streams.planner.PlannerContext;
import com.splunk.streaming.upl3.language.Category;
import com.splunk.streaming.upl3.node.FunctionCall;
import com.splunk.streaming.upl3.plugins.Attributes;
import com.splunk.streaming.upl3.plugins.Categories;
import com.splunk.streaming.upl3.type.FunctionType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.List;
import java.util.Map;

public class ${SDK_CLASS_NAME}Function implements SinkFunction {

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.newSinkFunctionBuilder(this)
      //.returns(AnyType.INSTANCE) //TODO: Add Return type here
      //.addArgument("foo", StringType.INSTANCE) //TODO: Add arguments here
      .build();
  }

  @Override
  public String getName() {
    return "${SDK_FUNCTION_NAME}";
  }

  @Override
  public List<Category> getCategories() {
    return ImmutableList.of(Categories.SINK.getCategory());
  }

  @Override
  public Map<String, Object> getAttributes() {
    Map<String, Object> attributes = Maps.newHashMap();
    attributes.put(Attributes.NAME.toString(), "${SDK_UI_NAME}");
    //attributes.put(Attributes.ICON.toString(), "Icon Name"); //TODO: Add ICON Name here
    //attributes.put(Attributes.COLOR.toString(), "UI Color"); //TODO: Add UI Color here
    return attributes;
  }

  @Override
  public void plan(FunctionCall functionCall, PlannerContext context, StreamExecutionEnvironment streamExecutionEnvironment) {
    //TODO: Add implementation here
  }

}

/*
 * ${SDK_COPYRIGHT_NOTICE}
 */

package com.splunk.streaming.user.functions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.splunk.streaming.upl3.language.FunctionDefinition;
import com.splunk.streaming.upl3.language.FunctionRegistry;
import com.splunk.streaming.upl3.language.TypeRegistry;
import com.splunk.streaming.upl3.plugins.Attributes;
import com.splunk.streaming.upl3.plugins.Categories;
import com.splunk.streaming.upl3.plugins.Plugin;
import com.splunk.streaming.upl3.type.RecordType;
import com.splunk.streaming.upl3.type.FunctionType;
import com.splunk.streaming.upl3.type.TypeVariable;
import com.splunk.streaming.upl3.type.ArgumentType;
import com.splunk.streaming.upl3.type.CollectionType;

import java.util.List;
import java.util.Map;

// Pass-through function
public class ${SDK_CLASS_NAME}Function implements Plugin {

  public static final String FUNCTION_NAME = "${SDK_FUNCTION_NAME}";
  public static final String UI_NAME = "${SDK_UI_NAME}";

  private final FunctionType functionType;

  public ${SDK_CLASS_NAME}Function() {
    RecordType streamType = new RecordType(
            new TypeVariable("T")
    );

    List<ArgumentType> argList = ImmutableList.of(
            new ArgumentType("stream", new CollectionType(streamType))
    );

    functionType = FunctionType.newBuilder().
            addArguments(argList).
            returns(new CollectionType(streamType)).
            build();
  }

  @Override
  public FunctionRegistry registerPlugin(FunctionRegistry functionRegistry) {
    Map<String, Object> uiAttrs = Maps.newHashMap();
    uiAttrs.put(Attributes.NAME.toString(), UI_NAME);

    uiAttrs.put(Attributes.ICON.toString(), "magnet");
    uiAttrs.put(Attributes.COLOR.toString(), "#E68B40");

    return functionRegistry.registerFunction(
            name(),
            new FunctionDefinition(functionType, ImmutableList.of(Categories.FUNCTION.getCategory()), uiAttrs)
    );
  }

  @Override
  public TypeRegistry registerTypes(TypeRegistry typeRegistry) {
    return typeRegistry;
  }

  @Override
  public String name() {
    return FUNCTION_NAME;
  }

  @Override
  public FunctionType getFunctionType() {
    return functionType;
  }

}

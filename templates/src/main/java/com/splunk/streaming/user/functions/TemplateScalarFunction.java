/*
 * Copyright (c) 2019-2019 Splunk, Inc. All rights reserved.
 */

package com.splunk.streaming.user.functions;

import com.splunk.streaming.upl3.core.RuntimeContext;
import com.splunk.streaming.upl3.core.ScalarFunction;
import com.splunk.streaming.upl3.language.Category;
import com.splunk.streaming.upl3.plugins.Attributes;
import com.splunk.streaming.upl3.plugins.Categories;
import com.splunk.streaming.upl3.type.FunctionType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class TemplateScalarFunction implements ScalarFunction<Object> {

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.newScalarFunctionBuilder(this)
      //.returns(AnyType.INSTANCE) //TODO: Add Return type here
      //.addArgument("foo", StringType.INSTANCE) //TODO: Add arguments here
      .build();
  }

  @Override
  public String getName() {
    return "template-scalar";
  }

  @Override
  public List<Category> getCategories() {
    return ImmutableList.of(Categories.FUNCTION.getCategory());
  }

  @Override
  public Map<String, Object> getAttributes() {
    Map<String, Object> attributes = Maps.newHashMap();
    attributes.put(Attributes.NAME.toString(), "Template scalar function.");
    return attributes;
  }

  @Override
  public Object call(RuntimeContext context) {
    return null; //TODO: Add implementation here
  }

}

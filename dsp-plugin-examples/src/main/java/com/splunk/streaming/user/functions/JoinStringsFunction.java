/*
 * Copyright (c) 2019-2019 Splunk, Inc. All rights reserved.
 */

package com.splunk.streaming.user.functions;

import com.splunk.streaming.upl3.core.RuntimeContext;
import com.splunk.streaming.upl3.core.ScalarFunction;
import com.splunk.streaming.upl3.language.Category;
import com.splunk.streaming.upl3.plugins.Attributes;
import com.splunk.streaming.upl3.plugins.Categories;
import com.splunk.streaming.upl3.type.CollectionType;
import com.splunk.streaming.upl3.type.FunctionType;
import com.splunk.streaming.upl3.type.StringType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class JoinStringsFunction implements ScalarFunction<String> {

  private static final String NAME = "join_strings";
  private static final String UI_NAME = "Join Strings";
  private static final String UI_DESCRIPTION = "Joins a list of strings with a provided delimiter.";

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.newScalarFunctionBuilder(this)
      .returns(StringType.INSTANCE)
      // add first argument which is a list of strings
      .addArgument("strings", new CollectionType(StringType.INSTANCE))
      // add second argument which is a string
      .addArgument("delimiter", StringType.INSTANCE)
      .build();
  }

  @Override
  public String getName() {
    return NAME;
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
  public String call(RuntimeContext context) {
    // Get first argument by name
    List<String> strings = context.getArgument("strings");
    // Get second argument by name
    String delimiter = context.getArgument("delimiter");

    // validate arguments, return null if arguments are null
    if (strings == null || delimiter == null) {
      return null;
    }

    // return joined string
    return String.join(delimiter, strings);
  }
}
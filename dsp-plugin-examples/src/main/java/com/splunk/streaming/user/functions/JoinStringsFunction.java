/*
 * Copyright (c) 2019-2020 Splunk, Inc. All rights reserved.
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

/**
 * Scalar function example that joins strings with a provided delimiter.
 *
 * Example SPL usage:
 *
 * ... | eval strings_joined = join_strings(["foo", upper("host"), "baz], "-");
 */
public class JoinStringsFunction implements ScalarFunction<String> {

  /* Function name */
  private static final String NAME = "join_strings";

  /* Argument names */
  private static final String STRINGS_ARG = "strings";
  private static final String DELIMITER_ARG = "delimiter";

  /**
   * This method defines the function signature for "join_strings",
   * which has two arguments:
   *
   * strings: a collection of strings
   * delimiter: string
   *
   * and returns a string.
   *
   * @return FunctionType which defines the function signature
   */
  @Override
  public FunctionType getFunctionType() {
    return FunctionType.newScalarFunctionBuilder(this)
      .returns(StringType.INSTANCE)
      .addArgument(STRINGS_ARG, new CollectionType(StringType.INSTANCE))
      .addArgument(DELIMITER_ARG, StringType.INSTANCE)
      .build();
  }

  /**
   * Called at runtime to execute the scalar function.
   * @param context RuntimeContext provides access to arguments
   * @return joined string
   */
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

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<Category> getCategories() {
    // this is a scalar function
    return ImmutableList.of(Categories.SCALAR.getCategory());
  }

  @Override
  public Map<String, Object> getAttributes() {
    return Maps.newHashMap();
  }
}
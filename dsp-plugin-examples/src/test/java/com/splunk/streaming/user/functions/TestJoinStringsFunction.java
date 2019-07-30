/*
 * Copyright (c) 2019-2019 Splunk, Inc. All rights reserved.
 */

package com.splunk.streaming.user.functions;

import com.splunk.streaming.upl3.core.RuntimeContext;
import com.splunk.streaming.user.functions.JoinStringsFunction;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class TestJoinStringsFunction {
  @Parameterized.Parameters(name = "{0}")
  public static Object[][] inputParameters() {
    return new Object[][] {
      {"Join 1,2,3 with |", Arrays.asList("1", "2", "3"), "|", "1|2|3"},
      {"Join 1,2,3 with ,", Arrays.asList("1", "2", "3"), ",", "1,2,3"},
      {"Join 1,null,3 with ,", Arrays.asList("1", null, "3"), ",", "1,null,3"},
      {"Join 1,2,3 with null", Arrays.asList("1", "2", "3"), null, null},
      {"Join empty list with ,", Arrays.asList(), ",", ""},
      {"Join null list with ,", null, ",", null},
    };
  }

  private String description;
  private List<String> strings;
  private String delimiter;
  private Object expectedOutput;

  public TestJoinStringsFunction(String description, List<String> strings, String delimiter, Object expectedOutput) {
    this.description = description;
    this.strings = strings;
    this.delimiter = delimiter;
    this.expectedOutput = expectedOutput;
  }

  @Test
  public void testJoinStringsFunction() {
    RuntimeContext context = new RuntimeContext(null);
    context.addArgument("strings", strings);
    context.addArgument("delimiter", delimiter);

    JoinStringsFunction function = new JoinStringsFunction();
    Object actualOutput = function.call(context);

    Assert.assertEquals(description, expectedOutput, actualOutput);
  }
}

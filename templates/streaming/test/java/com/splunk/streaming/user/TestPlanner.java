/*
 * ${SDK_COPYRIGHT_NOTICE}
 */

package com.splunk.streaming.user;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import com.splunk.streaming.data.Tuple;
import com.splunk.streaming.flink.streams.planner.FunctionCallPlanner;
import com.splunk.streaming.flink.streams.planner.FunctionPlanner;
import com.splunk.streaming.flink.streams.planner.PlannerRegistry;
import com.splunk.streaming.upl3.language.Node;
import com.splunk.streaming.upl3.node.FunctionCall;
import com.splunk.streaming.upl3.node.StringValue;
import com.splunk.streaming.upl3.type.FunctionType;
import com.splunk.streaming.user.functions.${SDK_CLASS_NAME}Function;
import com.splunk.streaming.user.plugins.${SDK_CLASS_NAME}Planner;
import com.splunk.streaming.util.TypeUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Deque;

public class Test${SDK_CLASS_NAME}Planner {

  private static final Logger logger = LoggerFactory.getLogger(Test${SDK_CLASS_NAME}Planner.class);
  private PlannerRegistry plannerRegistry;
  private Deque<Object> stack;
  private FunctionCall functionCall;
  private DataStream<Tuple> directStream;


  @Before
  public void setup() throws IOException {
    plannerRegistry = new PlannerRegistry();
    stack = Queues.newArrayDeque();

    functionCall = new FunctionCall(
            "${SDK_FUNCTION_NAME}",
            ImmutableList.<Node>of(
                    new StringValue("DataStream input"),
                    new StringValue("topic")
            )
    );

    ${SDK_CLASS_NAME}Function function = new ${SDK_CLASS_NAME}Function();
    FunctionType functionType = function.getFunctionType();
    functionCall.setNodeType(functionType);
    functionCall.setFunctionType(functionType);

    directStream = TypeUtils.coerce(Mockito.mock(DataStream.class));

    ${SDK_CLASS_NAME}Planner planner = Mockito.spy(new ${SDK_CLASS_NAME}Planner());
    Mockito.doReturn(directStream).when(planner).createOutputStream(Mockito.<${SDK_CLASS_NAME}Planner.PlannerArguments>any());
    plannerRegistry.registerFunctionPlanner(function.name(), functionType, planner);
  }

  @Test
  public void testPassStreamThroughPlanner() throws Exception {
    String topic = "events";

    stack.push(directStream);
    stack.push(topic);

    FunctionCallPlanner planner = new FunctionCallPlanner(stack, Mockito.mock(StreamExecutionEnvironment.class), plannerRegistry);
    planner.process(functionCall);
    Assert.assertEquals("Result of function call should be on stack", 1, stack.size());
    Assert.assertEquals("Results should contain dummy object", directStream, stack.pop());

    ${SDK_CLASS_NAME}Function function = new ${SDK_CLASS_NAME}Function();
    FunctionPlanner functionPlanner = plannerRegistry.getByNameAndType(function.name(),
            function.getFunctionType());
    Assert.assertNotNull(functionPlanner);
    Mockito.verify(functionPlanner).plan(
            Mockito.eq(functionCall), Mockito.eq(ImmutableList.of(directStream, topic)), Mockito.any(StreamExecutionEnvironment.class));
  }
}

/*
 * ${SDK_COPYRIGHT_NOTICE}
 */

package com.splunk.streaming.user;

import com.splunk.streaming.upl3.language.FunctionDefinition;
import com.splunk.streaming.upl3.language.FunctionRegistry;
import com.splunk.streaming.upl3.plugins.Attributes;
import com.splunk.streaming.upl3.plugins.Categories;
import com.splunk.streaming.upl3.plugins.PluginService;
import com.splunk.streaming.upl3.type.ArgumentType;
import com.splunk.streaming.upl3.type.CollectionType;
import com.splunk.streaming.upl3.type.FunctionType;
import com.splunk.streaming.user.functions.${SDK_CLASS_NAME}Function;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class Test${SDK_CLASS_NAME}Function {

  private static final String FUNCTION_NAME = ${SDK_CLASS_NAME}Function.FUNCTION_NAME;
  private static final String FUNCTION_UI_NAME = ${SDK_CLASS_NAME}Function.UI_NAME;

  private final PluginService pluginService;
  private final FunctionRegistry functionRegistry;

  public Test${SDK_CLASS_NAME}Function() {
    this.pluginService = PluginService.getInstance();
    this.functionRegistry = pluginService.getFunctionRegistry();
  }

  @BeforeClass
  public static void registerPlugins() {
    PluginService.getInstance().registerPlugins();
  }

  @Test
  public void testFunctionIsLoadedIntoRegistryAndHasCorrectMetadata() {
    Set<FunctionDefinition> functionDefinitions = functionRegistry.getByName(FUNCTION_NAME);

    Assert.assertNotNull("functionDefinitions must not be null", functionDefinitions);
    Assert.assertEquals("We should only have one definition", 1, functionDefinitions.size());
    FunctionDefinition functionDefinition = functionDefinitions.iterator().next();

    Assert.assertEquals("Function definition should have 1 category",
            1, functionDefinition.getCategories().size());
    Assert.assertEquals("Function category should be correct",
            Categories.FUNCTION.getCategory(), functionDefinition.getCategories().get(0));

    Assert.assertNotNull("Function definition should have not-null attributes",
            functionDefinition.getAttributes());
    Assert.assertEquals("Function definition should have correct  name", FUNCTION_UI_NAME,
            functionDefinition.getAttributes().get(Attributes.NAME.toString()));

    FunctionType functionType = functionDefinition.getFunctionType();

    List<ArgumentType> arguments = functionType.getArguments();
    Assert.assertEquals("Wrong number of input arguments", 1, arguments.size());
    Assert.assertTrue("Wrong argument type", arguments.get(0).getType() instanceof CollectionType);
    Assert.assertEquals("Wrong argument field name", "stream", arguments.get(0).getFieldName());
    Assert.assertTrue("Wrong return type", functionType.getReturnType() instanceof CollectionType);

  }
}

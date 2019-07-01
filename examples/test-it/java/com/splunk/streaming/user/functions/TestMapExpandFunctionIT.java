/*
 * Copyright (c) 2019-2019 Splunk, Inc. All rights reserved.
 */

package com.splunk.streaming.user.functions;

import com.splunk.streaming.data.Record;
import com.splunk.streaming.flink.test.TestRunner;
import com.splunk.streaming.flink.test.TestSource;
import com.splunk.streaming.upl3.language.UplSchemaToRecordDescriptor;
import com.splunk.streaming.upl3.type.MapType;
import com.splunk.streaming.upl3.type.SchemaType;
import com.splunk.streaming.upl3.type.StringType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TestMapExpandFunctionIT {

  private TestRunner runner;
  private SchemaType inputSchema;
  private SchemaType outputSchema;
  private Map<String, String> attributes;

  @Before
  public void setUp() throws Exception {
    // Create a new TestRunner to execute the integration tests
    runner = new TestRunner();

    // Shared input schema for running tests.  Add more schemas as needed
    inputSchema = SchemaType.newBuilder()
      .addField("body", StringType.INSTANCE)
      .addField("attributes", new MapType(StringType.INSTANCE, StringType.INSTANCE))
      .build();

    // Shared outupt schema for validating tests.  Add more schemas as needed
    outputSchema = SchemaType.newBuilder()
      .addField("body", StringType.INSTANCE)
      .addField("attributes", new MapType(StringType.INSTANCE, StringType.INSTANCE))
      .addField("key", StringType.INSTANCE)
      .addField("value", StringType.INSTANCE)
      .build();

    attributes = ImmutableMap.of("name", "Joe Doe", "dob", "1970/01/01", "city", "San Francisco");
  }

  @Test
  // test function using the TestRunner.  Add more tests as needed for test coverager
  public void testMapExpand() throws Exception {
    // Create and register test data stream source to test your function
    TestSource.registerSource("input", generateInputRecords(), inputSchema);

    // DSL for your test
    String dsl = "map-expand(test-source(\"input\"), get(\"attributes\"), \"key\", \"value\");";

    // Compile the dsl, run the pipeline, and validate results against an expected records list.
    runner.runSynchronousTest(dsl, generateExpectedRecords());
  }

  // Input records for a test.  Add more generators as needed for test coverage
  // Records are processed in order
  private List<Record> generateInputRecords() {
    List<Record> records = Lists.newArrayListWithCapacity(10);
    for (int idx = 0; idx < 10; idx++) {
      // create new record with the given schema
      Record record = new Record(UplSchemaToRecordDescriptor.convert(inputSchema));

      // add fields to the record
      record.put("body", "Record " + idx);
      record.put("attributes", attributes);

      // add record to the return list
      records.add(record);
    }
    return records;
  }

  // Output records to test results of function.  Add more generators as needed for test coverage
  // Records are processed in order
  private List<Record> generateExpectedRecords() {
    List<String> keysOrder = ImmutableList.of("name", "city", "dob");
    List<Record> records = Lists.newArrayListWithCapacity(10);
    for (int idx = 0; idx < 10; idx++) {
      for (String key : keysOrder) {
        // create new record with the given schema
        Record record = new Record(UplSchemaToRecordDescriptor.convert(outputSchema));

        // add fields to the record
        record.put("body", "Record " + idx);
        record.put("attributes", attributes);
        record.put("key", key);
        record.put("value", attributes.get(key));

        // add record to the return list
        records.add(record);
      }
    }
    return records;
  }
}
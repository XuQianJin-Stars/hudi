/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


/**
 * Unit tests {@link TestPartialUpdateAvroPayload}.
 */
public class TestPartialUpdateAvroPayload {
  private Schema schema;

  String jsonSchema = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"partialRecord\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"name1\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"price1\", \"type\": [\"null\", \"double\"]},\n"
      + "    {\"name\": \"_ts1\", \"type\": [\"null\", \"long\"]},\n"
      + "    {\"name\": \"_hoodie_is_deleted\", \"type\": [\"null\", \"boolean\"], \"default\":false},\n"
      + "    {\"name\": \"name2\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"price2\", \"type\": [\"null\", \"double\"]},\n"
      + "    {\"name\": \"_ts2\", \"type\": [\"null\", \"long\"]}\n"
      + "  ]\n"
      + "}";

  @BeforeEach
  public void setUp() throws Exception {
    schema = new Schema.Parser().parse(jsonSchema);
  }

  @Test
  public void testActiveRecords() throws IOException {
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("name1", "a1_1");
    record1.put("price1", 1.11);
    record1.put("_ts1", 0L);
    record1.put("name2", "a1_2");
    record1.put("price2", 1.12);
    record1.put("_ts2", 0L);

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("name1", "a1_1_1");
    record2.put("price1", 2.11);
    record2.put("_ts1", 1L);
    record2.put("name2", null);
    record2.put("price2", 2.12);
    record2.put("_ts2", 1L);

    GenericRecord record3 = new GenericData.Record(schema);
    record3.put("id", "1");
    record3.put("name1", "a1_1_1");
    record3.put("price1", 2.11);
    record3.put("_ts1", 1L);
    record3.put("name2", null);
    record3.put("price2", 2.12);
    record3.put("_ts2", 1L);

    Properties properties = new Properties();
    properties.put("schema", jsonSchema);
    // Keep record with largest ordering fiel
    PartialUpdateAvroPayload payload1 =
        new PartialUpdateAvroPayload(record1, "_ts1=0:name1,price1;_ts2=0:name2,price2");
    PartialUpdateAvroPayload payload2 =
        new PartialUpdateAvroPayload(record2, "_ts1=1:name1,price1;_ts2=1:name2,price2");
    assertArrayEquals(new PartialUpdateAvroPayload(record3, 2).recordBytes,
        payload1.preCombine(payload2, properties).recordBytes);
    assertArrayEquals(new PartialUpdateAvroPayload(record3, 2).recordBytes,
        payload2.preCombine(payload1, properties).recordBytes);

    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertEquals(record2, payload2.getInsertValue(schema).get());

    assertEquals(record3, payload1.combineAndGetUpdateValue(record2, schema).get());
    assertEquals(record3, payload2.combineAndGetUpdateValue(record1, schema).get());

    // regenerate
    record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("name1", "a1_1");
    record1.put("price1", 2.11);
    record1.put("_ts1", 5L);
    record1.put("name2", "a1_2");
    record1.put("price2", 2.12);
    record1.put("_ts2", 4L);

    record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("name1", null);
    record2.put("price1", null);
    record2.put("_ts1", null);
    record2.put("name2", "a1_1_2");
    record2.put("price2", 2.22);
    record2.put("_ts2", 5L);

    GenericRecord record4 = new GenericData.Record(schema);
    record4.put("id", "1");
    record4.put("name1", "a1_1");
    record4.put("price1", 2.11);
    record4.put("_ts1", 5L);
    record4.put("name2", "a1_1_2");
    record4.put("price2", 2.22);
    record4.put("_ts2", 5L);

    // Update subtable columns if ordering field is larger
    payload1 = new PartialUpdateAvroPayload(record1, "_ts1=5:name1,price1;_ts2=4:name2,price2");
    payload2 = new PartialUpdateAvroPayload(record2, "_ts1=4:name1,price1;_ts2=5:name2,price2");

    PartialUpdateAvroPayload preCombineRes1 = payload1.preCombine(payload2, properties);
    PartialUpdateAvroPayload preCombineRes2 = payload2.preCombine(payload1, properties);

    String expOrderingVal = "_ts1=5:name1,price1;_ts2=5:name2,price2";
    PartialUpdateAvroPayload expPrecombineRes =
        new PartialUpdateAvroPayload(record4, expOrderingVal);

    assertArrayEquals(expPrecombineRes.recordBytes, preCombineRes1.recordBytes);
    assertArrayEquals(expPrecombineRes.recordBytes, preCombineRes2.recordBytes);
    assertEquals(expOrderingVal, preCombineRes1.orderingVal.toString());
    assertEquals(expOrderingVal, preCombineRes2.orderingVal.toString());
  }

  @Test
  public void testDeletedRecord() throws IOException {
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("name1", "a1_1");
    record1.put("price1", 1.11);
    record1.put("_ts1", 1L);
    record1.put("name2", "a1_2");
    record1.put("price2", 2.22);
    record1.put("_ts2", 1L);
    record1.put("_hoodie_is_deleted", false);

    GenericRecord delRecord1 = new GenericData.Record(schema);
    delRecord1.put("id", "1");
    delRecord1.put("name1", "a1_1");
    delRecord1.put("price1", 1.11);
    delRecord1.put("_ts1", 2L);
    delRecord1.put("name2", "a1_2");
    delRecord1.put("price2", 2.22);
    delRecord1.put("_ts2", 2L);
    delRecord1.put("_hoodie_is_deleted", true);

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("name1", "a1_1");
    record2.put("price1", 1.11);
    record2.put("_ts1", 1L);
    record2.put("name2", "a1_2");
    record2.put("price2", 2.22);
    record2.put("_ts2", 1L);
    record2.put("_hoodie_is_deleted", true);

    PartialUpdateAvroPayload payload1 =
        new PartialUpdateAvroPayload(record1, "_ts1=1:name1,price1;_ts2=1:name2,price2");
    PartialUpdateAvroPayload payload2 =
        new PartialUpdateAvroPayload(delRecord1, "_ts1=2:name1,price1;_ts2=2:name2,price2");

    assertEquals(payload1.preCombine(payload2), payload2);
    assertEquals(payload2.preCombine(payload1), payload2);

    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertFalse(payload2.getInsertValue(schema).isPresent());

    Properties properties = new Properties();
    properties.put(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "_ts1:name1,price1;_ts2:name2,price2");
    assertFalse(payload1.combineAndGetUpdateValue(delRecord1, schema, properties).isPresent());
    assertFalse(payload2.combineAndGetUpdateValue(record1, schema, properties).isPresent());
  }

  @Test
  public void testNullColumn() throws IOException {
    // Test to ensure that a record with an older ordering field that has a non-null column should overwrite
    // a null column of a record with a larger ordering field
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("name1", null);
    record1.put("price1", 1.11);
    record1.put("_ts1", 2L);
    record1.put("name2", null);
    record1.put("price2", null);
    record1.put("_ts2", null);

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("name1", "a1_1");
    record2.put("price1", 2.22);
    record2.put("_ts1", 1L);
    record2.put("name2", null);
    record2.put("price2", null);
    record2.put("_ts2", null);

    GenericRecord record3 = new GenericData.Record(schema);
    record3.put("id", "1");
    record3.put("name1", null);
    record3.put("price1", 1.11);
    record3.put("_ts1", 2L);
    record3.put("name2", null);
    record3.put("price2", null);
    record3.put("_ts2", null);

    PartialUpdateAvroPayload payload2 =
        new PartialUpdateAvroPayload(record2, "_ts1=1:name1,price1;_ts2=2:name2,price2");
    assertEquals(payload2.combineAndGetUpdateValue(record1, schema).get(), record3);
  }
}

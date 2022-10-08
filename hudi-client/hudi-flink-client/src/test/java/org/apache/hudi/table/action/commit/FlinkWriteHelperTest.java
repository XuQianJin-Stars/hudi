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

package org.apache.hudi.table.action.commit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.index.HoodieIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * UT for FlinkWriteHelper Test.
 */

public class FlinkWriteHelperTest {

  private transient Schema avroSchema;

  private String preCombineFields = "";

  public static final String SCHEMA = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"partialRecord\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"fa\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_ts1\", \"type\": [\"null\", \"long\"]},\n"
      + "    {\"name\": \"fb\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_ts2\", \"type\": [\"null\", \"long\"]}\n"
      + "  ]\n"
      + "}";

  @TempDir
  File tempFile;

  @BeforeEach
  public void setUp() throws Exception {
    this.preCombineFields = "_ts1:fa;_ts2:fb";
    this.avroSchema = new Schema.Parser().parse(SCHEMA);
  }

  @Test
  void deduplicateRecords() throws IOException, InterruptedException {
    List<HoodieAvroRecord> records = data();
    records = FlinkWriteHelper.newInstance().deduplicateRecords(records, (HoodieIndex) null, -1, this.avroSchema.toString());
    GenericRecord record = HoodieAvroUtils.bytesToAvro(((PartialUpdateAvroPayload) records.get(0).getData()).recordBytes, this.avroSchema);
    System.out.println("======================================================================================");
    System.out.println("last: " + record);
  }

  public List<HoodieAvroRecord> data() throws InterruptedException {
    AtomicInteger faCnt = new AtomicInteger(1);
    AtomicInteger fbCnt = new AtomicInteger(1);
    List<GenericRecord> records = new ArrayList<>();
    for (int i = 1; i <= 100; i++) {
      long ts = System.currentTimeMillis();
      GenericRecord row1 = new GenericData.Record(this.avroSchema);
      row1.put("id", "jack");
      row1.put("fa", faCnt.getAndIncrement() + "");
      row1.put("_ts1", ts);
      GenericRecord row2 = new GenericData.Record(this.avroSchema);
      row2.put("id", "jack");
      row2.put("fb", fbCnt.getAndIncrement() + "");
      row2.put("_ts2", ts);
      records.add(row1);
      records.add(row2);
      Thread.sleep(1);
    }

    return records.stream().map(genericRowData -> {
      try {
        String orderingFieldValText = HoodieAvroUtils.getMultipleOrderingVals(genericRowData,
            preCombineFields, false).toString();
        return new HoodieAvroRecord(new HoodieKey("1", "default"),
            new PartialUpdateAvroPayload(genericRowData, orderingFieldValText), HoodieOperation.INSERT);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

}

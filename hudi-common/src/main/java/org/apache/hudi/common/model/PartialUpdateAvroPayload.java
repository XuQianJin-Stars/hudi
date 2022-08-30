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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * subclass of OverwriteNonDefaultsWithLatestAvroPayload used for delta streamer.
 *
 * <ol>
 * <li>preCombine - Picks the latest delta record for a key, based on an ordering field;
 * <li>combineAndGetUpdateValue/getInsertValue - overwrite storage for specified fields
 * that doesn't equal defaultValue.
 * </ol>
 */
public class PartialUpdateAvroPayload extends OverwriteNonDefaultsWithLatestAvroPayload {

  public static ConcurrentHashMap<String, Schema> schemaRepo = new ConcurrentHashMap<>();

  public PartialUpdateAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public PartialUpdateAvroPayload(Option<GenericRecord> record) {
    super(record); // natural order
  }

  @Override
  public PartialUpdateAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue, Properties properties) {
    String schemaStringIn = properties.getProperty("schema");
    Schema schemaInstance;
    if (!schemaRepo.containsKey(schemaStringIn)) {
      schemaInstance = new Schema.Parser().parse(schemaStringIn);
      schemaRepo.put(schemaStringIn, schemaInstance);
    } else {
      schemaInstance = schemaRepo.get(schemaStringIn);
    }
    if (oldValue.recordBytes.length == 0) {
      // use natural order for delete record
      return this;
    }

    try {
      GenericRecord indexedOldValue = (GenericRecord) oldValue.getInsertValue(schemaInstance).get();
      Option<IndexedRecord> optValue = combineAndGetUpdateValue(indexedOldValue, schemaInstance, this.orderingVal.toString());
      // Rebuild ordering value if required
      String newOrderingVal = rebuildOrderingValMap((GenericRecord) optValue.get(), this.orderingVal.toString());
      if (optValue.isPresent()) {
        return new PartialUpdateAvroPayload((GenericRecord) optValue.get(), newOrderingVal);
      }
    } catch (Exception ex) {
      return this;
    }
    return this;
  }

  public Option<IndexedRecord> combineAndGetUpdateValue(
      IndexedRecord currentValue, Schema schema, String newOrderingValWithMappings) throws IOException {
    Option<IndexedRecord> recordOption = getInsertValue(schema);
    if (!recordOption.isPresent()) {
      return Option.empty();
    }

    // Perform a deserialization again to prevent resultRecord from sharing the same reference as recordOption
    GenericRecord resultRecord = (GenericRecord) getInsertValue(schema).get();
    List<Schema.Field> fields = schema.getFields();

    // newOrderingValWithMappings = _ts1:name1,price1=999;_ts2:name2,price2=;
    for (String newOrderingFieldMapping : newOrderingValWithMappings.split(";")) {
      String orderingField = newOrderingFieldMapping.split(":")[0];
      String newOrderingVal = getNewOrderingVal(orderingField, newOrderingFieldMapping);
      String oldOrderingVal = HoodieAvroUtils.getNestedFieldValAsString(
          (GenericRecord) currentValue, orderingField, true, false);
      if (oldOrderingVal == null) {
        oldOrderingVal = "";
      }

      // No update required
      if (oldOrderingVal.isEmpty() && newOrderingVal.isEmpty()) {
        continue;
      }

      // Pick the payload with greatest ordering value as insert record
      boolean isBaseRecord = false;
      try {
        if (Long.parseLong(oldOrderingVal) > Long.parseLong(newOrderingVal)) {
          isBaseRecord = true;
        }
      } catch (NumberFormatException e) {
        if (oldOrderingVal.compareTo(newOrderingVal) > 0) {
          isBaseRecord = true;
        }
      }

      // Initialise the fields of the sub-tables
      GenericRecord insertRecord;
      if (isBaseRecord) {
        insertRecord = (GenericRecord) currentValue;
        // resultRecord is already assigned as recordOption
      } else {
        insertRecord = (GenericRecord) recordOption.get();
        GenericRecord currentRecord = (GenericRecord) currentValue;
        fields.stream()
            .filter(f -> newOrderingFieldMapping.contains(f.name()))
            .forEach(f -> resultRecord.put(f.name(), currentRecord.get(f.name())));
      }

      // If any of the sub-table records is flagged for deletion, delete entire row
      if (isDeleteRecord(insertRecord)) {
        return Option.empty();
      } else {
        // Perform partial update
        fields.stream()
            .filter(f -> newOrderingFieldMapping.contains(f.name()))
            .forEach(f -> {
              Object value = insertRecord.get(f.name());
              value = f.schema().getType().equals(Schema.Type.STRING) && value != null ? value.toString() : value;
              resultRecord.put(f.name(), value);
            });
      }
    }
    return Option.of(resultRecord);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return this.combineAndGetUpdateValue(currentValue, schema, this.orderingVal.toString());
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties prop) throws IOException {
    Option<IndexedRecord> recordOption = getInsertValue(schema);
    if (!recordOption.isPresent()) {
      return Option.empty();
    }
    String newOrderingFieldMappings = rebuildOrderingValMap(
        (GenericRecord) recordOption.get(), this.orderingVal.toString());
    return combineAndGetUpdateValue(currentValue, schema, newOrderingFieldMappings);
  }

  private static String getNewOrderingVal(String orderingFieldToUse, String newOrderingValWithMapping) {
    String[] newOrderingValWithMappingArr = newOrderingValWithMapping.split(":.*=");
    if (newOrderingValWithMappingArr[0].equals(orderingFieldToUse)) {
      return newOrderingValWithMappingArr.length > 1 ? newOrderingValWithMappingArr[1] : "";
    }
    throw new HoodieException("No ordering field of interest found");
  }

  private static String rebuildOrderingValMap(GenericRecord record, String oldOrderingValWithMappings) {
    StringBuilder sb = new StringBuilder();
    for (String oldOrderingValWithMapping : oldOrderingValWithMappings.split(";")) {
      String[] oldOrderingValWithMappingArr = oldOrderingValWithMapping.split(":.*=");
      Object orderingVal = record.get(oldOrderingValWithMappingArr[0]);
      orderingVal = orderingVal == null ? "" : orderingVal;
      sb.append(oldOrderingValWithMapping.split("=")[0])
          .append("=")
          .append(orderingVal)
          .append(";");
    }
    return sb.toString();
  }

  /**
   * Return true if value equals defaultValue otherwise false.
   */
  public Boolean overwriteField(Object value, Object defaultValue) {
    return value == null;
  }
}

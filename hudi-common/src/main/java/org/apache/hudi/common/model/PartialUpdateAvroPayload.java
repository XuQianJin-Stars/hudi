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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.util.StringUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

  private MultiplePartialUpdateUnit multiplePartialUpdateUnit = null;

  public PartialUpdateAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public PartialUpdateAvroPayload(Option<GenericRecord> record) {
    super(record); // natural order
  }

  @Override
  public PartialUpdateAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue, Properties properties) {
    String schemaStringIn = properties.getProperty("hoodie.avro.schema");
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
      Option<IndexedRecord> optValue = combineAndGetUpdateValue(indexedOldValue, schemaInstance, properties);
      // Rebuild ordering value if required
      if (optValue.isPresent()) {
        return new PartialUpdateAvroPayload(Option.of((GenericRecord) optValue.get()));
      }
    } catch (Exception ex) {
      return this;
    }
    return this;
  }

  public Option<IndexedRecord> combineAndGetUpdateValue(
      IndexedRecord currentValue, Schema schema, MultiplePartialUpdateUnit multiplePartialUpdateUnit) throws IOException {
    Option<IndexedRecord> incomingRecord = getInsertValue(schema);
    if (!incomingRecord.isPresent()) {
      return Option.empty();
    }

    // Perform a deserialization again to prevent resultRecord from sharing the same reference as recordOption
    GenericRecord resultRecord = (GenericRecord) incomingRecord.get();

    Map<String, Schema.Field> name2Field = schema.getFields().stream().collect(Collectors.toMap(Schema.Field::name, item -> item));
    // multipleOrderingFieldsWithCols = _ts1:name1,price1=999;_ts2:name2,price2=;

    final Boolean[] deleteFlag = new Boolean[1];
    deleteFlag[0] = false;
    multiplePartialUpdateUnit.getMultiplePartialUpdateUnits().forEach(partialUpdateUnit -> {

      // Initialise the fields of the sub-tables
      GenericRecord insertRecord = resultRecord;
      boolean needUseOldRecordToUpdate = needUseOldRecordToUpdate(resultRecord, (GenericRecord) currentValue, partialUpdateUnit);
      if (needUseOldRecordToUpdate) {
        insertRecord = (GenericRecord) currentValue;
        // resultRecord is already assigned as recordOption
        GenericRecord finalInsertRecord = insertRecord;
        partialUpdateUnit.getColumnNames().stream()
            .filter(name2Field::containsKey)
            .forEach(fieldName -> resultRecord.put(fieldName, finalInsertRecord.get(fieldName)));
        String oldOrderingVal = HoodieAvroUtils.getNestedFieldValAsString(finalInsertRecord, partialUpdateUnit.getOrderingField(), true, false);
        resultRecord.put(partialUpdateUnit.getOrderingField(), Long.parseLong(oldOrderingVal));
      }
      // If any of the sub-table records is flagged for deletion, delete entire row
      if (isDeleteRecord(insertRecord)) {
        deleteFlag[0] = true;
      }
    });

    if (deleteFlag[0]) {
      return Option.empty();
    }
    return Option.of(resultRecord);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return this.combineAndGetUpdateValue(currentValue, schema, this.multiplePartialUpdateUnit);
  }

  public boolean needUseOldRecordToUpdate(GenericRecord incomingRecord, GenericRecord currentRecord, MultiplePartialUpdateUnit.PartialUpdateUnit partialUpdateUnit) {
    String orderingField = partialUpdateUnit.getOrderingField();
    Comparable currentOrderingVal = HoodieAvroUtils.getNestedFieldValAsString(currentRecord, orderingField, true, false);
    Comparable incomingOrderingVal = HoodieAvroUtils.getNestedFieldValAsString(incomingRecord, orderingField, true, false);
    return Objects.isNull(incomingOrderingVal) || Objects.nonNull(currentOrderingVal) && currentOrderingVal.compareTo(incomingOrderingVal) > 0;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties prop) throws IOException {
    Option<IndexedRecord> recordOption = getInsertValue(schema);
    if (!recordOption.isPresent()) {
      return Option.empty();
    }
    if (multiplePartialUpdateUnit == null) {
      this.multiplePartialUpdateUnit = new MultiplePartialUpdateUnit(this.orderingVal.toString());
    }
    return combineAndGetUpdateValue(currentValue, schema, this.multiplePartialUpdateUnit);
  }

  /**
   * Return true if value equals defaultValue otherwise false.
   */
  public Boolean overwriteField(Object value, Object defaultValue) {
    return value == null;
  }

  public static boolean isMultipleOrderFields(String preCombineField) {
    if (StringUtils.isNullOrEmpty(preCombineField) && preCombineField.split(":").length > 1) {
      return true;
    }
    return false;
  }
}

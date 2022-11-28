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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
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
public interface MultiplePartialUpdateHelper {

  Logger LOG = LogManager.getLogger(MultiplePartialUpdateHelper.class);
  default Option<IndexedRecord> preCombineMultiplePartialUpdate(Option<IndexedRecord> oldValue, Option<IndexedRecord> incomingRecord,  Schema schema, Comparable orderVal, Properties properties)
      throws IOException {
    try {
      //if old value not present (eg: old value is  delete record),just return incoming record
      if (!oldValue.isPresent()) {
        return incomingRecord;
      }
      return combineAndGetUpdateValue(oldValue.get(), incomingRecord, schema,orderVal, properties);
    } catch (IOException e) {
      LOG.error("multiple partial update preCombine failed. " + e);
      throw new HoodieException(e);
    }
  }

  default Option<IndexedRecord> combineAndGetUpdateValue(
      IndexedRecord currentValue, Option<IndexedRecord> incomingRecord, Schema schema, MultiplePartialUpdateUnit multiplePartialUpdateUnit) throws IOException {
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
            .forEach(fieldName -> {
              Object value = finalInsertRecord.get(fieldName);
              if (value != null) {
                resultRecord.put(fieldName, value);
              }
            });
        Object oldOrderingVal = HoodieAvroUtils.getNestedFieldVal(finalInsertRecord, partialUpdateUnit.getOrderingField(), true, false);
        resultRecord.put(partialUpdateUnit.getOrderingField(), oldOrderingVal);
      } else {
        partialUpdateUnit.getColumnNames()
            .stream()
            .filter(name2Field::containsKey)
            .forEach(name -> {
              Schema.Field field = name2Field.get(name);
              setField(resultRecord, (GenericRecord) currentValue, field);
            });

      }
      // If any of the sub-table records is flagged for deletion, delete entire row
      if (isDelete(insertRecord)) {
        deleteFlag[0] = true;
      }
    });

    if (deleteFlag[0]) {
      return Option.empty();
    }
    return Option.of(resultRecord);
  }

  default void overwrite(GenericRecord insertRecord, GenericRecord currentValue, Schema schema, String column) {

  }

  default Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue,Option<IndexedRecord> incomingRecord, Schema schema, Comparable orderingVal) throws IOException {
    try {
      return combineAndGetUpdateValue(currentValue, incomingRecord, schema, new MultiplePartialUpdateUnit(orderingVal.toString()));
    } catch (IOException e) {
      LOG.error("multiple partial update combineAndGetUpdateValue failed. " + e);
      throw new RuntimeException(e);
    }
  }

  default boolean needUseOldRecordToUpdate(GenericRecord incomingRecord, GenericRecord currentRecord, MultiplePartialUpdateUnit.PartialUpdateUnit partialUpdateUnit) {
    Comparable currentOrderingVal = null;
    Comparable incomingOrderingVal = null;
    try {
      String orderingField = partialUpdateUnit.getOrderingField();
      currentOrderingVal = (Comparable) HoodieAvroUtils.getNestedFieldVal(currentRecord, orderingField, true, false);
      incomingOrderingVal = (Comparable) HoodieAvroUtils.getNestedFieldVal(incomingRecord, orderingField, true, false);
      return Objects.isNull(incomingOrderingVal) && Objects.nonNull(currentOrderingVal) || Objects.nonNull(currentOrderingVal) && currentOrderingVal.compareTo(incomingOrderingVal) > 0;
    } catch (Exception e) {
      throw new HoodieException(e);
    }

  }

  default Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Option<IndexedRecord> incomingRecord, Schema schema, Comparable orderingVal, Properties prop)
      throws IOException {
    return combineAndGetUpdateValue(currentValue, incomingRecord, schema, orderingVal);
  }

  default  boolean isMultipleOrderFields(String preCombineField) {
    return !StringUtils.isNullOrEmpty(preCombineField) && preCombineField.split(":").length > 1;
  }

  default boolean isDelete(GenericRecord genericRecord) {
    final String isDeleteKey = HoodieRecord.HOODIE_IS_DELETED;
    // Modify to be compatible with new version Avro.
    // The new version Avro throws for GenericRecord.get if the field name
    // does not exist in the schema.
    if (genericRecord.getSchema().getField(isDeleteKey) == null) {
      return false;
    }
    Object deleteMarker = genericRecord.get(isDeleteKey);
    return (deleteMarker instanceof Boolean && (boolean) deleteMarker);
  }

 default void setField(GenericRecord resultRecord, GenericRecord insertRecord, Schema.Field field) {
    Object value = resultRecord.get(field.name());
    value = field.schema().getType().equals(Schema.Type.STRING) && value != null ? value.toString() : value;
    Object defaultValue = field.defaultVal();
    if (overwriteField(value, defaultValue)) {
      String name = field.name();
      resultRecord.put(name, insertRecord.get(name));
    }
  }

  default Boolean overwriteField(Object value, Object defaultValue) {
    if (JsonProperties.NULL_VALUE.equals(defaultValue)) {
      return value == null;
    }
    return Objects.equals(value, defaultValue);
  }

}

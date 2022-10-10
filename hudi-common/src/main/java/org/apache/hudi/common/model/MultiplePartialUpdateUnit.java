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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * MultipleOrderingVal2ColsInfo
 *  _ts1=999:name1,price1;_ts2=111:name2,price2
 * _ts1:name1,price1=999;_ts2:name2,price2=111
 */
public class MultiplePartialUpdateUnit {
  private List<PartialUpdateUnit> multiplePartialUpdateUnits = Collections.EMPTY_LIST;

  public MultiplePartialUpdateUnit(String multipleUpdateUnitText) {
    this.multiplePartialUpdateUnits =
      Arrays.stream(multipleUpdateUnitText.split(";")).filter(Objects::nonNull)
        .map(PartialUpdateUnit::new).collect(Collectors.toList());
  }

  public List<PartialUpdateUnit> getMultiplePartialUpdateUnits() {
    return multiplePartialUpdateUnits;
  }

  public class PartialUpdateUnit {
    private String orderingField;
    private String orderingValue = "";
    private List<String> columnNames;

    public PartialUpdateUnit(String partialUpdateUnitText) {
      List<String> partialUpdateList = Arrays.asList(partialUpdateUnitText.split(":|,"));
      this.orderingField = partialUpdateList.get(0);
      this.columnNames = partialUpdateList.subList(1, partialUpdateList.size());
    }

    public String getOrderingField() {
      return orderingField;
    }

    public String getOrderingValue() {
      return orderingValue;
    }

    public void setOrderingValue(String value) {
      this.orderingValue = value;
    }

    public List<String> getColumnNames() {
      return columnNames;
    }

    @Override
    public String toString() {
      return String.format("%s=%s:%s", this.orderingField, this.orderingValue, String.join(",", this.columnNames));
    }
  }

  @Override
  public String toString() {
    int len = multiplePartialUpdateUnits.size();
    return len > 1 ? this.multiplePartialUpdateUnits
        .stream()
        .map(PartialUpdateUnit::toString)
        .collect(Collectors.joining(";")) : multiplePartialUpdateUnits.get(0).toString();
  }
}

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * MultipleOrderingVal2ColsInfo
 *  _ts1=999:name1,price1;_ts2=111:name2,price2
 * _ts1:name1,price1=999;_ts2:name2,price2=111
 */
public class MultipleOrderingVal2ColsInfo {
  private List<OrderingVal2ColsInfo> orderingVal2ColsInfoList = new ArrayList<>();

  public MultipleOrderingVal2ColsInfo(String multipleOrderingFieldsWithColsText) {
    for (String orderingFieldWithColsText : multipleOrderingFieldsWithColsText.split(";")) {
      if (orderingFieldWithColsText == null || orderingFieldWithColsText.isEmpty()) {
        continue;
      }
      OrderingVal2ColsInfo orderingVal2ColsInfo = new OrderingVal2ColsInfo(orderingFieldWithColsText);
      orderingVal2ColsInfoList.add(orderingVal2ColsInfo);
    }
  }

  public List<OrderingVal2ColsInfo> getOrderingVal2ColsInfoList() {
    return orderingVal2ColsInfoList;
  }

  public String generateOrderingText() {
    StringBuilder sb = new StringBuilder();
    orderingVal2ColsInfoList.stream().forEach(orderingVal2ColsInfo -> {
      sb.append(orderingVal2ColsInfo.orderingField);
      sb.append("=");
      if (Objects.nonNull(orderingVal2ColsInfo.orderingValue)) {
        sb.append(orderingVal2ColsInfo.orderingValue);
      }
      sb.append(":");
      sb.append(String.join(",", orderingVal2ColsInfo.getColumnNames()));
      sb.append(";");
    });
    sb.deleteCharAt(sb.length() - 1);

    return sb.toString();
  }

  public class OrderingVal2ColsInfo {
    private String orderingField;
    private String orderingValue = "";
    private List<String> columnNames;

    public OrderingVal2ColsInfo(String orderingFieldWithColsText) {
      String[] orderInfo2ColsArr = orderingFieldWithColsText.split(":");
      String[] orderingField2Value = orderInfo2ColsArr[0].split("=");
      String[] columnArr = orderInfo2ColsArr[1].split(",");
      this.orderingField = orderingField2Value[0];
      if (orderingField2Value.length > 1) {
        this.orderingValue = orderingField2Value[1];
      }
      this.columnNames = Arrays.asList(columnArr);
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
  }
}

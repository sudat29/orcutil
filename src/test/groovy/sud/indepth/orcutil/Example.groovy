/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sud.indepth.orcutil

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import sud.indepth.orcutil.annotation.ListField
import sud.indepth.orcutil.annotation.MapField
import sud.indepth.orcutil.annotation.PrimitiveField
import sud.indepth.orcutil.annotation.StructField

/**
 * Sample classes for testing.
 */
class Example {
  static class SimplePerson {
    @PrimitiveField
    private int id
    @PrimitiveField
    private Double salary
    @PrimitiveField
    private String name
    @StructField
    private Country country

    SimplePerson() {}

    void setId(int id) {
      this.id = id
    }

    void setSalary(Double salary) {
      this.salary = salary
    }

    void setName(String name) {
      this.name = name
    }

    void setCountry(Country country) {
      this.country = country
    }
  }

  static class Address {
    @PrimitiveField
    String line1

    @StructField
    City city

    @PrimitiveField(type = PrimitiveObjectInspector.PrimitiveCategory.INT)
    long pinCode
  }

  static class SimpleOrder {
    @PrimitiveField
    private int orderId

    @ListField
    private List<String> itemIds

    void setOrderId(final int orderId) {
      this.orderId = orderId
    }

    void setItemIds(final List<String> itemIds) {
      this.itemIds = itemIds
    }
  }

  static class Person {
    @PrimitiveField
    String name

    @PrimitiveField(key = "emp_id")
    int id

    @PrimitiveField
    Double salary

    @StructField(key = "address")
    Address addrs
  }

  static class Country {
    @PrimitiveField
    String name
  }

  static class City {
    @PrimitiveField
    String name

    @StructField
    Country country
  }

  static class ComplexOrder {
    @PrimitiveField
    private int orderId

    @ListField
    private List<Item> itemIds

    void setOrderId(final int orderId) {
      this.orderId = orderId
    }

    void setItemIds(final List<Item> itemIds) {
      this.itemIds = itemIds
    }
  }

  static class Item {
    @PrimitiveField
    private int itemId

    @PrimitiveField
    private String name

    void setItemId(int itemId) {
      this.itemId = itemId
    }

    void setName(String name) {
      this.name = name
    }
  }

  static class OrderMap {
    @PrimitiveField(type = PrimitiveObjectInspector.PrimitiveCategory.LONG)
    private int orderId

    @MapField(key = "itemMap")
    private Map<String, Item> idVsItem

    void setOrderId(int orderId) {
      this.orderId = orderId
    }

    void setIdVsItem(Map<String, Item> idVsItem) {
      this.idVsItem = idVsItem
    }
  }
}

// End Example.groovy

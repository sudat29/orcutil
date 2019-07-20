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

import spock.lang.Specification

import static groovy.util.GroovyTestCase.assertEquals

/**
 * Spec for testing ORC schema generated.
 */
class OrcSchemaGeneratorSpec extends Specification {
  private OrcSchemaGenerator schemaGenerator = new OrcSchemaGenerator()

  def "testOrcSchemaGenerator"() {
    given:
    String expectedSchema = "struct<name:string,emp_id:int,salary:double,address:struct<line1:string," +
        "city:struct<name:string,country:struct<name:string>>,pinCode:int>>"
    when:
    String actualSchema = schemaGenerator.generateStructTypeDesc(Example.Person.class).toString()
    then:
    assertEquals(expectedSchema, actualSchema)
  }

  def "testOrcListSchema"() {
    given:
//    TypeDescription schema = TypeDescription.createStruct()
//        .addField("list",
//            TypeDescription.createList(TypeDescription.createString()))
    when:
    String actualSchema = schemaGenerator.generateStructTypeDesc(Example.SimpleOrder.class).toString()
    String expectedSchema = "struct<orderId:int,itemIds:array<string>>"
    then:
    assertEquals(expectedSchema, actualSchema)
  }

  def "testOrcComplexListSchema"() {
    given:
    String expectedSchema = "struct<orderId:int,itemIds:array<struct<itemId:int,name:string>>>"
    when:
    String actualSchema = schemaGenerator.generateStructTypeDesc(Example.ComplexOrder.class).toString()
    then:
    assertEquals(expectedSchema, actualSchema)
  }

  def "testOrcStructWithMapSchema"() {
    given:
    String expectedSchema = "struct<orderId:bigint,itemMap:map<string,struct<itemId:int,name:string>>>"
    when:
    String actualSchema = schemaGenerator.generateStructTypeDesc(Example.OrderMap.class).toString()
    then:
    assertEquals(expectedSchema, actualSchema)
  }
}

// End OrcSchemaGeneratorSpec.groovy

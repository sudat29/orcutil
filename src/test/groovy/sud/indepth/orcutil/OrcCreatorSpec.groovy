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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.*
import org.apache.hadoop.hive.serde2.objectinspector.*
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.unitils.reflectionassert.ReflectionAssert
import spock.lang.Specification
import sud.indepth.orcutil.annotation.AnnotationUtil
import sud.indepth.orcutil.annotation.ListField
import sud.indepth.orcutil.annotation.MapField
import sud.indepth.orcutil.annotation.PrimitiveField
import sud.indepth.orcutil.annotation.StructField
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl

import java.lang.reflect.Field

import static groovy.util.GroovyTestCase.assertEquals

/**
 * Spec for testing ORC schema creator.
 */
class OrcCreatorSpec extends Specification {
  private OrcCreator orcCreator = new OrcCreator()
  private OrcSchemaGenerator schemaGenerator = new OrcSchemaGenerator()

  def "testSimpleOrcStructCreation"() {
    given:
    Example.SimplePerson p = new Example.SimplePerson()
    p.setId(10)
    p.setName("John")
    p.setSalary(70000d)
    Example.Country country = new Example.Country()
    country.name = "India"
    p.setCountry(country)

    when:
    OrcStruct orcStruct = orcCreator.createOrcStruct(p)

    then:
    assertEquals(orcStruct.getNumFields(), 4)
    assertEquals(orcStruct.toString(), "{10, 70000.0, John, {India}}")
  }

  def "testOrcStructWithListCreation"() {
    given:
    Example.SimpleOrder order = new Example.SimpleOrder()
    order.setOrderId(10)
    order.setItemIds(Arrays.asList("item1", "item2"))
    when:
    OrcStruct orcStruct = orcCreator.createOrcStruct(order)
    then:
    assertEquals(orcStruct.getNumFields(), 2)
    assertEquals(orcStruct.toString(), "{10, [item1, item2]}")
  }

  def "testOrcStructWithComplexListCreation"() {
    given:
    Example.ComplexOrder order = new Example.ComplexOrder()
    order.setOrderId(10)
    Example.Item item1 = new Example.Item()
    item1.setName("item1")
    item1.setItemId(101)
    Example.Item item2 = new Example.Item()
    item2.setName("item2")
    item2.setItemId(102)
    order.setItemIds(Arrays.asList(item1, item2))
    when:
    OrcStruct orcStruct = orcCreator.createOrcStruct(order)
    then:
    assertEquals(orcStruct.getNumFields(), 2)
    assertEquals(orcStruct.toString(), "{10, [{101, item1}, {102, item2}]}")
  }

  def "testOrcStructWithListReader"() {
    given:
    Example.ComplexOrder order = new Example.ComplexOrder()
    order.setOrderId(10)
    Example.Item item1 = new Example.Item()
    item1.setName("item1")
    item1.setItemId(101)
    Example.Item item2 = new Example.Item()
    item2.setName("item2")
    item2.setItemId(102)
    order.setItemIds(Arrays.asList(item1, item2))
    List<Example.ComplexOrder> expectedOrders = new ArrayList<>()
    expectedOrders.add(order)
    expectedOrders.add(order)

    when:
    OrcStruct orcStruct = orcCreator.createOrcStruct(order)
    Configuration conf = new Configuration()
    Path path = new Path("complex_order.orc")
    ObjectInspector objectInspector = createObjectInspector(Example.ComplexOrder.class)
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf).inspector(objectInspector)
    Writer writer = OrcFile.createWriter(path, writerOptions)
    writer.addRow(orcStruct)
    writer.addRow(orcStruct)
    writer.close()

    then:
    OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf)
    Reader reader = OrcFile.createReader(path, readerOptions)
    RecordReader recordReader = reader.rows()
    ObjectInspector inspector = reader.getObjectInspector()
    ((SettableStructObjectInspector) inspector).getAllStructFieldRefs()

    Object row
    List<Example.ComplexOrder> actualOrders = new ArrayList<>()
    while (recordReader.hasNext()) {
      row = recordReader.next(row);
      actualOrders.add((Example.ComplexOrder) populateEntityByType(null, Example.ComplexOrder.class, null, inspector, row))
    }
    ReflectionAssert.assertReflectionEquals(expectedOrders, actualOrders)

    cleanup:
    FileSystem fs = FileSystem.getLocal(conf)
    fs.delete(path, true)
  }

  def "testOrcStructWithMapReader"() {
    given:
    Example.OrderMap order = new Example.OrderMap()
    order.setOrderId(10)
    Example.Item item1 = new Example.Item()
    item1.setName("item1")
    item1.setItemId(101)
    Example.Item item2 = new Example.Item()
    item2.setName("item2")
    item2.setItemId(102)
    Map<String, Example.Item> itemMap = new HashMap<>()
    itemMap.put("item1", item1)
    itemMap.put("item2", item2)
    order.setIdVsItem(itemMap)
    List<Example.OrderMap> expectedOrders = new ArrayList<>()
    expectedOrders.add(order)
    when:
    OrcStruct orcStruct = orcCreator.createOrcStruct(order)
    Configuration conf = new Configuration()
    Path path = new Path("order_map.orc")
    ObjectInspector objectInspector = createObjectInspector(Example.OrderMap.class)
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf).inspector(objectInspector)
    Writer writer = OrcFile.createWriter(path, writerOptions)
    writer.addRow(orcStruct)
    writer.close()

    then:
    OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf)
    Reader reader = OrcFile.createReader(path, readerOptions)
    RecordReader recordReader = reader.rows()
    ObjectInspector inspector = reader.getObjectInspector()
    ((SettableStructObjectInspector) inspector).getAllStructFieldRefs()

    Object row
    List<Example.OrderMap> actualOrders = new ArrayList<>()
    while (recordReader.hasNext()) {
      row = recordReader.next(row);
      actualOrders.add((Example.OrderMap) populateEntityByType(null, Example.OrderMap.class, null, inspector, row))
    }
    ReflectionAssert.assertReflectionEquals(expectedOrders, actualOrders)

    cleanup:
    FileSystem fs = FileSystem.getLocal(conf)
    fs.delete(path, true)
  }

  def "testComplexOrcStructCreation"() {
    given:
    Example.Person p = new Example.Person()
    p.id = 10
    p.name = "John"
    p.salary = 70000d
    Example.Address address = new Example.Address()
    address.line1 = "Lane no 1"
    Example.City city = new Example.City()
    city.name = "Delhi"
    Example.Country country = new Example.Country()
    country.name = "India"
    city.country = country
    address.city = city
    p.addrs = address

    when:
    OrcStruct orcStruct = orcCreator.createOrcStruct(p)

    then:
    assertEquals(orcStruct.getNumFields(), 4)
    assertEquals(orcStruct.toString(), "{John, 10, 70000.0, {Lane no 1, {Delhi, {India}}, 0}}")
  }

  def "testSimpleOrcStructReader"() {
    given:
    Example.SimplePerson p = new Example.SimplePerson()
    p.setId(10)
    p.setName("John")
    p.setSalary(70000d)
    Example.Country country = new Example.Country()
    country.name = "India"
    p.setCountry(country)
    List<Example.SimplePerson> expectedSimplePeople = new ArrayList<>()
    expectedSimplePeople.add(p)

    when:
    OrcStruct orcStruct = orcCreator.createOrcStruct(p)
    Configuration conf = new Configuration()
    Path path = new Path("simple_person.orc")
    ObjectInspector objectInspector = createObjectInspector(Example.SimplePerson.class)
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf).inspector(objectInspector)
    Writer writer = OrcFile.createWriter(path, writerOptions)
    writer.addRow(orcStruct)
    writer.close()

    then:
    OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf)
    Reader reader = OrcFile.createReader(path, readerOptions)
    RecordReader recordReader = reader.rows()
    ObjectInspector inspector = reader.getObjectInspector()
    ((SettableStructObjectInspector) inspector).getAllStructFieldRefs()

    Object row
    List<Example.SimplePerson> simplePeople = new ArrayList<>()
    while (recordReader.hasNext()) {
      row = recordReader.next(row);
      simplePeople.add((Example.SimplePerson) populateEntityByType(null, Example.SimplePerson.class, null, inspector, row))
    }
    ReflectionAssert.assertReflectionEquals(expectedSimplePeople, simplePeople)

    cleanup:
    FileSystem fs = FileSystem.getLocal(conf)
    fs.delete(path, true)
  }

  def "testComplexOrcStructReader"() throws IllegalAccessException, IOException, InstantiationException {
    given:
    Example.Person p = new Example.Person()
    p.id = 10
    p.name = "John"
    p.salary = 70000d
    Example.Address address = new Example.Address()
    address.line1 = "Lane no 1"
    Example.City city = new Example.City()
    city.name = "Delhi"
    Example.Country country = new Example.Country()
    country.name = "India"
    city.country = country
    address.city = city
    address.pinCode = 300201
    p.addrs = address
    List<Example.Person> expectedSimplePeople = new ArrayList<>()
    expectedSimplePeople.add(p)

    when:
    OrcStruct orcStruct = orcCreator.createOrcStruct(p)
    Configuration conf = new Configuration()
    Path path = new Path("complex_person.orc")
    ObjectInspector objectInspector = createObjectInspector(Example.Person.class)
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf).inspector(objectInspector)
    Writer writer = OrcFile.createWriter(path, writerOptions)
    writer.addRow(orcStruct)
    writer.close()

    then:
    OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf)
    Reader reader = OrcFile.createReader(path, readerOptions)
    RecordReader recordReader = reader.rows()
    ObjectInspector inspector = reader.getObjectInspector()
    ((SettableStructObjectInspector) inspector).getAllStructFieldRefs()

    Object row
    List<Example.Person> simplePeople = new ArrayList<>()
    while (recordReader.hasNext()) {
      row = recordReader.next(row);
      simplePeople.add((Example.Person) populateEntityByType(null, Example.Person.class, null, inspector, row))
    }
    ReflectionAssert.assertReflectionEquals(simplePeople, expectedSimplePeople)

    cleanup:
    FileSystem fs = FileSystem.getLocal(conf)
    fs.delete(path, true)
  }

  private <T, U> Object populateEntityByType(String key, Class<T> entityClass, Class<U> parentClass,
                                             ObjectInspector inspector, Object value)
      throws IllegalAccessException, InstantiationException {
    switch (inspector.getCategory()) {
      case ObjectInspector.Category.PRIMITIVE:
        PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) inspector
        switch (primitiveObjectInspector.getPrimitiveCategory()) {
          case PrimitiveObjectInspector.PrimitiveCategory.INT:
            return Integer.class.cast(((IntWritable) value).get())
          case PrimitiveObjectInspector.PrimitiveCategory.DOUBLE:
            return Double.class.cast(((DoubleWritable) value).get())
          case PrimitiveObjectInspector.PrimitiveCategory.STRING:
            return String.class.cast(value.toString())
          case PrimitiveObjectInspector.PrimitiveCategory.LONG:
            return Long.class.cast(((LongWritable) value).get())
          default:
            throw new UnsupportedOperationException("Primitive category["
                + primitiveObjectInspector.getPrimitiveCategory() + "] not supported yet.")
        }
      case ObjectInspector.Category.STRUCT:
        T entity = entityClass.newInstance()
        SettableStructObjectInspector structObjectInspector = ((SettableStructObjectInspector) inspector)
        assert value.getClass() == OrcStruct.class:
            "Expected[" + OrcStruct.class + "] but found record[" + value.getClass() + "]"
        List<? extends org.apache.hadoop.hive.serde2.objectinspector.StructField> structFieldRefs = structObjectInspector.getAllStructFieldRefs()
        List<Object> fieldValues = structObjectInspector.getStructFieldsDataAsList(value)
        for (int i = 0; i < structFieldRefs.size(); i++) {
          org.apache.hadoop.hive.serde2.objectinspector.StructField structField = structFieldRefs.get(i)
          ObjectInspector fieldObjectInspector = structField.getFieldObjectInspector()
          String fieldKey = structField.getFieldName()
          Field field = findField(fieldKey, entity.getClass())
          field.setAccessible(true)
          Object fieldValue =
              populateEntityByType(fieldKey, field.getType(), entityClass, fieldObjectInspector, fieldValues.get(i))
          setFieldValue(entity, field, fieldValue)
        }
        return entity
      case ObjectInspector.Category.LIST:
        List list = new ArrayList()
        List inputList = (List) value
        ListObjectInspector listObjectInspector = ((ListObjectInspector) inspector)
        ObjectInspector elementObjectInspector = listObjectInspector.getListElementObjectInspector()
        Field listField = findField(key, parentClass)
        Class<?> elementClass = (Class<?>) ((ParameterizedTypeImpl) listField.getGenericType()).getActualTypeArguments()[0]
        for (Object obj : inputList) {
          list.add(populateEntityByType(null, elementClass, entityClass, elementObjectInspector, obj))
        }
        return list
      case ObjectInspector.Category.MAP:
        Map map = new HashMap()
        Map inputMap = (Map) value
        MapObjectInspector mapObjectInspector = ((MapObjectInspector) inspector)
        ObjectInspector keyObjectInspector = mapObjectInspector.getMapKeyObjectInspector()
        ObjectInspector valueObjectInspector = mapObjectInspector.getMapValueObjectInspector()
        Field mapField = findField(key, parentClass)
        Class<?> keyClass = (Class<?>) ((ParameterizedTypeImpl) mapField.getGenericType()).getActualTypeArguments()[0]
        Class<?> valueClass = (Class<?>) ((ParameterizedTypeImpl) mapField.getGenericType()).getActualTypeArguments()[1]
        for (Object obj : inputMap.entrySet()) {
          Map.Entry entry = (Map.Entry) obj
          Object keyObj = populateEntityByType(null, keyClass, entityClass, keyObjectInspector, entry.getKey())
          Object valueObj = populateEntityByType(null, valueClass, entityClass, valueObjectInspector, entry.getValue())
          map.put(keyObj, valueObj)
        }
        return map
      default:
        throw new UnsupportedOperationException("Category [" + inspector.getCategory() + "] not supported yet.")
    }
  }

  static Field findField(String key, Class<?> aClass) {
    return aClass.getDeclaredFields()
        .find { field ->
      ((field.isAnnotationPresent(PrimitiveField.class) && key == AnnotationUtil.getPrimitiveFieldKey(field))
          || (field.isAnnotationPresent(StructField.class) && key == AnnotationUtil.getStructFieldKey(field))
          || (field.isAnnotationPresent(ListField.class) && key == AnnotationUtil.getListFieldKey(field))
          || (field.isAnnotationPresent(MapField.class) && key == AnnotationUtil.getMapFieldKey(field)))
    }
  }

  static <T> void setFieldValue(T entity, Field field, Object fieldValue) throws IllegalAccessException {
    if (field.getType().isAssignableFrom(fieldValue.getClass())) {
      field.set(entity, fieldValue)
    } else {
      if (field.getType().isPrimitive()) {
        String fieldTypeStr = field.getType().toString()
        switch (fieldTypeStr) {
          case "int":
            field.set(entity, Integer.valueOf(fieldValue.toString()))
            break
          case "long":
            field.set(entity, Long.valueOf(fieldValue.toString()))
            break
          default:
            throw new UnsupportedOperationException("Casting of primitive type[" + fieldTypeStr + "] not supported.")
        }
      }
    }
  }

  private ObjectInspector createObjectInspector(Class<?> aClass) {
    String schema = schemaGenerator.generateStructTypeDesc(aClass).toString()
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schema)
    return OrcStruct.createObjectInspector(typeInfo)
  }

}

// End OrcCreatorSpec.groovy

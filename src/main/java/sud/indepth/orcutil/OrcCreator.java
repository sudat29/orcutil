/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
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

package sud.indepth.orcutil;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.orc.TypeDescription;

import sud.indepth.orcutil.annotation.ListField;
import sud.indepth.orcutil.annotation.MapField;
import sud.indepth.orcutil.annotation.PrimitiveField;
import sud.indepth.orcutil.annotation.StructField;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class OrcCreator {
  private OrcSchemaGenerator schemaGenerator = new OrcSchemaGenerator();

  /**
   * Given a user defied object, generates corresponding {@link OrcStruct}
   * based on annotations ({@code sud.indepth.orcutil.annotation.*}) marked fields.
   *
   * @param obj
   * @return
   * @throws IllegalAccessException
   */
  public OrcStruct createOrcStruct(Object obj) throws IllegalAccessException {
    Class<?> clazz = obj.getClass();
    TypeDescription typeDescription = schemaGenerator.generateStructTypeDesc(clazz);
    String schema = typeDescription.toString();
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schema);
    SettableStructObjectInspector objectInspector
        = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
    OrcStruct orcStruct = (OrcStruct) objectInspector.create();
    List<? extends org.apache.hadoop.hive.serde2.objectinspector.StructField> structFields = objectInspector.getAllStructFieldRefs();
    orcStruct.setNumFields(structFields.size());
    List<Object> fieldValues = traverseFields(obj);
    assert structFields.size() == fieldValues.size() : "Schema field size and object field size do not match.";
    for (int i = 0; i < fieldValues.size(); i++) {
      Object writableValue = createWritableValue(
          typeDescription.getChildren().get(i),
          structFields.get(i).getFieldObjectInspector(),
          fieldValues.get(i));
      objectInspector.setStructFieldData(orcStruct, structFields.get(i), writableValue);
    }
    return orcStruct;
  }

  @SuppressWarnings("unchecked")
  private Object createWritableValue(
      TypeDescription typeDescription,
      ObjectInspector fieldObjectInspector,
      Object value) throws IllegalAccessException {
    Object writableValue;
    switch (fieldObjectInspector.getCategory()) {
    case PRIMITIVE:
      PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) fieldObjectInspector;
      switch (primitiveObjectInspector.getPrimitiveCategory()) {
      case INT:
        writableValue = ((WritableIntObjectInspector) primitiveObjectInspector).create(castToInt(value));
        break;
      case DOUBLE:
        writableValue = ((WritableDoubleObjectInspector) primitiveObjectInspector).create((double) value);
        break;
      case STRING:
        writableValue = ((WritableStringObjectInspector) primitiveObjectInspector).create((String) value);
        break;
      case LONG:
        writableValue = ((WritableLongObjectInspector) primitiveObjectInspector).create(castToLong(value));
        break;
      default:
        throw new UnsupportedOperationException("Primitive category["
            + primitiveObjectInspector.getPrimitiveCategory() + "] not supported yet.");
      }
      break;
    case STRUCT:
      writableValue = createOrcStruct(value);
      break;
    case LIST:
      ListObjectInspector listObjectInspector = ((ListObjectInspector) fieldObjectInspector);
      writableValue = createOrcList(typeDescription, listObjectInspector, (List<Object>) value);
      break;
    case MAP:
      MapObjectInspector mapObjectInspector = ((MapObjectInspector) fieldObjectInspector);
      writableValue = createOrcMap(typeDescription, mapObjectInspector, (Map<Object, Object>) value);
      break;
    default:
      throw new UnsupportedOperationException("Category ["
          + fieldObjectInspector.getCategory() + "] not supported yet.");
    }
    return writableValue;
  }

  @SuppressWarnings("unchecked")
  private Map<Object, Object> createOrcMap(
      TypeDescription typeDescription,
      MapObjectInspector mapObjectInspector,
      Map<Object, Object> map)
      throws IllegalAccessException {
    ObjectInspector keyObjectInspector = mapObjectInspector.getMapKeyObjectInspector();
    ObjectInspector valueObjectInspector = mapObjectInspector.getMapValueObjectInspector();
    StandardMapObjectInspector standardMapObjectInspector =
        ObjectInspectorFactory.getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
    Map<Object, Object> orcMap = (Map<Object, Object>) standardMapObjectInspector.create();
    TypeDescription keyDescription = typeDescription.getChildren().get(0);
    TypeDescription valueDescription = typeDescription.getChildren().get(1);
    for (Object obj: map.entrySet()) {
      Map.Entry entry = (Map.Entry) obj;
      Object keyObj = createWritableValue(keyDescription, keyObjectInspector, entry.getKey());
      Object valueObj = createWritableValue(valueDescription, valueObjectInspector, entry.getValue());
      orcMap.put(keyObj, valueObj);
    }
    return orcMap;
  }

  private int castToInt(Object obj) {
    if (obj.getClass().isAssignableFrom(Integer.class)) {
      return (int) obj;
    } else if (obj.getClass().isAssignableFrom(Long.class)) {
      return ((Long) obj).intValue();
    } else if (obj.getClass().isAssignableFrom(Double.class)) {
      return ((Double) obj).intValue();
    } else {
      return (int) obj;
    }
  }

  private long castToLong(Object obj) {
    if (obj.getClass().isAssignableFrom(Long.class)) {
      return (long) obj;
    } else if (obj.getClass().isAssignableFrom(Integer.class)) {
      return ((Integer) obj).longValue();
    } else if (obj.getClass().isAssignableFrom(Double.class)) {
      return ((Double) obj).longValue();
    } else {
      return (long) obj;
    }
  }

  private List<Object> traverseFields(Object obj) throws IllegalAccessException {
    Class<?> aClass = obj.getClass();
    List<Object> orcFields = new ArrayList<>();
    for (Field field : aClass.getDeclaredFields()) {
      field.setAccessible(true);
      field.getAnnotations();
      if (field.isAnnotationPresent(PrimitiveField.class)
          || field.isAnnotationPresent(StructField.class)
          || field.isAnnotationPresent(ListField.class)
          || field.isAnnotationPresent(MapField.class)) {
        orcFields.add(field.get(obj));
      }
    }
    return orcFields;
  }

  @SuppressWarnings("unchecked")
  private List<Object> createOrcList(
      TypeDescription typeDescription,
      ListObjectInspector listObjectInspector,
      List<Object> list)
      throws IllegalAccessException {
    ObjectInspector elementObjectInspector = listObjectInspector.getListElementObjectInspector();
    StandardListObjectInspector standardListObjectInspector =
        ObjectInspectorFactory.getStandardListObjectInspector(elementObjectInspector);
    List<Object> orcList = (List<Object>) standardListObjectInspector.create(0);
    TypeDescription elementDescription = typeDescription.getChildren().get(0);
    for (Object obj: list) {
      Object value = createWritableValue(elementDescription, elementObjectInspector, obj);
      orcList.add(value);
    }
    return orcList;
  }

}

// End OrcCreator.java

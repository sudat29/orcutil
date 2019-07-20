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

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.orc.TypeDescription;

import sud.indepth.orcutil.annotation.AnnotationUtil;
import sud.indepth.orcutil.annotation.ListField;
import sud.indepth.orcutil.annotation.MapField;
import sud.indepth.orcutil.annotation.PrimitiveField;
import sud.indepth.orcutil.annotation.StructField;
import sud.indepth.orcutil.exception.DuplicateFieldKeyException;
import sud.indepth.orcutil.exception.NonPrimitiveFieldException;
import sud.indepth.orcutil.exception.SchemaGenerationException;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Deduces ORC schema based on class design.
 */
public class OrcSchemaGenerator {
  /**
   * Generates ORC schema for a custom class. It looks
   * for below annotations to generate schema.
   * <li>{@link sud.indepth.orcutil.annotation.PrimitiveField}</li>
   * <li>{@link sud.indepth.orcutil.annotation.ListField}</li>
   * <li>{@link sud.indepth.orcutil.annotation.MapField}</li>
   * <li>{@link sud.indepth.orcutil.annotation.StructField}</li>
   *
   * <p><i>Note: </i> Does not support recursive type hierarchy like:
   * <pre>
   *   class Person {
   *     &#64;StructField
   *     Person manager;
   *   }
   * </pre>
   * </p>
   *
   * @param aClass - input Class for schema generation
   * @return ORC schema
   */
  public TypeDescription generateStructTypeDesc(Class<?> aClass) {
    TypeDescription structTypeDescription = TypeDescription.createStruct();
    generateOrcSchema(aClass).forEach(structTypeDescription::addField);
    return structTypeDescription;
  }

  private PrimitiveObjectInspectorUtils.PrimitiveTypeEntry getFieldType(Field field) {
    PrimitiveObjectInspector.PrimitiveCategory type = field.getAnnotation(PrimitiveField.class).type();
    if (type == PrimitiveObjectInspector.PrimitiveCategory.UNKNOWN) {
      return PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveJava(field.getType());
    } else {
      return PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveCategory(type);
    }
  }

  private Map<String, TypeDescription> generateOrcSchema(Class<?> aClass) {
    Map<String, TypeDescription> typeDescriptions = new LinkedHashMap<>();
    Set<String> fields = new HashSet<>();
    for (Field field : aClass.getDeclaredFields()) {
      field.setAccessible(true);
      if (field.isAnnotationPresent(PrimitiveField.class)) {
        String key = AnnotationUtil.getPrimitiveFieldKey(field);
        validateKey(fields, key);
        PrimitiveObjectInspectorUtils.PrimitiveTypeEntry keyTypeEntry = getFieldType(field);
        if (keyTypeEntry == null) {
          throw new NonPrimitiveFieldException("Key[" + key + "] of type[" + field.getType() + "] is non primitive.");
        }
        fields.add(key);
        TypeDescription primitiveDescription = getPrimitiveTypeDescription(keyTypeEntry);
        typeDescriptions.put(key, primitiveDescription);
      } else if (field.isAnnotationPresent(StructField.class)) {
        String key = AnnotationUtil.getStructFieldKey(field);
        validateKey(fields, key);
        TypeDescription structTypeDescription = TypeDescription.createStruct();
        Map<String, TypeDescription> fieldTypeDescriptions = generateOrcSchema(field.getType());
        fieldTypeDescriptions.forEach(structTypeDescription::addField);
        typeDescriptions.put(key, structTypeDescription);
        fields.add(key);
      } else if (field.isAnnotationPresent(ListField.class)) {
        String key = AnnotationUtil.getListFieldKey(field);
        validateKey(fields, key);
        Class<?> elementClass = (Class<?>) ((ParameterizedTypeImpl)field.getGenericType()).getActualTypeArguments()[0];
        TypeDescription listTypeDescription = TypeDescription.createList(getElementSchema(elementClass));
        typeDescriptions.put(key, listTypeDescription);
        fields.add(key);
      } else if (field.isAnnotationPresent(MapField.class)) {
        String key = AnnotationUtil.getMapFieldKey(field);
        validateKey(fields, key);
        Class<?> keyClass = (Class<?>) ((ParameterizedTypeImpl)field.getGenericType()).getActualTypeArguments()[0];
        Class<?> valueClass = (Class<?>) ((ParameterizedTypeImpl)field.getGenericType()).getActualTypeArguments()[1];
        TypeDescription keyTypeDescription = getElementSchema(keyClass);
        TypeDescription valueTypeDescription = getElementSchema(valueClass);
        TypeDescription mapTypeDescription = TypeDescription.createMap(keyTypeDescription, valueTypeDescription);
        typeDescriptions.put(key, mapTypeDescription);
        fields.add(key);
      }
    }
    return typeDescriptions;
  }

  private TypeDescription getElementSchema(Class<?> elementClass) {
    if (PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveJava(elementClass) != null) {
      PrimitiveObjectInspectorUtils.PrimitiveTypeEntry primitiveTypeEntry =
          PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveJava(elementClass);
      return getPrimitiveTypeDescription(primitiveTypeEntry);
    } else {
      TypeDescription structTypeDescription = TypeDescription.createStruct();
      generateOrcSchema(elementClass).forEach(structTypeDescription::addField);
      return structTypeDescription;
    }
  }

  private TypeDescription getPrimitiveTypeDescription(PrimitiveObjectInspectorUtils.PrimitiveTypeEntry keyTypeEntry) {
    TypeDescription primitiveDescription;
    switch (keyTypeEntry.primitiveCategory) {
    case INT:
      primitiveDescription = TypeDescription.createInt();
      break;
    case DOUBLE:
      primitiveDescription = TypeDescription.createDouble();
      break;
    case STRING:
      primitiveDescription = TypeDescription.createString();
      break;
    case LONG:
      primitiveDescription = TypeDescription.createLong();
      break;
    case CHAR:
      primitiveDescription = TypeDescription.createChar();
      break;
    case BYTE:
      primitiveDescription = TypeDescription.createByte();
      break;
    case DATE:
      primitiveDescription = TypeDescription.createDate();
      break;
    case FLOAT:
      primitiveDescription = TypeDescription.createFloat();
      break;
    case SHORT:
      primitiveDescription = TypeDescription.createShort();
      break;
    case BOOLEAN:
      primitiveDescription = TypeDescription.createBoolean();
      break;
    case TIMESTAMP:
      primitiveDescription = TypeDescription.createTimestamp();
      break;
    case VARCHAR:
      primitiveDescription = TypeDescription.createVarchar();
      break;
    case BINARY:
      primitiveDescription = TypeDescription.createBinary();
      break;
    default:
      throw new SchemaGenerationException("Primitive type "
          + keyTypeEntry.primitiveCategory + " not supported yet.");
    }
    return primitiveDescription;
  }

  private void validateKey(Set<String> fields, String key) {
    if (fields.contains(key)) {
      throw new DuplicateFieldKeyException("Key[" + key + "] already exists in KeySet" + fields);
    }
  }

}

// End OrcSchemaGenerator.java

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.management.impl.view;

import org.apache.activemq.artemis.core.management.impl.view.predicate.PredicateFilterPart;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.core.management.impl.view.predicate.ActiveMQFilterPredicate;
import org.apache.activemq.artemis.json.JsonArray;
import org.apache.activemq.artemis.json.JsonArrayBuilder;
import org.apache.activemq.artemis.json.JsonObject;
import org.apache.activemq.artemis.json.JsonObjectBuilder;
import org.apache.activemq.artemis.json.JsonValue;
import org.apache.activemq.artemis.utils.JsonLoader;

public abstract class ActiveMQAbstractView<T, V extends PredicateFilterPart<T>> {

   // use this for values which couldn't be retrieved (e.g. an exception was thrown)
   protected static final String N_A = "n/a";

   private static final String FILTER_ARRAY_FIELD = "searchFilters";

   private static final String FILTER_FIELD = "field";

   private static final String FILTER_OPERATION = "operation";

   private static final String FILTER_VALUE = "value";

   private static final String SORT_ORDER = "sortOrder";

   private static final String ASCENDING = "asc";

   @Deprecated(forRemoval = true)
   private static final String SORT_COLUMN = "sortColumn";

   private static final String SORT_FIELD = "sortField";

   private static final JsonObject DEFAULT_FILTER = JsonUtil.toJsonObject(Map.of(FILTER_FIELD, "", FILTER_OPERATION, "", FILTER_VALUE, ""));

   protected Collection<T> collection;

   protected ActiveMQFilterPredicate<T, V> predicate;

   protected String sortField;

   protected String sortOrder;

   public ActiveMQAbstractView() {
      this.sortField = getDefaultOrderColumn();
      this.sortOrder = ASCENDING;
   }

   public void setCollection(Collection<T> collection) {
      this.collection = collection;
   }

   public List<T> filter() {
      List<T> collect = collection.stream().filter(getPredicate()).collect(Collectors.toList());
      return collect;
   }

   public String getResultsAsJson(int page, int pageSize) {
      JsonObjectBuilder obj = JsonLoader.createObjectBuilder();
      JsonArrayBuilder array = JsonLoader.createArrayBuilder();
      collection = collection.stream().filter(getPredicate()).collect(Collectors.toList());
      for (T element : getPagedResult(page, pageSize)) {
         JsonObjectBuilder jsonObjectBuilder = toJson(element);
         //toJson() may return a null
         if (jsonObjectBuilder != null) {
            array.add(jsonObjectBuilder);
         }
      }
      obj.add("data", array);
      obj.add("count", collection.size());
      return obj.build().toString();
   }

   public String AsJson(int page, int pageSize) {
      JsonObjectBuilder obj = JsonLoader.createObjectBuilder();
      JsonArrayBuilder array = JsonLoader.createArrayBuilder();
      for (T element : getPagedResult(page, pageSize)) {
         JsonObjectBuilder jsonObjectBuilder = toJson(element);
         //toJson() may return a null
         if (jsonObjectBuilder != null) {
            array.add(jsonObjectBuilder);
         }
      }
      obj.add("data", array);
      obj.add("count", collection.size());
      return obj.build().toString();
   }

   public Map<String, Object> getResultsAsData(int page, int pageSize) {
      collection = collection.stream().filter(getPredicate()).collect(Collectors.toList());
      return asData(page, pageSize);
   }

   /**
    * Structured equivalent of {@link #AsJson(int, int)}. Field values keep their type (numbers, booleans, strings) and
    * dates are returned as epoch milliseconds, so management clients (e.g. Jolokia) can serialize the result natively
    * instead of receiving JSON inside a string.
    * <p>
    * The rows are returned as an array rather than a {@code List} because core management messages serialize arrays
    * but not collections.
    *
    * @return a map with the keys {@code data} (an array of maps, one per element) and {@code count}
    */
   public Map<String, Object> asData(int page, int pageSize) {
      List<Map<String, Object>> data = new ArrayList<>();
      for (T element : getPagedResult(page, pageSize)) {
         Map<String, Object> map = toMap(element);
         //toMap() may return a null
         if (map != null) {
            data.add(map);
         }
      }
      Map<String, Object> result = new LinkedHashMap<>();
      result.put("data", data.toArray(new Map[0]));
      result.put("count", collection.size());
      return result;
   }

   /**
    * Builds the structured representation of an element from {@link #getField(Object, String)} for each of
    * {@link #getDataFields()}. Views override this to return {@code null} for elements which should be skipped, like
    * {@link #toJson(Object)} does.
    */
   public Map<String, Object> toMap(T obj) {
      Map<String, Object> map = new LinkedHashMap<>();
      for (String field : getDataFields()) {
         map.put(field, toDataValue(getField(obj, field)));
      }
      return map;
   }

   public List<T> getPagedResult(int page, int pageSize) {
      if (collection == null || collection.isEmpty()) {
         return List.of();
      }

      List<T> collectionList = new ArrayList<>(collection);

      //pre-compute fields once per element
      Map<Object, Object> fieldCache = new IdentityHashMap<>(collectionList.size());
      for (T item : collectionList) {
         if (item != null) {
            try {
               Object fieldValue = getField(item, sortField);
               fieldCache.put(item, fieldValue);
            } catch (Exception e) {
               //swallow exception and continue
            }
         }
      }

      boolean sortOrderAscending = sortOrder.equalsIgnoreCase(ASCENDING);

      Comparator<T> cachedComparator = (left, right) -> {
         Object leftValue = fieldCache.get(left);
         Object rightValue = fieldCache.get(right);

         if (leftValue == rightValue) {
            return 0;
         }
         // push nulls to bottom of the list
         if (leftValue == null) {
            return 1;
         }
         if (rightValue == null) {
            return -1;
         }

         return sortOrderAscending ? compareValues(leftValue, rightValue) : compareValues(rightValue, leftValue);
      };

      collectionList.sort(cachedComparator);

      if (page == -1 || pageSize == -1) {
         return Collections.unmodifiableList(collectionList);
      }

      int start = (page - 1) * pageSize;
      int size = collectionList.size();
      if (start >= size || start < 0) {
         return List.of();
      }
      int end = Math.min(page * pageSize, size);

      return Collections.unmodifiableList(collectionList.subList(start, end));
   }

   @SuppressWarnings("unchecked")
   private static int compareValues(Object leftValue, Object rightValue) {
      Class<?> leftClass = leftValue.getClass();
      Class<?> rightClass = rightValue.getClass();
      if (leftClass == rightClass) {
         if (leftValue instanceof Comparable l && rightValue instanceof Comparable) {
            return l.compareTo(rightValue);
         }
         return leftValue.toString().compareTo(rightValue.toString());
      }
      // sort different classes separately by class name
      return leftClass.getName().compareTo(rightClass.getName());
   }

   public Predicate<T> getPredicate() {
      return predicate;
   }

   abstract Object getField(T t, String fieldName);

   public void setOptions(String options) {
      JsonObject json;
      if (options == null || options.isBlank()) {
         json = DEFAULT_FILTER;
      } else {
         json = JsonUtil.readJsonObject(options);
      }
      if (predicate != null) {
         predicate.addFilterParts(createFilterPredicates(json));
         if (json.containsKey(SORT_ORDER)) {
            if (json.containsKey(SORT_FIELD)) {
               this.sortField = json.getString(SORT_FIELD);
            } else if (json.containsKey(SORT_COLUMN)) {
               this.sortField = json.getString(SORT_COLUMN);
            }
            this.sortOrder = json.getString(SORT_ORDER);
         }
      }
   }

   private List<V> createFilterPredicates(JsonObject json) {
      ArrayList<V> predicates = new ArrayList<>();
      JsonArray jsonArray = json.getJsonArray(FILTER_ARRAY_FIELD);
      if (jsonArray == null) {
         predicates.add(predicate.createFilterPart(json.getString(FILTER_FIELD), json.getString(FILTER_OPERATION), json.getString(FILTER_VALUE)));
      } else {
         for (JsonValue jsonValue : jsonArray) {
            JsonObject jsonObject = (JsonObject) jsonValue;
            predicates.add(predicate.createFilterPart(jsonObject.getString(FILTER_FIELD), jsonObject.getString(FILTER_OPERATION), jsonObject.getString(FILTER_VALUE)));
         }
      }
      return predicates;
   }

   public abstract Class getClassT();

   public abstract JsonObjectBuilder toJson(T obj);

   public abstract String getDefaultOrderColumn();

   /**
    * {@return the names of the fields returned by {@link #toMap(Object)}; the same fields {@link #toJson(Object)}
    * returns}
    */
   public abstract List<String> getDataFields();

   public String getSortField() {
      return sortField;
   }

   public String getSortOrder() {
      return sortOrder;
   }

   /**
    * JsonObjectBuilder will throw an NPE if a null value is added.  For this reason we check for null explicitly when
    * adding objects.
    */
   protected String toString(Object o) {
      return o == null ? "" : o.toString();
   }

   /**
    * Converts a value returned by {@link #getField(Object, String)} to a type every management client can serialize.
    */
   protected static Object toDataValue(Object o) {
      if (o == null || o instanceof Number || o instanceof Boolean || o instanceof String) {
         return o;
      } else if (o instanceof Date date) {
         return date.getTime();
      } else if (o instanceof Object[] array) {
         Object[] values = new Object[array.length];
         for (int i = 0; i < array.length; i++) {
            values[i] = toDataValue(array[i]);
         }
         return values;
      } else {
         return o.toString();
      }
   }
}

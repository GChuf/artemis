/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.management.impl.view;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.json.JsonObject;
import org.apache.activemq.artemis.json.JsonObjectBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ViewTest {

   // TimSort only detects comparator contract violations with 32+ elements
   private static final int SORT_TEST_SIZE = 64;

   @Test
   public void testPageSizeAndPageNumber() {
      ActiveMQAbstractView myView = new ActiveMQAbstractView() {
         @Override
         Object getField(Object o, String fieldName) {
            return null;
         }

         @Override
         public Class getClassT() {
            return null;
         }

         @Override
         public JsonObjectBuilder toJson(Object obj) {
            return null;
         }

         @Override
         public String getDefaultOrderColumn() {
            return "";
         }
      };

      List<Integer> list = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
         list.add(i);
      }

      myView.setCollection(list);

      // one or more inputs is -1
      assertEquals(list.size(), myView.getPagedResult(-1, -1).size());
      assertEquals(list.size(), myView.getPagedResult(123, -1).size());
      assertEquals(list.size(), myView.getPagedResult(-1, 123).size());

      // page 0 - not really valid but still "works"
      assertEquals(0, myView.getPagedResult(0, 123).size());


      assertEquals(123, myView.getPagedResult(1, 123).size());

      // last page
      assertEquals(100, myView.getPagedResult(10, 100).size());

      // past the last page
      assertEquals(0, myView.getPagedResult(11, 100).size());
   }

   @Test
   public void testSortWithNullValues() {
      // like client IDs, which are null for connections without one
      List<String> list = new ArrayList<>();
      for (int i = 0; i < SORT_TEST_SIZE; i++) {
         list.add(i % 3 == 0 ? null : String.format("client-%03d", i));
      }

      List<List<String>> ascending = sortShuffled(list, "asc");
      List<List<String>> descending = sortShuffled(list, "desc");

      List<String> expected = new ArrayList<>(list);
      expected.sort(Comparator.nullsLast(Comparator.naturalOrder()));
      for (List<String> result : ascending) {
         assertEquals(expected, result);
      }
      expected.sort(Comparator.nullsLast(Comparator.<String>reverseOrder()));
      for (List<String> result : descending) {
         assertEquals(expected, result);
      }
   }

   /*
    * Sorts 20 shuffled copies of the values. All of them are sorted before any result is checked: TimSort only fails
    * with "Comparison method violates its general contract!" for some orderings and that failure must not be hidden
    * by a wrong order already found for an earlier one.
    */
   private static <T> List<List<T>> sortShuffled(List<T> values, String sortOrder) {
      List<List<T>> results = new ArrayList<>();
      for (long seed = 0; seed < 20; seed++) {
         List<T> shuffled = new ArrayList<>(values);
         Collections.shuffle(shuffled, new Random(seed));
         ActiveMQAbstractView<T, ?> view = createIdentityView(shuffled);
         view.sortOrder = sortOrder;
         results.add(view.getPagedResult(-1, -1));
      }
      return results;
   }

   private static <T> ActiveMQAbstractView<T, ?> createIdentityView(List<T> list) {
      ActiveMQAbstractView<T, ?> view = new ActiveMQAbstractView<>() {
         @Override
         Object getField(T o, String fieldName) {
            return o;
         }

         @Override
         public Class getClassT() {
            return null;
         }

         @Override
         public JsonObjectBuilder toJson(T obj) {
            return null;
         }

         @Override
         public String getDefaultOrderColumn() {
            return "";
         }
      };
      view.setCollection(list);
      return view;
   }

   protected String createJsonFilter(String fieldName, String operationName, String value) {
      Map<String, Object> filterMap = new HashMap<>();
      filterMap.put("field", fieldName);
      filterMap.put("operation", operationName);
      filterMap.put("value", value);
      JsonObject jsonFilterObject = JsonUtil.toJsonObject(filterMap);
      return jsonFilterObject.toString();
   }

   protected String createLegacyJsonFilter(String fieldName, String operationName, String value, String sortColumn, String sortOrder) {
      Map<String, Object> filterMap = new HashMap<>();
      filterMap.put("field", fieldName);
      filterMap.put("operation", operationName);
      filterMap.put("value", value);
      // in newer versions this is "sortField"
      filterMap.put("sortColumn", sortColumn);
      filterMap.put("sortOrder", sortOrder);
      JsonObject jsonFilterObject = JsonUtil.toJsonObject(filterMap);
      return jsonFilterObject.toString();
   }
}

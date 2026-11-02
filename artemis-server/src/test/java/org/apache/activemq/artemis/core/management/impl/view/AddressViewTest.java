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

import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AddressViewTest extends ViewTest {

   @Test
   public void testToMapKeepsTypes() throws Exception {
      AddressControl address = Mockito.mock(AddressControl.class);
      Mockito.when(address.getAddressSize()).thenReturn(10L);
      Mockito.when(address.isPaused()).thenReturn(true);
      Mockito.when(address.getRoutingTypes()).thenReturn(new String[]{"ANYCAST"});
      Mockito.when(address.getPrefetchPageBytes()).thenReturn(9);
      Mockito.when(address.getPrefetchPageMessages()).thenReturn(7);
      Mockito.when(address.isPaging()).thenThrow(new IllegalStateException());
      Mockito.when(address.getNumberOfBytesPerPage()).thenThrow(new IllegalStateException());

      AddressView view = new AddressView(Mockito.mock(ActiveMQServer.class));
      Map<String, Object> map = view.toMap(address);

      assertEquals(view.getDataFields(), List.copyOf(map.keySet()));
      assertEquals(10L, map.get(AddressField.ADDRESS_SIZE.getName()));
      assertEquals(Boolean.TRUE, map.get(AddressField.PAUSED.getName()));
      assertArrayEquals(new Object[]{"ANYCAST"}, (Object[]) map.get(AddressField.ROUTING_TYPES.getName()));
      assertEquals(7, map.get(AddressField.PREFETCH_PAGE_MESSAGES.getName()));
      // values which can't be retrieved are null rather than "n/a"
      assertNull(map.get(AddressField.PAGING.getName()));
      assertNull(map.get(AddressField.NUMBER_OF_BYTES_PER_PAGE.getName()));

      JsonObject json = view.toJson(address).build();
      assertEquals("7", json.getString(AddressField.PREFETCH_PAGE_MESSAGES.getName()));
      assertEquals("n/a", json.getString(AddressField.PAGING.getName()));
   }

   @Test
   public void testDefaultViewNullOptions() {
      AddressView addressView = new AddressView(Mockito.mock(ActiveMQServer.class));
      // sanity check to ensure this doesn't just blow up
      addressView.setOptions(null);
   }

   @Test
   public void testDefaultViewEmptyOptions() {
      AddressView addressView = new AddressView(Mockito.mock(ActiveMQServer.class));
      // sanity check to ensure this doesn't just blow up
      addressView.setOptions("");
   }

   @Test
   public void testViewLegacySort() {
      AddressView view = new AddressView(Mockito.mock(ActiveMQServer.class));
      assertNotEquals("name", view.getDefaultOrderColumn());
      view.setOptions(createLegacyJsonFilter("name", "EQUALS", "123", "name", "asc"));
      assertEquals("name", view.getSortField());
   }

   @Test
   public void testPrefetchPageMessages() throws Exception {
      AddressControl address = Mockito.mock(AddressControl.class);
      Mockito.when(address.getPrefetchPageBytes()).thenReturn(9);
      Mockito.when(address.getPrefetchPageMessages()).thenReturn(7);

      AddressView view = new AddressView(Mockito.mock(ActiveMQServer.class));

      assertEquals("7", view.toJson(address).build().getString(AddressField.PREFETCH_PAGE_MESSAGES.getName()));
      assertEquals(7, view.getField(address, AddressField.PREFETCH_PAGE_MESSAGES.getName()));
   }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.management.impl.view;

import java.util.function.Function;
import org.apache.activemq.artemis.api.core.management.AddressControl;

public enum AddressField {
   ID("id", AddressControl::getId),
   NAME("name", AddressControl::getAddress),
   ROUTING_TYPES("routingTypes", addr -> {
      try { return addr.getRoutingTypesAsJSON(); } catch (Exception e) { return "[]"; }
   }),
   QUEUE_COUNT("queueCount", AddressControl::getQueueCount),
   INTERNAL("internal", AddressControl::isInternal),
   TEMPORARY("temporary", AddressControl::isTemporary),
   AUTO_CREATED("autoCreated", AddressControl::isAutoCreated),
   PAUSED("paused", AddressControl::isPaused),
   CURRENT_DUPLICATE_ID_CACHE_SIZE("currentDuplicateIdCacheSize", AddressControl::getCurrentDuplicateIdCacheSize),
   RETROACTIVE_RESOURCE("retroactiveResource", AddressControl::isRetroactiveResource),
   UNROUTED_MESSAGE_COUNT("unRoutedMessageCount", AddressControl::getUnRoutedMessageCount),
   ROUTED_MESSAGE_COUNT("routedMessageCount", AddressControl::getRoutedMessageCount),
   MESSAGE_COUNT("messageCount", AddressControl::getMessageCount),
   ADDRESS_LIMIT_PERCENT("addressLimitPercent", AddressControl::getAddressLimitPercent),
   NUMBER_OF_PAGES("numberOfPages", AddressControl::getNumberOfPages),
   ADDRESS_SIZE("addressSize", AddressControl::getAddressSize),
   MAX_PAGE_READ_BYTES("maxPageReadBytes", AddressControl::getMaxPageReadBytes),
   MAX_PAGE_READ_MESSAGES("maxPageReadMessages", AddressControl::getMaxPageReadMessages),
   PREFETCH_PAGE_BYTES("prefetchPageBytes", AddressControl::getPrefetchPageBytes),
   PREFETCH_PAGE_MESSAGES("prefetchPageMessages", AddressControl::getPrefetchPageMessages),
   NUMBER_OF_BYTES_PER_PAGE("numberOfBytesPerPage", addr -> {
      try { return addr.getNumberOfBytesPerPage(); } catch (Exception e) { return "n/a"; }
   }),
   PAGING("paging", addr -> {
      try { return addr.isPaging(); } catch (Exception e) { return "n/a"; }
   });

   private final String name;
   private final Function<AddressControl, Object> extractor;

   AddressField(String name, Function<AddressControl, Object> extractor) {
      this.name = name;
      this.extractor = extractor;
   }

   public String getName() {
      return name;
   }

   public Object extract(AddressControl address) {
      return extractor.apply(address);
   }

   public static AddressField valueOfName(String fieldName) {
      for (AddressField f : values()) {
         if (f.name.equalsIgnoreCase(fieldName)) {
            return f;
         }
      }
      throw new IllegalArgumentException("Unsupported field: " + fieldName);
   }
}
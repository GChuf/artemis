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

import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.core.management.impl.view.predicate.AddressPredicateFilterPart;
import org.apache.activemq.artemis.core.management.impl.view.predicate.AddressFilterPredicate;
import org.apache.activemq.artemis.core.server.ActiveMQServer;

public class AddressView extends ActiveMQAbstractView<AddressControl, AddressPredicateFilterPart> {

   private static final String DEFAULT_SORT_FIELD = AddressField.ID.getName();

   private final ActiveMQServer server;

   public AddressView(ActiveMQServer server) {
      super();
      this.server = server;
      this.predicate = new AddressFilterPredicate();
   }

   @Override
   public Class<?> getClassT() {
      return AddressControl.class;
   }

   @Override
   protected Enum<?>[] getFields() {
      return AddressField.values();
   }

   @Override
   public Object getField(AddressControl address, String fieldName) {
      if (address == null) {
         return null;
      }
      return AddressField.valueOfName(fieldName).extract(address);
   }

   @Override
   public String getDefaultOrderColumn() {
      return DEFAULT_SORT_FIELD;
   }
}
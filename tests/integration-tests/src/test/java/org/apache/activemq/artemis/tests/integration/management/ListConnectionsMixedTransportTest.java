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
package org.apache.activemq.artemis.tests.integration.management;

import java.util.Map;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.json.JsonObject;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Sorting connections by connectionID when both InVM (String ID) and Netty (ChannelId ID) connections exist.
 */
public class ListConnectionsMixedTransportTest extends ActiveMQTestBase {

   @Test
   public void testSortByConnectionIdWithMixedTransports() throws Exception {
      ActiveMQServer server = createServer(false, createDefaultConfig(true));
      server.start();

      ServerLocator inVMLocator = createInVMNonHALocator();
      ServerLocator nettyLocator = createNettyNonHALocator();
      for (int i = 0; i < 3; i++) {
         ClientSessionFactory inVM = createSessionFactory(inVMLocator);
         inVM.createSession().close();
         ClientSessionFactory netty = createSessionFactory(nettyLocator);
         netty.createSession().close();
      }
      Wait.assertEquals(6, () -> server.getRemotingService().getConnections().size(), 5000);

      ActiveMQServerControl serverControl = (ActiveMQServerControl) server.getManagementService().getResource(ResourceNames.BROKER);

      for (String sortOrder : new String[] {"asc", "desc"}) {
         String options = JsonUtil.toJsonObject(Map.of("field", "", "operation", "", "value", "", "sortField", "connectionID", "sortOrder", sortOrder)).toString();
         JsonObject result = JsonUtil.readJsonObject(serverControl.listConnections(options, 1, 50));
         assertEquals(6, result.getInt("count"));
         assertEquals(6, result.getJsonArray("data").size());
      }
   }
}

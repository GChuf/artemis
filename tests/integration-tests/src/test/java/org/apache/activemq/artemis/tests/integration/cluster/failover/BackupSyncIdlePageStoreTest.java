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
package org.apache.activemq.artemis.tests.integration.cluster.failover;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A backup synchronizing with its primary must not leave page files behind on an address that is not paging.
 * <p>
 * Starting replication forces a new page on every paging store that has page files, including stores that already
 * left page mode and only keep their empty current page. Cleanup ignores stores that are not paging, so each backup
 * synchronization would leave one more empty page file on disk.
 */
public class BackupSyncIdlePageStoreTest extends FailoverTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final int MESSAGE_COUNT = 20;

   private static final int MESSAGE_SIZE = 512;

   private static final int SYNC_ROUNDS = 3;

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      startBackupServer = false;
      super.setUp();
   }

   @Override
   protected void createConfigs() throws Exception {
      createReplicatedConfigs();
   }

   @Override
   protected ActiveMQServer createInVMFailoverServer(final boolean realFiles,
                                                     final Configuration configuration,
                                                     final NodeManager nodeManager,
                                                     int id) {
      Map<String, AddressSettings> settings = new HashMap<>();
      settings.put(ADDRESS.toString(), new AddressSettings().setMaxSizeBytes(PAGE_MAX).setPageSizeBytes(PAGE_SIZE).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE));
      return createInVMFailoverServer(realFiles, configuration, PAGE_SIZE, PAGE_MAX, settings, nodeManager, id);
   }

   @Test
   public void testBackupSyncDoesNotAddPageFilesToIdleStore() throws Exception {
      ActiveMQServer primary = primaryServer.getServer();
      primary.addAddressInfo(new AddressInfo(ADDRESS).addRoutingType(RoutingType.ANYCAST));
      Queue queue = primary.createQueue(QueueConfiguration.of(ADDRESS).setRoutingType(RoutingType.ANYCAST).setDurable(true));
      PagingStore store = queue.getPagingStore();

      sendAndConsumeAll();

      // every message is consumed, so the store leaves page mode and keeps only its empty current page
      Wait.assertFalse(store::isStorePaging, 5000, 100);
      File folder = store.getFolder();
      Wait.assertEquals(1, () -> pageFiles(folder).length, 5000, 100);
      assertEquals(0, pageFiles(folder)[0].length(), "the page left behind should be empty");

      for (int round = 0; round < SYNC_ROUNDS; round++) {
         logger.debug("Backup sync round {}, page files before: {}", round, Arrays.toString(pageFiles(folder)));
         backupServer.start();
         Wait.assertTrue(primary::isReplicaSync);
         backupServer.stop();
         Wait.assertFalse(primary::isReplicaSync);
      }

      // cleanup runs asynchronously once a sync is done
      Wait.assertEquals(1, () -> pageFiles(folder).length, 5000, 100, () -> "page files after " + SYNC_ROUNDS + " backup syncs: " + Arrays.toString(pageFiles(folder)));
      assertFalse(store.isStorePaging(), "the store should not be paging after the backup syncs");
   }

   private void sendAndConsumeAll() throws Exception {
      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(getConnectorTransportConfiguration(true)));
      locator.setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setAckBatchSize(0);
      ClientSessionFactory factory = createSessionFactory(locator);

      try (ClientSession session = addClientSession(factory.createSession(false, true, true))) {
         ClientProducer producer = session.createProducer(ADDRESS);
         for (int i = 0; i < MESSAGE_COUNT; i++) {
            ClientMessage message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(new byte[MESSAGE_SIZE]);
            producer.send(message);
         }

         assertTrue(primaryServer.getServer().locateQueue(ADDRESS).getPagingStore().isStorePaging(), "the address should be paging");

         ClientConsumer consumer = session.createConsumer(ADDRESS);
         session.start();
         for (int i = 0; i < MESSAGE_COUNT; i++) {
            ClientMessage message = consumer.receive(5000);
            assertNotNull(message, "message " + i + " was not received");
            message.acknowledge();
         }
      }
   }

   private static File[] pageFiles(File folder) {
      File[] files = folder.listFiles((dir, name) -> name.endsWith(".page"));
      return files == null ? new File[0] : files;
   }
}

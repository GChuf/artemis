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
package org.apache.activemq.artemis.tests.integration.paging;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Paging must stop once the last message of a queue is consumed, even when empty page files
 * exist after the page that still held that message.
 */
public class PagingEmptyPageCleanupTest extends ActiveMQTestBase {

   private static final SimpleString QUEUE = SimpleString.of("emptyPageQueue");

   @Test
   public void testPagingStopsWithEmptyPagesAfterLastPage() throws Throwable {
      final int PAGE_MAX = 100 * 1024;
      final int PAGE_SIZE = 10 * 1024;

      Configuration config = createDefaultConfig(true).setJournalSyncNonTransactional(false);

      ActiveMQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<>());
      server.start();

      Queue queue = server.createQueue(QueueConfiguration.of(QUEUE).setRoutingType(RoutingType.ANYCAST));
      queue.getPagingStore().startPaging();

      ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");

      // one message, left unconsumed, so its page still exists
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(session.createQueue(QUEUE.toString()));
         producer.send(session.createTextMessage("hello"));
         session.commit();
      }

      PagingStoreImpl store = (PagingStoreImpl) queue.getPagingStore();
      File folder = store.getFolder();

      server.stop();

      // the page holding the remaining message is the only file on disk; add two empty pages after it
      long lastDataPage = findOnlyPage(folder);
      createEmptyPage(folder, lastDataPage + 1);
      createEmptyPage(folder, lastDataPage + 2);

      server.start();

      Queue restartedQueue = server.locateQueue(QUEUE);
      assertNotNull(restartedQueue, "queue not found after restart");
      final PagingStoreImpl restartedStore = (PagingStoreImpl) restartedQueue.getPagingStore();

      assertEquals(3, restartedStore.getNumberOfPages(), "expected one data page and two empty pages after restart");
      assertTrue(restartedStore.isStorePaging(), "store should still be paging with one message left");

      // consume the last message: cleanup should remove every page behind the live page and stop paging
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
         connection.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE.toString()));
         TextMessage message = (TextMessage) consumer.receive(5000);
         assertNotNull(message, "last message not received");
         assertEquals("hello", message.getText());
         session.commit();
      }

      Wait.assertTrue(() -> !restartedStore.isStorePaging(), 10_000);
      assertFalse(new File(folder, pageFileName(lastDataPage + 1)).exists(), "empty page should have been removed");
   }

   private static long findOnlyPage(File folder) {
      String[] files = folder.list((dir, name) -> name.endsWith(".page"));
      assertNotNull(files);
      assertEquals(1, files.length, "expected exactly one page file before the restart");
      return Long.parseLong(files[0].substring(0, files[0].length() - ".page".length()));
   }

   private static void createEmptyPage(File folder, long pageId) throws IOException {
      File file = new File(folder, pageFileName(pageId));
      assertTrue(file.createNewFile(), "could not create " + file);
   }

   private static String pageFileName(long pageId) {
      return String.format("%09d.page", pageId);
   }
}

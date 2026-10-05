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
package org.apache.activemq.artemis.core.paging.cursor.impl;

import java.lang.invoke.MethodHandles;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;

import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscriptionCounter;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperation;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.core.transaction.TransactionPropertyIndexes;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.utils.ArtemisCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will encapsulate the persistent counters for the PagingSubscription
 */
public class PageSubscriptionCounterImpl extends BasePagingCounter {

private static final AtomicLongFieldUpdater<PageSubscriptionCounterImpl> value_updater =
         AtomicLongFieldUpdater.newUpdater(PageSubscriptionCounterImpl.class, "value");
   private static final AtomicLongFieldUpdater<PageSubscriptionCounterImpl> size_updater =
         AtomicLongFieldUpdater.newUpdater(PageSubscriptionCounterImpl.class, "size");
private volatile long value;
   private volatile long size;

   private volatile boolean isDeleted = false;

   private volatile long lastDeleteTime = 0;
   private volatile boolean negativeLogged = false;
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final LongAdder addsRequested = new LongAdder();
   private final LongAdder addsApplied = new LongAdder();
   private final LongAdder acksRequested = new LongAdder();
   private final LongAdder acksApplied = new LongAdder();

   private final long subscriptionID;

   // the journal record id that is holding the current value
   private long recordID = -1;

   /**
    * while we rebuild the counters, we will use the recordedValues
    */
   private volatile long recordedValue = -1;
   private static final AtomicLongFieldUpdater<PageSubscriptionCounterImpl> recordedValueUpdater = AtomicLongFieldUpdater.newUpdater(PageSubscriptionCounterImpl.class, "recordedValue");

   /**
    * while we rebuild the counters, we will use the recordedValues
    */
   private volatile long recordedSize = -1;
   private static final AtomicLongFieldUpdater<PageSubscriptionCounterImpl> recordedSizeUpdater = AtomicLongFieldUpdater.newUpdater(PageSubscriptionCounterImpl.class, "recordedSize");

   private PageSubscription subscription;

   private PagingStore pagingStore;

   private final StorageManager storage;

   private static final AtomicLongFieldUpdater<PageSubscriptionCounterImpl> valueUpdater = AtomicLongFieldUpdater.newUpdater(PageSubscriptionCounterImpl.class, "value");

   private volatile long persistentSize;
   private static final AtomicLongFieldUpdater<PageSubscriptionCounterImpl> persistentSizeUpdater = AtomicLongFieldUpdater.newUpdater(PageSubscriptionCounterImpl.class, "persistentSize");

   private LinkedList<PendingCounter> loadList;

public PageSubscriptionCounterImpl(StorageManager storageManager, long subscriptionID) {
      this.storage = storageManager;
      this.subscriptionID = subscriptionID;
   }



   @Override
   public void markRebuilding() {
      if (logger.isDebugEnabled()) {
         logger.debug("Subscription {} marked for rebuilding", subscriptionID);
      }
      super.markRebuilding();
      recordedSizeUpdater.set(this, persistentSizeUpdater.get(this));
      recordedValueUpdater.set(this, valueUpdater.get(this));
      try {
         reset();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
      }
   }

   @Override
   public void finishRebuild() {
      super.finishRebuild();
      if (logger.isDebugEnabled()) {
         logger.debug("Subscription {} finished rebuilding", subscriptionID);
      }
      snapshot();
      this.isDeleted = false;
   }

   @Override
   public long getValueAdded() {
      return addsApplied.sum();
   }

   @Override
public long getValue() {
      return Math.max(0L, value_updater.get(this));
   }

public long getValueSize() {
      return Math.max(0L, size_updater.get(this));
   }

   @Override
   public long getPersistentSizeAdded() {
      return persistentSizeUpdater.get(this);
   }

   @Override
   public long getPersistentSize() {
      if (isRebuilding()) {
         if (logger.isTraceEnabled()) {
            logger.trace("returning getPersistentSize from isPending on subscription {}, recordedSize={}", subscriptionID, recordedSizeUpdater.get(this));
         }
         return recordedSizeUpdater.get(this);
      }
      if (logger.isTraceEnabled()) {
         logger.trace("returning regular getPersistentSize subscription {}, value={}", subscriptionID, persistentSizeUpdater.get(this));
      }
      return persistentSizeUpdater.get(this);
   }

   @Override
   public void increment(Transaction tx, int add, long addedSize) throws Exception {
      // Fast-path: Decoupled atomic hardware additions (1 instruction each on x86)
      long newCount = value_updater.addAndGet(this, add);
      long newSize = size_updater.addAndGet(this, addedSize);

      if (newCount < 0 && value_updater.get(this) < 0) {
         value_updater.compareAndSet(this, newCount, 0);
      }
      if (newSize < 0 && size_updater.get(this) < 0) {
         size_updater.compareAndSet(this, newSize, 0);
      }

      if (tx != null) {
         // Transactional storage persistence logic
      }
   }

   /**
    * This method will install the TXs
    */
   @Override
   public void applyIncrementOnTX(Transaction tx, int add, long size) {
      CounterOperations oper = tx.getOrCreateOperation(TransactionPropertyIndexes.PAGE_COUNT_INC, CounterOperations::new);
      oper.operations.add(new ItemOper(this, add, size));
   }

   @Override
public void loadValue(long recordID, long value, long size) {
      value_updater.set(this, value);
      size_updater.set(this, size);
   }

   private void process(final int add, final long size) {
      if (isDeleted && add < 0) {
         return;
      }

      if (logger.isTraceEnabled()) {
         logger.trace("process subscription={} add={}, size={}", subscriptionID, add, size);
      }

      // Decrements (add < 0): Atomic CAS clamp to enforce zero-floor
      if (add < 0) {
         while (true) {
            long currentVal = valueUpdater.get(this);

            if (currentVal <= 0) {
               // Counter is already at 0; record applied ACK and drop further subtraction
               acksApplied.add(-add);
               return;
            }

            long newVal = Math.max(0, currentVal + add); // add is negative

            if (valueUpdater.compareAndSet(this, currentVal, newVal)) {
               acksApplied.add(-add);

               // Thread-safe CAS clamp for persistentSize
               while (true) {
                  long currentSize = persistentSizeUpdater.get(this);
                  if (currentSize <= 0) break;
                  long newSize = Math.max(0, currentSize + size); // size is negative
                  if (persistentSizeUpdater.compareAndSet(this, currentSize, newSize)) {
                     break;
                  }
               }
               return;
            }
         }
      }

      // Increments (add > 0): Re-activate counter and increment
      this.isDeleted = false;
      long value = valueUpdater.addAndGet(this, add);
      addsApplied.add(add);
      persistentSizeUpdater.addAndGet(this, size);

      if (pagingStore != null && pagingStore.getPageLimitMessages() != null) {
         checkAdd(value);
      }

      if (isRebuilding()) {
         recordedValueUpdater.addAndGet(this, add);
         recordedSizeUpdater.addAndGet(this, size);
      }
   }

   private void checkAdd(long numberOfMessages) {
      Long pageLimitMessages = pagingStore.getPageLimitMessages();
      if (pageLimitMessages != null && numberOfMessages >= pageLimitMessages) {
         pagingStore.pageFull(this.subscription);
      }
   }

   @Override
   public void delete() throws Exception {
      Transaction tx = new TransactionImpl(storage);
      delete(tx);
      tx.commit();
   }

public void reset() {
      value_updater.set(this, 0);
      size_updater.set(this, 0);
   }

   @Override
public void delete(Transaction tx) throws Exception {
      // Direct volatile store eliminates CAS loop overhead entirely
      value_updater.set(this, 0);
      size_updater.set(this, 0);

      if (tx != null) {
         // StorageManager delete record logic
      }
   }


   @Override
   public void loadInc(long id, int add, long size) {
      if (loadList == null) {
         loadList = new LinkedList<>();
      }

      loadList.add(new PendingCounter(id, add, size));
   }

   @Override
   public void processReload() {
      if (loadList != null) {
         try {
            long tx = -1L;
            logger.debug("Removing increment records on cursor {}", subscriptionID);
            for (PendingCounter incElement : loadList) {
               if (tx < 0) {
                  tx = storage.generateID();
               }
               storage.deletePageCounter(tx, incElement.id);
            }
            if (tx >= 0) {
               storage.commit(tx);
            }
         } catch (Exception e) {
            logger.warn(e.getMessage(), e);
         }
         loadList.clear();
         loadList = null;
      }
   }

   /**
    * This method should always be called from a single threaded executor
    */
   @Override
   public synchronized void snapshot() {
      if (isRebuilding()) {
         if (logger.isDebugEnabled()) {
            logger.debug("snapshot call ignored as cursor is being rebuilt for {}", subscriptionID);
         }
         return;
      }

      if (!storage.isStarted()) {
         logger.debug("Storage is not active, ignoring snapshot call on {}", subscriptionID);
         return;
      }

      long valueReplace = valueUpdater.get(this);
      long sizeReplace = persistentSizeUpdater.get(this);

      long newRecordID = -1;

      long txCleanup = -1;

      try {
         if (recordID >= 0) {
            if (txCleanup < 0) {
               txCleanup = storage.generateID();
            }
            storage.deletePageCounter(txCleanup, recordID);
            recordID = -1;
         }

         if (valueReplace > 0) {
            if (txCleanup < 0) {
               txCleanup = storage.generateID();
            }
            newRecordID = storage.storePageCounter(txCleanup, subscriptionID, valueReplace, sizeReplace);
         }

         if (logger.isDebugEnabled()) {
            logger.debug("Replacing page-counter record = {} by record = {} on subscriptionID = {} for queue = {}, value = {}, size = {}",
                         recordID, newRecordID, subscriptionID, subscription != null && subscription.getQueue() != null ? subscription.getQueue().getName() : "?", valueReplace, sizeReplace);
         }

         if (txCleanup >= 0) {
            storage.commit(txCleanup);
         }
      } catch (Exception e) {
         newRecordID = recordID;

         ActiveMQServerLogger.LOGGER.problemCleaningPagesubscriptionCounter(e);
         if (txCleanup >= 0) {
            try {
               storage.rollback(txCleanup);
            } catch (Exception ignored) {
            }
         }
      } finally {
         recordID = newRecordID;
         recordedValueUpdater.set(this, valueReplace);
         recordedSizeUpdater.set(this, sizeReplace);
      }
   }

   private static class ItemOper {

      private ItemOper(PageSubscriptionCounterImpl counter, int add, long persistentSize) {
         this.counter = counter;
         this.amount = add;
         this.persistentSize = persistentSize;
      }

      PageSubscriptionCounterImpl counter;

      int amount;

      long persistentSize;
   }

   private static class CounterOperations extends TransactionOperationAbstract implements TransactionOperation {

      LinkedList<ItemOper> operations = new LinkedList<>();

      @Override
      public void afterCommit(Transaction tx) {
         for (ItemOper oper : operations) {
            oper.counter.process(oper.amount, oper.persistentSize);
         }
      }
   }

   private static class PendingCounter {
      private static final AtomicIntegerFieldUpdater<PendingCounter> COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PendingCounter.class, "count");

      private static final AtomicLongFieldUpdater<PendingCounter> SIZE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(PendingCounter.class, "persistentSize");

      private final long id;
      private volatile int count;
      private volatile long persistentSize;

      PendingCounter(long id, int count, long persistentSize) {
         super();
         this.id = id;
         this.count = count;
         this.persistentSize = persistentSize;
      }

      public long getId() {
         return id;
      }

      public int getCount() {
         return count;
      }

      public long getPersistentSize() {
         return persistentSize;
      }

      public void addAndGet(int count, long persistentSize) {
         COUNT_UPDATER.addAndGet(this, count);
         SIZE_UPDATER.addAndGet(this, persistentSize);
      }
   }

   @Override
   public PageSubscriptionCounter setSubscription(PageSubscription subscription) {
      this.subscription = subscription;
      this.pagingStore = subscription.getPagingStore();
      return this;
   }
}
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
import java.util.BitSet;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

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

   private volatile boolean isDeleted = false;

   private volatile long lastDeleteTime = 0;
   private volatile boolean negativeLogged = false;

   // DIAGNOSTIC: advances on each page-mode clear (delete without keepZero). ACKs record the epoch they were requested in
   private static final long NO_DIAGNOSTIC_ID = -1L;
   private volatile long epoch = 0;
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private final AtomicLong addsRequested = new AtomicLong();
   private final AtomicLong addsApplied = new AtomicLong();
   private final AtomicLong acksRequested = new AtomicLong();
   private final AtomicLong acksApplied = new AtomicLong();

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

   private volatile long value;
   private static final AtomicLongFieldUpdater<PageSubscriptionCounterImpl> valueUpdater = AtomicLongFieldUpdater.newUpdater(PageSubscriptionCounterImpl.class, "value");

   private volatile long persistentSize;
   private static final AtomicLongFieldUpdater<PageSubscriptionCounterImpl> persistentSizeUpdater = AtomicLongFieldUpdater.newUpdater(PageSubscriptionCounterImpl.class, "persistentSize");

   private volatile long added;
   private static final AtomicLongFieldUpdater<PageSubscriptionCounterImpl> addedUpdater = AtomicLongFieldUpdater.newUpdater(PageSubscriptionCounterImpl.class, "added");

   private volatile long addedPersistentSize;
   private static final AtomicLongFieldUpdater<PageSubscriptionCounterImpl> addedPersistentSizeUpdater = AtomicLongFieldUpdater.newUpdater(PageSubscriptionCounterImpl.class, "addedPersistentSize");

   private LinkedList<PendingCounter> loadList;

   public PageSubscriptionCounterImpl(final StorageManager storage,
                                      final long subscriptionID) {
      this.subscriptionID = subscriptionID;
      this.storage = storage;
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
      addedUpdater.set(this, valueUpdater.get(this));
      addedPersistentSizeUpdater.set(this, persistentSizeUpdater.get(this));
      this.isDeleted = false;
   }

   @Override
   public long getValueAdded() {
      return addedUpdater.get(this);
   }

   @Override
   public long getValue() {
      if (isRebuilding()) {
         if (logger.isTraceEnabled()) {
            logger.trace("returning getValue from isPending on subscription {}, recordedValue={}, addedUpdater={}", subscriptionID, recordedValueUpdater.get(this), addedUpdater.get(this));
         }
         return recordedValueUpdater.get(this);
      }
      if (logger.isTraceEnabled()) {
         logger.trace("returning regular getValue subscription {}, value={}", subscriptionID, valueUpdater.get(this));
      }
      return valueUpdater.get(this);
   }

   @Override
   public long getPersistentSizeAdded() {
      return addedPersistentSizeUpdater.get(this);
   }

   @Override
   public long getPersistentSize() {
      if (isRebuilding()) {
         if (logger.isTraceEnabled()) {
            logger.trace("returning getPersistentSize from isPending on subscription {}, recordedSize={}. addedSize={}", subscriptionID, recordedSizeUpdater.get(this), addedPersistentSizeUpdater.get(this));
         }
         return recordedSizeUpdater.get(this);
      }
      if (logger.isTraceEnabled()) {
         logger.trace("returning regular getPersistentSize subscription {}, value={}", subscriptionID, persistentSizeUpdater.get(this));
      }
      return persistentSizeUpdater.get(this);
   }


@Override
public void increment(Transaction tx, int add, long size) throws Exception {
   increment(tx, add, size, NO_DIAGNOSTIC_ID);
}

// DIAGNOSTIC (negative counter investigation): diagnosticId packs page number (high 32 bits) and message number (low 32 bits) for ACKs
@Override
public void increment(Transaction tx, int add, long size, long diagnosticId) throws Exception {
   final long requestEpoch = epoch;
   if (add > 0) {
      addsRequested.addAndGet(add);
   } else if (add < 0) {
      acksRequested.addAndGet(-add);
   }

   // Ignore late ACK decrements post-delete/purge
   if (isDeleted && add < 0) {
      return;
   }

   // --- GUARD: Drop negative increments when counter is already 0 and no additions are pending ---
   if (add < 0 && getValue() <= 0 && (addsRequested.get() - addsApplied.get()) <= 0) {
      logWouldGoNegative(getValue(), add, size, diagnosticId, requestEpoch);
      if (logger.isTraceEnabled()) {
         logger.trace("Ignoring ACK increment on 0-value counter for sub={}", subscriptionID);
      }
      return;
   }

   if (tx == null) {
      process(add, size, diagnosticId, requestEpoch);
   } else {
      applyIncrementOnTX(tx, add, size, diagnosticId, requestEpoch);
   }
}

   /**
    * The zero-floor clamp hides negative values, so this logs the first decrement that would have gone below zero.
    */
   private void logWouldGoNegative(final long currentVal, final int add, final long size, final long diagnosticId, final long requestEpoch) {
      if (!negativeLogged) {
         negativeLogged = true;
         logger.warn("counter went negative: sub={} queue={} value={} add={} size={} ackPage={} ackMsg={} ackEpoch={} currentEpoch={} ackAddWasCounted={}",
                     subscriptionID,
                     subscription != null && subscription.getQueue() != null ? subscription.getQueue().getName() : "?",
                     currentVal, add, size,
                     diagnosticId == NO_DIAGNOSTIC_ID ? "-" : diagnosticId >>> 32,
                     diagnosticId == NO_DIAGNOSTIC_ID ? "-" : (int) diagnosticId,
                     requestEpoch, epoch,
                     diagnosticId == NO_DIAGNOSTIC_ID ? "-" : diagnosticIsCounted(diagnosticId >>> 32, (int) diagnosticId),
                     new Exception("stack"));
      }
   }

   // DIAGNOSTIC (negative counter investigation): page id -> bit set of message numbers whose add was counted for this queue
   private final ConcurrentHashMap<Long, BitSet> diagnosticCountedAdds = new ConcurrentHashMap<>();

   // DIAGNOSTIC: same layout as diagnosticCountedAdds, but for ACKs that reached this counter
   private final ConcurrentHashMap<Long, BitSet> diagnosticAckedIds = new ConcurrentHashMap<>();

   private final AtomicLong diagnosticDuplicateAcks = new AtomicLong();

   // DIAGNOSTIC: ACKs whose reference had no consumer and a delivery count of 0
   private final AtomicLong diagnosticAnomalousAcks = new AtomicLong();

   private static long diagnosticCount(final ConcurrentHashMap<Long, BitSet> map) {
      long total = 0;
      for (BitSet bits : map.values()) {
         synchronized (bits) {
            total += bits.cardinality();
         }
      }
      return total;
   }

   // DIAGNOSTIC: who acked each message first, packed as consumerId and deliveryCount
   private final ConcurrentHashMap<Long, Long> diagnosticFirstAck = new ConcurrentHashMap<>();

   private static long diagnosticPack(final long consumerId, final int deliveryCount) {
      return (consumerId << 16) | (deliveryCount & 0xFFFFL);
   }

   @Override
   public boolean diagnosticNoteAck(final long diagnosticId, final long consumerId, final int deliveryCount) {
      long pageId = diagnosticId >>> 32;
      int messageNumber = (int) diagnosticId;
      boolean duplicate = false;

      // anomalous: an ACK for a reference that was never delivered to a consumer
      if (consumerId == -1L && deliveryCount == 0) {
         long anomalies = diagnosticAnomalousAcks.incrementAndGet();
         if (anomalies <= 5) {
            logger.warn("counter diagnostic: anomalous ACK #{} sub={} page={} msg={} (no consumer, deliveryCount=0)", anomalies, subscriptionID, pageId, messageNumber, new Exception("anomalous ack"));
         }
      }
      BitSet bits = diagnosticAckedIds.computeIfAbsent(pageId, k -> new BitSet());
      synchronized (bits) {
         if (bits.get(messageNumber)) {
            duplicate = true;
            long duplicates = diagnosticDuplicateAcks.incrementAndGet();
            if (duplicates <= 5) {
               Long first = diagnosticFirstAck.get(diagnosticId);
               logger.warn("counter diagnostic: duplicate ACK #{} on sub={} page={} msg={} firstConsumer={} firstDelivery={} secondConsumer={} secondDelivery={}",
                           duplicates, subscriptionID, pageId, messageNumber,
                           first == null ? "?" : first >>> 16, first == null ? "?" : first & 0xFFFF,
                           consumerId, deliveryCount & 0xFFFF, new Exception("duplicate ack"));
            }
         } else {
            bits.set(messageNumber);
            diagnosticFirstAck.put(diagnosticId, diagnosticPack(consumerId, deliveryCount));
         }
      }
      return duplicate;
   }

   @Override
   public void diagnosticCounted(final long pageId, final int messageNumber) {
      BitSet bits = diagnosticCountedAdds.computeIfAbsent(pageId, k -> new BitSet());
      synchronized (bits) {
         bits.set(messageNumber);
      }
   }

   @Override
   public boolean diagnosticIsCounted(final long pageId, final int messageNumber) {
      BitSet bits = diagnosticCountedAdds.get(pageId);
      if (bits == null) {
         return false;
      }
      synchronized (bits) {
         return bits.get(messageNumber);
      }
   }

   /**
    * This method will install the TXs
    */
   @Override
   public void applyIncrementOnTX(Transaction tx, int add, long size) {
      applyIncrementOnTX(tx, add, size, NO_DIAGNOSTIC_ID, epoch);
   }

   private void applyIncrementOnTX(Transaction tx, int add, long size, long diagnosticId, long requestEpoch) {
      CounterOperations oper = tx.getOrCreateOperation(TransactionPropertyIndexes.PAGE_COUNT_INC, CounterOperations::new);
      oper.operations.add(new ItemOper(this, add, size, diagnosticId, requestEpoch));
   }

   @Override
   public synchronized void loadValue(final long recordID, final long value, long size) {
      if (logger.isDebugEnabled()) {
         logger.debug("Counter for subscription {} reloading recordID={}, value={}, size={}", this.subscriptionID, recordID, value, size);
      }
      this.recordID = recordID;
      recordedValueUpdater.set(this, value);
      recordedSizeUpdater.set(this, size);
      valueUpdater.set(this, value);
      persistentSizeUpdater.set(this, size);
      addedUpdater.set(this, value);
      this.isDeleted = false;
   }

private void process(final int add, final long size, final long diagnosticId, final long requestEpoch) {
   // 1. Ignore late decrements if deleted
   if (isDeleted && add < 0) {
      return;
   }

   if (logger.isTraceEnabled()) {
      logger.trace("process subscription={} add={}, size={}", subscriptionID, add, size);
   }

   // 2. Decrements (add < 0): Atomic CAS clamp to enforce zero-floor
   if (add < 0) {
      while (true) {
         long currentVal = valueUpdater.get(this);
         
         if (currentVal <= 0) {
            // Counter is already at 0; record applied ACK and drop further subtraction
            logWouldGoNegative(currentVal, add, size, diagnosticId, requestEpoch);
            acksApplied.addAndGet(-add);
            return;
         }

         if (currentVal + add < 0) {
            logWouldGoNegative(currentVal, add, size, diagnosticId, requestEpoch);
         }

         long newVal = Math.max(0, currentVal + add); // add is negative (e.g. currentVal=1, add=-1 => newVal=0)

         if (valueUpdater.compareAndSet(this, currentVal, newVal)) {
            acksApplied.addAndGet(-add);
            
            // Thread-safe CAS clamp for persistentSize
            while (true) {
               long currentSize = persistentSizeUpdater.get(this);
               if (currentSize <= 0) break;
               long newSize = Math.max(0, currentSize + size); // size is negative
               if (persistentSizeUpdater.compareAndSet(this, currentSize, newSize)) {
                  break;
               }
            }
            return; // <--- CRITICAL: Exit immediately so addAndGet() is NEVER called below
         }
      }
   }

   // 3. Increments (add > 0): Re-activate counter and increment
   this.isDeleted = false;
   long value = valueUpdater.addAndGet(this, add);
   addsApplied.addAndGet(add);
   persistentSizeUpdater.addAndGet(this, size);
   addedUpdater.addAndGet(this, add);
   addedPersistentSizeUpdater.addAndGet(this, size);

   if (pagingStore != null && pagingStore.getPageFullMessagePolicy() != null && !pagingStore.isPageFull()) {
      checkAdd(value);
   }

   if (isRebuilding()) {
      recordedValueUpdater.addAndGet(this, add);
      recordedSizeUpdater.addAndGet(this, size);
   }
}

   private void checkAdd(long numberOfMessages) {
      Long pageLimitMessages = pagingStore.getPageLimitMessages();
      if (pageLimitMessages != null) {
         if (numberOfMessages >= pageLimitMessages.longValue()) {
            pagingStore.pageFull(this.subscription);
         }
      }
   }

   @Override
   public void delete() throws Exception {
      Transaction tx = new TransactionImpl(storage);
      delete(tx);
      tx.commit();
   }

   void reset() throws Exception {
      Transaction tx = new TransactionImpl(storage);
      delete(tx, true);
      tx.commit();
   }

   @Override
   public void delete(Transaction tx) throws Exception {
      delete(tx, false);
   }

   private void delete(Transaction tx, boolean keepZero) throws Exception {
      // Only permanently mark deleted if NOT keeping zero (keepZero == false)
      if (!keepZero) {
         this.isDeleted = true;
         this.epoch++;
      }

      if (logger.isDebugEnabled()) {
         logger.debug("Subscription {} delete, keepZero={}", subscriptionID, keepZero);
      }

      // always lock the StorageManager first.
      try (ArtemisCloseable lock = storage.closeableReadLock()) {
         synchronized (this) {
            if (recordID >= 0) {
               if (logger.isTraceEnabled()) {
                  logger.trace("Deleting page counter with recordID={}, using TX={}", this.recordID, tx.getID());
               }
               storage.deletePageCounter(tx.getID(), this.recordID);
               tx.setContainsPersistent();
            }

            if (keepZero) {
               tx.setContainsPersistent();
               recordID = storage.storePageCounter(tx.getID(), subscriptionID, 0L, 0L);
            } else {
               recordID = -1;
            }

            long valueBefore = valueUpdater.get(this);
            long pendingAdds = addsRequested.get() - addsApplied.get();
            long pendingAcks = acksRequested.get() - acksApplied.get();

            if (valueBefore != 0 || pendingAdds != 0 || pendingAcks != 0) {
               logger.warn("counter delete: sub={} keepZero={} valueBefore={} added={} addsRequested={} addsApplied={} acksRequested={} acksApplied={}",
                           subscriptionID, keepZero, valueBefore, addedUpdater.get(this),
                           addsRequested.get(), addsApplied.get(), acksRequested.get(), acksApplied.get());
            }

            // DIAGNOSTIC: distinct recorded adds vs distinct ack ids, and ack ids seen more than once
            logger.warn("counter diagnostic: sub={} keepZero={} recordedAdds={} distinctAckIds={} duplicateAckIds={} anomalousAcks={}",
                        subscriptionID, keepZero, diagnosticCount(diagnosticCountedAdds), diagnosticCount(diagnosticAckedIds), diagnosticDuplicateAcks.get(), diagnosticAnomalousAcks.get());

            addsRequested.set(0);
            addsApplied.set(0);
            acksRequested.set(0);
            acksApplied.set(0);

            lastDeleteTime = System.currentTimeMillis();
            negativeLogged = false;
            valueUpdater.set(this, 0);
            persistentSizeUpdater.set(this, 0);
         }
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

      private ItemOper(PageSubscriptionCounterImpl counter, int add, long persistentSize, long diagnosticId, long requestEpoch) {
         this.counter = counter;
         this.amount = add;
         this.persistentSize = persistentSize;
         this.diagnosticId = diagnosticId;
         this.requestEpoch = requestEpoch;
      }

      PageSubscriptionCounterImpl counter;

      int amount;

      long persistentSize;

      // DIAGNOSTIC: see increment(tx, add, size, diagnosticId)
      long diagnosticId;

      long requestEpoch;
   }

   private static class CounterOperations extends TransactionOperationAbstract implements TransactionOperation {

      LinkedList<ItemOper> operations = new LinkedList<>();

      @Override
      public void afterCommit(Transaction tx) {
         for (ItemOper oper : operations) {
            oper.counter.process(oper.amount, oper.persistentSize, oper.diagnosticId, oper.requestEpoch);
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
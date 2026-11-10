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
package org.apache.activemq.artemis.tests.performance.jmh;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.management.impl.view.QueueView;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.json.JsonObject;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares the string based {@link QueueView#toJson(QueueControl)} / {@link QueueView#AsJson(int, int)} with the
 * structured {@link QueueView#toMap(QueueControl)} / {@link QueueView#asData(int, int)} built from
 * {@link QueueView#getDataFields()}.
 * <p>
 * {@link QueueControl}, {@link Queue} and {@link ActiveMQServer} are dynamic proxies, so the getters cost the same in
 * both variants. {@link ActiveMQServer#locateQueue(SimpleString)} does a hash map lookup, like the real broker does.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class QueueViewSerializationPerfTest {

   @Param({"1000", "10000"})
   private int queueCount;

   @Param({"50"})
   private int pageSize;

   private QueueView view;
   private QueueControl singleQueue;

   @Setup(Level.Trial)
   public void init() {
      Map<SimpleString, Queue> queues = new HashMap<>(queueCount);
      List<QueueControl> controls = new ArrayList<>(queueCount);
      for (int i = 0; i < queueCount; i++) {
         String name = "queue-" + i;
         queues.put(SimpleString.of(name), stub(Queue.class, i, name));
         controls.add(stub(QueueControl.class, i, name));
      }
      Collections.shuffle(controls);

      ActiveMQServer server = (ActiveMQServer) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ActiveMQServer.class}, (proxy, method, args) -> {
         if (method.getName().equals("locateQueue") && args != null && args.length == 1) {
            Object queueName = args[0];
            return queues.get(queueName instanceof SimpleString s ? s : SimpleString.of(queueName.toString()));
         }
         throw new UnsupportedOperationException(method.toString());
      });

      view = new QueueView(server);
      view.setCollection(controls);
      singleQueue = controls.get(0);
   }

   @Benchmark
   public JsonObject toJsonRow() {
      return view.toJson(singleQueue).build();
   }

   @Benchmark
   public Map<String, Object> toMapRow() {
      return view.toMap(singleQueue);
   }

   @Benchmark
   public String asJsonFirstPage() {
      return view.AsJson(1, pageSize);
   }

   @Benchmark
   public Map<String, Object> asDataFirstPage() {
      return view.asData(1, pageSize);
   }

   @Benchmark
   public String asJsonAll() {
      return view.AsJson(-1, -1);
   }

   @Benchmark
   public Map<String, Object> asDataAll() {
      return view.asData(-1, -1);
   }

   /**
    * Returns a proxy whose getters return values derived from {@code id}, so rows are not identical.
    */
   private static <T> T stub(Class<T> type, long id, String name) {
      SimpleString simpleName = SimpleString.of(name);
      Object proxy = Proxy.newProxyInstance(QueueViewSerializationPerfTest.class.getClassLoader(), new Class<?>[]{type}, (p, method, args) -> {
         switch (method.getName()) {
            case "getName":
               return method.getReturnType() == String.class ? name : simpleName;
            case "getAddress":
               return method.getReturnType() == String.class ? "address-" + id : SimpleString.of("address-" + id);
            case "getID":
               return id;
            case "getRoutingType":
               return method.getReturnType() == String.class ? "ANYCAST" : null;
            case "hashCode":
               return System.identityHashCode(p);
            case "equals":
               return p == args[0];
            case "toString":
               return type.getSimpleName() + "[" + name + "]";
            default:
               break;
         }
         Class<?> returnType = method.getReturnType();
         if (returnType == long.class || returnType == Long.class) {
            return id * 31;
         } else if (returnType == int.class || returnType == Integer.class) {
            return (int) (id % 100);
         } else if (returnType == boolean.class || returnType == Boolean.class) {
            return (id & 1) == 0;
         } else if (returnType == String.class) {
            return "value-" + id;
         } else if (returnType == SimpleString.class) {
            return simpleName;
         }
         return null;
      });
      return type.cast(proxy);
   }
}

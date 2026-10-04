package org.apache.activemq.artemis.tests.performance.jmh;

import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.paging.cursor.impl.PageSubscriptionCounterImpl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.utils.ArtemisCloseable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class PageSubscriptionCounterBenchmark {

   private static final int[] THREAD_COUNTS = {1, 4, 16};

   private PageSubscriptionCounterImpl counter;
   private Transaction mockTx;

   @Setup(Level.Trial)
   public void setup() throws Exception {
      StorageManager storageManager = (StorageManager) Proxy.newProxyInstance(
            StorageManager.class.getClassLoader(),
            new Class<?>[]{StorageManager.class},
            (proxy, method, args) -> {
               if ("closeableReadLock".equals(method.getName())) {
                  return (ArtemisCloseable) () -> {};
               }
               if ("storePageCounter".equals(method.getName()) ||
                     "generateID".equals(method.getName())) {
                  return 1L;
               }
               if ("isStarted".equals(method.getName())) {
                  return true;
               }
               if (method.getReturnType().equals(boolean.class)) return false;
               if (method.getReturnType().equals(int.class)) return 0;
               if (method.getReturnType().equals(long.class)) return 0L;
               return null;
            }
      );

      mockTx = (Transaction) Proxy.newProxyInstance(
            Transaction.class.getClassLoader(),
            new Class<?>[]{Transaction.class},
            (proxy, method, args) -> {
               if ("getID".equals(method.getName())) return 1L;
               if (method.getReturnType().equals(boolean.class)) return false;
               if (method.getReturnType().equals(int.class)) return 0;
               if (method.getReturnType().equals(long.class)) return 0L;
               return null;
            }
      );

      counter = new PageSubscriptionCounterImpl(storageManager, 1L);

      counter.loadValue(1L, 100_000_000L, 100_000_000_000L);
   }

   @Benchmark
   public void benchmarkProcessAdd() throws Exception {
      counter.increment(null, 1, 1024L);
   }

   @Benchmark
   public void benchmarkProcessAck() throws Exception {
      counter.increment(null, -1, -1024L);
   }

   @Benchmark
   public void benchmarkProcessMixed(ThreadState state) throws Exception {
      if (state.toggle) {
         counter.increment(null, 1, 1024L);
      } else {
         counter.increment(null, -1, -1024L);
      }

      state.toggle = !state.toggle;
   }

   @Benchmark
   public void benchmarkDelete() throws Exception {
      counter.delete(mockTx);
   }

   @State(Scope.Thread)
   public static class ThreadState {
      boolean toggle = true;
   }

   public static void main(String[] args) throws RunnerException {
      for (int threadCount : THREAD_COUNTS) {
         Options opt = new OptionsBuilder()
               .include(PageSubscriptionCounterBenchmark.class.getSimpleName())
               .threads(threadCount)
               .forks(1)
               .build();

         new Runner(opt).run();
      }
   }
}


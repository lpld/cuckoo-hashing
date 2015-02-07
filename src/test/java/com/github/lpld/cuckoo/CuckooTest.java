package com.github.lpld.cuckoo;

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author leopold
 * @since 3/02/15
 */
public class CuckooTest {

  @Test
  public void testSimpleScenario() {
    Map<String, String> simpleMap = new NotThreadSafeCuckooHashMap<String, String>();
//
    String key1 = "1";
//    String key2 = "ABC";
//    String key3 = "$%^&*";
//    String key4 = "zxcvbnm,.asdfghjkl;qwertyuio";
//
//    assertNull(simpleMap.get(key1));
//    assertNull(simpleMap.get(key2));
//    assertNull(simpleMap.get(key3));
//    assertNull(simpleMap.get(key4));

    // simple scenario
    simpleMap.put(key1, "value11");
    assertEquals("value11", simpleMap.get(key1));

    String prev = simpleMap.put(key1, "value12");
    assertEquals("value11", prev);
    assertEquals("value12", simpleMap.get(key1));

    String prev2 = simpleMap.remove(key1);
    assertEquals("value12", prev2);
    assertNull(simpleMap.get(key1));


  }

  @Test
  public void testMultipleKeys() {
    Map<String, String> simpleMap = new ThreadSafeCuckooHashMap<String, String>(512);

    // more complex scenario:

    final int itcount = 340;

    // putting values:
    for (int i = 0; i < itcount; i++) {

      String key = "key_" + i;
      String value = "value_" + i + "_1";

      simpleMap.put(key, value);
    }

    // reading values and putting new:
    for (int i = 0; i < itcount; i++) {
      String key = "key_" + i;
      String valueOld = "value_" + i + "_1";
      String valueNew = "value_" + i + "_2";

      assertEquals(valueOld, simpleMap.get(key));
      assertEquals(valueOld, simpleMap.put(key, valueNew));
    }

    // reading again and removing
    for (int i = 0; i < itcount; i++) {
      String key = "key_" + i;
      String value = "value_" + i + "_2";

      assertEquals(value, simpleMap.get(key));
      assertEquals(value, simpleMap.remove(key));
    }

    // reading again:
    for (int i = 0; i < itcount; i++) {
      String key = "key_" + i;

      assertNull(simpleMap.get(key));
    }
  }

  @Test
//  @Ignore
  public void testParallelInsert() throws InterruptedException {
    final Map<String, String> simpleMap = new ThreadSafeCuckooHashMap<String, String>(2048);
//    final Map<String, String> simpleMap = new ConcurrentHashMap<String, String>();

    final int threadsCount = 20;
    final int iterations = 50;
    final int expectedSize = threadsCount * iterations;

    ExecutorService service = Executors.newFixedThreadPool(threadsCount);
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch end = new CountDownLatch(threadsCount);

    for (int i = 0; i < threadsCount; i++) {

      final int ii = i;
      service.submit(new Runnable() {
        @Override
        public void run() {
          try {
            start.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          try {
            for (int j = 0; j < iterations; j++) {
              int idx = ii + threadsCount * j;

              final String key = "key_" + idx;
              final String value = "value_" + idx;
              System.out.println("[" + ii + "] PUT " + key + ":" + value);
              assertNull(simpleMap.put(key, value));

              int readIdx = (idx - threadsCount) % expectedSize;

              System.out.println("[" + ii + "] READ key_" + readIdx);
              String v = simpleMap.get("key_" + readIdx);
              System.out.println("[" + ii + "] READ key_" + readIdx + ":" + v);

              if (readIdx > idx || readIdx < 0) {
                assertNull(v);
              } else {
                assertEquals("value_" + readIdx, v);
              }

            }
          } finally {
            end.countDown();
          }

        }
      });
    }

    start.countDown();
    end.await();


    for (int i = 0; i < expectedSize; i++) {
      int idx = i;
      final String key = "key_" + idx;
      final String value = "value_" + idx;
      assertEquals(value, simpleMap.get(key));
    }


  }
}
package com.github.lpld.cuckoo;

import org.junit.Test;

import java.util.Map;

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

    final int itcount = 250;

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
}
package com.github.lpld.cuckoo;

import java.util.AbstractMap;
import java.util.Set;

/**
 * @author leopold
 * @since 3/02/15
 */
public class NotThreadSafeCuckooHashMap<K, V> extends AbstractMap<K, V> {


  private static final int DEFAULT_CAPACITY = 16;
  private static final int TABLES_COUNT = 2;

  int capacity;

  int MAX_ROUTE = 10;

  SimpleEntry<K, V>[][] tables;


  public NotThreadSafeCuckooHashMap(int capacity) {
    this.capacity = capacity;
    tables = new SimpleEntry[TABLES_COUNT][capacity];
  }

  public NotThreadSafeCuckooHashMap() {
    this(DEFAULT_CAPACITY);
  }

  @Override
  public V put(K key, V value) {
    int h1 = hash1(key);
    int idx1 = indexFor(h1);
    SimpleEntry<K, V> existingValue = null;

    SimpleEntry<K, V> e1 = tables[0][idx1];
    if (e1 != null && e1.getKey().equals(key)) {
      existingValue = e1;
    }

    int h2 = hash2(key);
    int idx2 = indexFor(h2);
    SimpleEntry<K, V> e2 = tables[1][idx2];
    if (e2 != null && e2.getKey().equals(key)) {
      existingValue = e2;
    }

    if (existingValue == null) {
      if (e1 == null) {
        tables[0][idx1] = new SimpleEntry<K, V>(key, value);
      } else if (e2 == null) {
        tables[1][idx2] = new SimpleEntry<K, V>(key, value);
      } else {

        if (relocate(0, idx1)) {
          tables[0][idx1] = new SimpleEntry<K, V>(key, value);
        } else if (relocate(1, idx2)) {
          tables[1][idx2] = new SimpleEntry<K, V>(key, value);
        } else {
          throw new IllegalStateException("rehashing needed");
        }

//        throw new UnsupportedOperationException("Not implemented yet");
      }

      return null;
    } else {
      return existingValue.setValue(value);
    }
  }

  private boolean relocate(int table, int index) {
    int[] route = new int[MAX_ROUTE];

    boolean pathFound = false;
    int depth = 0;
    do {
      SimpleEntry<K, V> e = tables[table][index];
      if (e == null) {
        pathFound = true;
      } else {
        route[depth] = index;
        table = 1 - table;
        int hash = table == 0 ? hash1(e.getKey()) : hash2(e.getKey());
        index = indexFor(hash);
        depth++;
      }


    } while (!pathFound && depth < MAX_ROUTE);

    if (!pathFound) {
      return false;
    }

    table = 1 - table;
    for (int i = depth - 1; i >= 0; i--, table = 1 - table) {
      index = route[i];
      SimpleEntry<K, V> e = tables[table][index];

      int hash = table == 0 ? hash2(e.getKey()) : hash1(e.getKey());
      int destIndex = indexFor(hash);

      tables[1 - table][destIndex] = e;
    }

    return true;

  }

  @Override
  public V remove(Object key) {
    int h1 = hash1(key);
    int idx1 = indexFor(h1);
    SimpleEntry<K, V> existingValue = null;

    SimpleEntry<K, V> e1 = tables[0][idx1];
    if (e1 != null && e1.getKey().equals(key)) {
      existingValue = e1;
    } else {
      e1 = null;
    }

    int h2 = hash2(key);
    int idx2 = indexFor(h2);
    SimpleEntry<K, V> e2 = tables[1][idx2];
    if (e2 != null && e2.getKey().equals(key)) {
      existingValue = e2;
    } else {
      e2 = null;
    }

    if (existingValue == null) {
      return null;
    } else {
      if (e1 == null) {
        tables[1][idx2] = null;
        return e2.getValue();
      } else {
        tables[0][idx1] = null;
        return e1.getValue();
      }
    }
  }

  @Override
  public V get(Object key) {
    SimpleEntry<K, V> entry = findInternal(key);

    return entry == null ? null : entry.getValue();
  }

  private SimpleEntry<K, V> findInternal(Object key) {
    int h1 = hash1(key);
    SimpleEntry<K, V> e1 = tables[0][indexFor(h1)];
    if (e1 != null && e1.getKey().equals(key)) {
      return e1;
    }

    int h2 = hash2(key);
    SimpleEntry<K, V> e2 = tables[1][indexFor(h2)];
    if (e2 != null && e2.getKey().equals(key)) {
      return e2;
    }

    return null;
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return null;
  }

  int hash1(Object key) {
    int h = key.hashCode();

    return h ^ (h >>> 16);

  }

  int hash2(Object key) {
    int h = key.hashCode();

    h ^= (h >>> 20) ^ (h >>> 12);
    return h ^ (h >>> 7) ^ (h >>> 4);
  }

  int indexFor(int h) {
    return h & (capacity - 1);
  }


}

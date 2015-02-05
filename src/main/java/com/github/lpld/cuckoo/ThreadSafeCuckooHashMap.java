package com.github.lpld.cuckoo;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

/**
 * @author leopold
 * @since 3/02/15
 */
public class ThreadSafeCuckooHashMap<K, V> extends AbstractMap<K, V> {


  private static final int DEFAULT_CAPACITY = 16;
  private static final int TABLES_COUNT = 2;

  int capacity;

  int MAX_ROUTE = 10;

  Entry<K, V>[][] tables;
  int[][] timestamps;

  public ThreadSafeCuckooHashMap(int capacity) {
    this.capacity = capacity;
    tables = new Entry[TABLES_COUNT][capacity];
    timestamps = new int[TABLES_COUNT][capacity];
  }

  public ThreadSafeCuckooHashMap() {
    this(DEFAULT_CAPACITY);
  }

  @Override
  public V put(K key, V value) {
    int idx1 = indexFor(hash1(key));
    int idx2 = indexFor(hash2(key));

    while (true) {

      FindResult findResult = internalFind(key);

      if (findResult.table >= 0) {
        Entry<K, V> e = findResult.table == 0 ? findResult.e1 : findResult.e2;
        return e.setValue(value);
      }

      if (findResult.e1 == null) {
        // todo make it atomic

        tables[0][idx1] = new Entry<K, V>(key, value);
        return null;
      }

      if (findResult.e2 == null) {
        // todo make it atomic
        tables[1][idx2] = new Entry<K, V>(key, value);
        return null;
      }

      // no room, relocation is needed
      if (relocate(0, idx1)) {
        continue;
      } else {
        throw new IllegalStateException("rehashing needed");
      }

    }

  }

  private boolean relocate(int table, int index) {
    int[] route = new int[MAX_ROUTE];

    boolean pathFound = false;
    int depth = 0;
    do {
      Entry<K, V> e = tables[table][index];
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
      Entry<K, V> e = tables[table][index];

      int hash = table == 0 ? hash2(e.getKey()) : hash1(e.getKey());
      int destIndex = indexFor(hash);

      tables[1 - table][destIndex] = e;
    }

    return true;

  }

  @Override
  public V remove(Object key) {
    int idx1 = indexFor(hash1(key));
    int idx2 = indexFor(hash2(key));

    FindResult findResult = internalFind(key);

    if (findResult.table == 0) {
      tables[0][idx1] = new Entry<K, V>(findResult.e1.timestamp);
      return findResult.e1.getValue();
    } else if (findResult.table == 1) {

      // todo why do we need this?
      // if (table[0][h1]!=<ent1,ts1>)
      // continue

      tables[1][idx2] = new Entry<K, V>(findResult.e2.timestamp);;
      return findResult.e2.getValue();
    } else {
      return null;
    }

  }

  @Override
  public V get(Object key) {
    boolean counterOk;
    do {
      int idx1 = indexFor(hash1(key));
      Entry<K, V> e1 = tables[0][idx1];
      if (e1 != null && key.equals(e1.key)) {
        return e1.getValue();
      }

      int idx2 = indexFor(hash2(key));
      Entry<K, V> e2 = tables[1][idx2];
      if (e2 != null && key.equals(e2.key)) {
        return e2.getValue();
      }

      // trying once again.
      Entry<K, V> ee1 = tables[0][idx1];
      if (ee1 != null && key.equals(ee1.key)) {
        return ee1.getValue();
      }

      Entry<K, V> ee2 = tables[1][idx2];
      if (ee2 != null && key.equals(ee2.key)) {
        return ee2.getValue();
      }

      // todo check timestamps
      counterOk = true;

    } while (!counterOk);

    return null;
  }

  private FindResult internalFind(Object key) {
    int idx1 = indexFor(hash1(key));
    int idx2 = indexFor(hash2(key));
    int table = -1;

    Entry<K, V> e1 = null;
    Entry<K, V> e2 = null;

    Entry<K, V> e1Prev;
    Entry<K, V> e2Prev;

    while (true) {
      e1Prev = e1;
      e2Prev = e2;

      e1 = tables[0][idx1];
      if (!Entry.isNull(e1)) {
        if (e1.isOngoingRelocation()) {
          // help relocation
          continue;
        }

        if (e1.key.equals(key)) {
          table = 0;
        }
      }

      e2 = tables[1][idx2];

      if (!Entry.isNull(e2)) {
        if (e2.isOngoingRelocation()) {
          // help relocate
          continue;
        }

        if (e2.key.equals(key)) {
          if (table == 0) { // already found
            // delete duplicate
          } else {
            table = 1;
          }
        }
      }

      if (table >= 0) {
        return new FindResult(table, e1, e2);
      }

//      if (checkCounter)
      // continue;

      return new FindResult(-1, e1, e2);

    }

  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return null;
  }

  private static class Entry<K, V> implements Map.Entry<K, V> {

    public Entry(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public Entry(K key, V value, int timestamp) {
      this.key = key;
      this.value = value;
      this.timestamp = timestamp;
    }

    public Entry(int timestamp) {
      this.key = null;
      this.timestamp = timestamp;
    }

    private final K key;
    private V value;
    private int timestamp;

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      V old = this.value;
      this.value = value;
      return old;
    }

    private static boolean isNull(Entry<?, ?> e) {
      return e == null || e.getKey() == null;
    }

    private boolean isOngoingRelocation() {
      return false;
    }
  }

  private class FindResult {

    private final int table;
    private final Entry<K, V> e1;
    private final Entry<K, V> e2;

    public FindResult(int table, Entry<K, V> e1, Entry<K, V> e2) {
      this.table = table;
      this.e1 = e1;
      this.e2 = e2;
    }
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

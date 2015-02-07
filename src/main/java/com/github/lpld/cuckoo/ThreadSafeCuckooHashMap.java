package com.github.lpld.cuckoo;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author leopold
 * @since 3/02/15
 */
public class ThreadSafeCuckooHashMap<K, V> extends AbstractMap<K, V> {


  private static final int DEFAULT_CAPACITY = 16;

  int capacity;

  int MAX_ROUTE = 10;

  AtomicReferenceArray<Entry<K, V>>[] tables;
  AtomicIntegerArray[] flags;

  public ThreadSafeCuckooHashMap(int capacity) {
    this.capacity = capacity;

    this.tables = new AtomicReferenceArray[]{
        new AtomicReferenceArray<Entry<K, V>>(capacity),
        new AtomicReferenceArray<Entry<K, V>>(capacity)
    };

    this.flags = new AtomicIntegerArray[]{
        new AtomicIntegerArray(capacity),
        new AtomicIntegerArray(capacity)
    };
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
        if (tables[0].compareAndSet(idx1, null, new Entry<K, V>(key, value))) {
          return null;
        }
        continue;
      }

      if (findResult.e2 == null) {
        if (tables[1].compareAndSet(idx2, null, new Entry<K, V>(key, value))) {
          return null;
        }
        continue;
      }

      // no room, relocation is needed
      if (relocate(0, idx1)) {
        continue;
      } else {
        throw new IllegalStateException("rehashing needed");
      }

    }

  }

  private boolean checkCounters(int f1, int f2, int ff1, int ff2) {

    boolean result = timestamp(ff1) - timestamp(f1) < 2 &&
                     timestamp(ff2) - timestamp(f2) < 2 &&
                     timestamp(ff2) - timestamp(f1) < 3;
    if (!result) {
      System.out.println("-- Counters differ");
    }

    return result;
  }

  private boolean relocate(final int table, final int index) {
    int[] route = new int[MAX_ROUTE];
    int startLevel = 0;

    int tbl = table;
    int idx = index;

    while (true) {
      int depth = getCuckooPath(route, startLevel, tbl, idx);

      if (depth < 0) {
        return false;
      }

//       - 1;

      if (depth % 2 == 0)  {
        tbl = 1 - tbl; // + 1 - (depth % 2);

      }

      boolean ok = true;

      for (int i = depth - 1; i >= 0; i--, tbl = 1 - tbl) {
        idx = route[i];
        Entry<K, V> e = getAndRelocate(tbl, idx);
        if (e == null) {
          continue; // really ?
        }

        int hash = tbl == 0 ? hash2(e.getKey()) : hash1(e.getKey());
        int destIndex = indexFor(hash);

        Entry<K, V> old = tables[1 - tbl].get(destIndex);

        if (old != null) {
          startLevel = i + 1;
          idx = destIndex;
          tbl = 1 - tbl;
          ok = false;
          break;
        }
        helpRelocate(tbl, idx, true);

      }

      if (ok) {
        return true;
      }
    }
  }

  private int getCuckooPath(int[] route, int startLevel, int table, int index) {

    int depth = startLevel;
    Entry<K, V> prev = null;
    int prevIdx = -1;
    while (true) {
      Entry<K, V> e = getAndRelocate(table, index);

      if (e == null) {
        return depth;
      }

      if (prev != null && e.key.equals(prev.key)) {
        if (table == 0) {
          deleteDuplicate(prev, prevIdx);
        } else {
          deleteDuplicate(e, index);
        }
      }

      route[depth] = index;
      table = 1 - table;
      int hash = table == 0 ? hash1(e.getKey()) : hash2(e.getKey());
      index = indexFor(hash);
      depth++;
      prev = e;
      prevIdx = index;

      if (depth >= MAX_ROUTE) {
        return -1;
      }
    }

  }

  private Entry<K, V> getAndRelocate(int table, int index) {
    Entry<K, V> e;
    int f;
    int eFlags = -1;
    do {
      e = tables[table].get(index);
      f = eFlags;
      eFlags = flags[table].get(index);

      if (f == eFlags && isMarked(eFlags)) {
        helpRelocate(table, index, false);
        f = -1;
      }

    } while (f != eFlags);

    return e;
  }

  private boolean helpRelocate(int table, int index, boolean doMark) {
    while (true) {

      int f;

      // read source and mark it for relocation
      Entry<K, V> source;
      int sFlags = -1;

      do {
        source = tables[table].get(index);

        if (source == null) {
          return true;
        }

        f = sFlags;
        sFlags = flags[table].get(index);

        if (f == sFlags && doMark && !isMarked(sFlags)) {
          flags[table].compareAndSet(index, sFlags, mark(sFlags));
          f = -1; // we need read source once again
        }
      } while (f != sFlags);

      if (!isMarked(sFlags)) {
        return true;
      }

      // read destination
      Entry<K, V> dest;
      int destTable = 1 - table;
      int hash = destTable == 0 ? hash1(source.getKey()) : hash2(source.getKey());
      int destIdx = indexFor(hash);
      int dFlags = -1;

      do {
        dest = tables[destTable].get(destIdx);
        f = dFlags;
        dFlags = flags[destTable].get(destIdx);
      } while (f != dFlags);

      int sourceTs = timestamp(sFlags);
      int destTs = timestamp(dFlags);

      if (dest == null) {
        int newTs = Math.max(sourceTs, destTs) + 1;
        if (tables[table].get(index) != source) {
          continue;
        }

        // updating destination
        if (tables[destTable].compareAndSet(destIdx, null, source)) {
          tables[table].compareAndSet(index, source, null); // setting source to null

          // updating timestamps
          flags[table].compareAndSet(index, sFlags, updateTimestamp(sFlags, sourceTs + 1));
          flags[destTable].compareAndSet(destIdx, dFlags, updateTimestamp(dFlags, newTs));
          return true;
        }
      }

      // means that someone has already moved the entry
      if (source == dest) {
        tables[table].compareAndSet(index, source, null); // setting source to null
        flags[table].compareAndSet(index, sFlags, updateTimestamp(sFlags, sourceTs + 1));
        return true;
      }

      // unmarking
      flags[table].compareAndSet(index, sFlags, unmark(sFlags));
      return false;
    }
  }

  @Override
  public V remove(Object key) {
    int idx1 = indexFor(hash1(key));
    int idx2 = indexFor(hash2(key));

    while (true) {
      FindResult findResult = internalFind(key);

      if (findResult.table == 0) {
        if (tables[0]
            .compareAndSet(idx1, findResult.e1, null)) {
          return findResult.e1.getValue();
        }

        continue;
      }

      if (findResult.table == 1) {

        // not sure why we need this:
        if (tables[0].get(idx1) != findResult.e1) {
          continue;
        }

        if (tables[1]
            .compareAndSet(idx2, findResult.e2, null)) {
          return findResult.e2.getValue();
        }

        continue;
      }

      return null;
    }
  }

  private void deleteDuplicate(Entry<K, V> e2, int idx2) {
    tables[0].compareAndSet(idx2, e2, null);
  }

  @Override
  public V get(Object key) {
    int idx1 = indexFor(hash1(key));
    int idx2 = 0;

    Entry<K, V> e1;
    Entry<K, V> e2;
    int e1Flags = -1;
    int e2Flags = -1;

    int e1PrevFlags;
    int e2PrevFlags;

    boolean first = true;

    while (true) {
      e1PrevFlags = e1Flags;
      e2PrevFlags = e2Flags;
      int f;

      do {
        e1 = tables[0].get(idx1);
        f = e1Flags;
        e1Flags = flags[0].get(idx1);
      } while (f != e1Flags);

      if (e1 != null && key.equals(e1.key)) {
        return e1.getValue();
      }

      if (first) {
        idx2 = indexFor(hash2(key));
      }

      do {
        e2 = tables[1].get(idx2);
        f = e2Flags;
        e2Flags = flags[1].get(idx2);
      } while (f != e2Flags);

      if (e2 != null && key.equals(e2.key)) {
        return e2.getValue();
      }

      if (first || !checkCounters(e1PrevFlags, e2PrevFlags, e1Flags, e2Flags)) {
        first = false;
        continue;
      }

      return null;
    }
  }

  private FindResult internalFind(Object key) {
    int idx1 = indexFor(hash1(key));
    int idx2 = indexFor(hash2(key));
    int table = -1;

    Entry<K, V> e1;
    Entry<K, V> e2;
    int e1Flags = -1;
    int e2Flags = -1;

    int e1PrevFlags;
    int e2PrevFlags;

    boolean first = true;

    while (true) {
      e1PrevFlags = e1Flags;
      e2PrevFlags = e2Flags;
      int f;

      do {
        e1 = tables[0].get(idx1);
        f = e1Flags;
        e1Flags = flags[0].get(idx1);
      } while (f != e1Flags);

      if (e1 != null) {
        if (isMarked(e1Flags)) {
          helpRelocate(0, idx1, false);
          first = true;
          continue;
        }

        if (e1.key.equals(key)) {
          table = 0;
        }
      }

      do {
        e2 = tables[1].get(idx2);
        f = e2Flags;
        e2Flags = flags[1].get(idx2);
      } while (f != e2Flags);

      if (e2 != null) {
        if (isMarked(e2Flags)) {
          helpRelocate(1, idx2, false);
          first = true;
          continue;
        }

        if (e2.key.equals(key)) {
          if (table == 0) { // already found
            deleteDuplicate(e2, idx2);
          } else {
            table = 1;
          }
        }
      }

      if (table >= 0) {
        return new FindResult(table, e1, e2);
      }

      if (first || !checkCounters(e1PrevFlags, e2PrevFlags, e1Flags, e2Flags)) {
        first = false;
        continue;
      }

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

    private final K key;
    private V value;

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

  static int hash2(Object key) {
    int h = key.hashCode();

    return h ^ (h >>> 16);
  }

  static int hash1(Object key) {
    int h = key.hashCode();

    h ^= (h >>> 20) ^ (h >>> 12);
    return h ^ (h >>> 7) ^ (h >>> 4);
  }

  int indexFor(int h) {
    return h & (capacity - 1);
  }

  static boolean isMarked(int flags) {
    return (flags & 1) == 1;
  }

  static int mark(int flags) {
    return flags | 1;
  }

  static int markBit(int flags) {
    return flags & 1;
  }

  static int unmark(int flags) {
    return flags ^ 1;
  }

  static int timestamp(int flags) {
    return flags >> 1;
  }

  static int updateTimestamp(int flags, int newTs) {
    return newTs << 1 | (flags & 1);
  }

}

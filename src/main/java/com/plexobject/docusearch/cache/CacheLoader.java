package com.plexobject.docusearch.cache;

public interface CacheLoader<K, V> {
    V get(K key);
}

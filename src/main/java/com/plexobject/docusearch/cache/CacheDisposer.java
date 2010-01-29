package com.plexobject.docusearch.cache;

public interface CacheDisposer<T> {
    void dispose(T object);
}

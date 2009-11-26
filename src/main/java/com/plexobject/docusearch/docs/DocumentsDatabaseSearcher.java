package com.plexobject.docusearch.docs;

import java.util.Collection;

import org.apache.lucene.store.Directory;

import com.plexobject.docusearch.domain.Document;

public interface DocumentsDatabaseSearcher {

    public abstract Collection<Document> query(final String database,
            final String owner, final String keywords,
            final boolean includeSuggestions, final int startKey,
            final int limit);

    public abstract Collection<Document> query(final String database,
            final Directory dir, final String owner, final String keywords,
            final boolean includeSuggestions, final int startKey,
            final int limit);

}
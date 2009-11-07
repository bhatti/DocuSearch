package com.plexobject.docusearch.persistence;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.plexobject.docusearch.domain.Document;

public class SimpleDocumentsIterator implements Iterator<List<Document>> {
    private final List<Document> docs;
    private boolean consumed;

    public SimpleDocumentsIterator(final Document... docs) {
        this(Arrays.asList(docs));
    }

    public SimpleDocumentsIterator(final List<Document> docs) {
        this.docs = docs;
    }

    @Override
    public boolean hasNext() {
        return !consumed;
    }

    @Override
    public List<Document> next() {
        consumed = true;
        return docs;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}

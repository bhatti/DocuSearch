package com.plexobject.docusearch.index;

import java.util.Iterator;
import java.util.List;

import com.plexobject.docusearch.domain.Document;

/**
 * @author Shahzad Bhatti
 * 
 */
public interface Indexer {
    /**
     * This method updates index with given documents
     * 
     * @param policy
     *            - index policy
     * @param docsIt
     *            - iterator that returns collection of documents
     * @param deleteExisting
     *            - deletes existing indexed document, otherwise adds terms to
     *            existing document
     * @return number of documents that were indexed successfully.
     */
    public int index(IndexPolicy policy, Iterator<List<Document>> docsIt,
            boolean deleteExisting);
}

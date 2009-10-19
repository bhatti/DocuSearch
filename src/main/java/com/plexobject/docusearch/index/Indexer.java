package com.plexobject.docusearch.index;

import java.util.Collection;

import com.plexobject.docusearch.domain.Document;

/**
 * @author bhatti@plexobject.com
 * 
 */
public interface Indexer {
    /**
	 * This method updates index with given documents
	 * @param policy - index policy
	 * @param docs - documents
	 * @return number of documents that were indexed successfully.
	 */
	public int index(IndexPolicy policy, Collection<Document> docs);
}

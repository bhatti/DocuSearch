package com.plexobject.docusearch.docs.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.store.Directory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.docs.DocumentsDatabaseSearcher;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.query.CriteriaBuilder;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.SearchDocList;
import com.plexobject.docusearch.query.lucene.QueryImpl;
import com.sun.jersey.spi.inject.Inject;

@Component("documentsDatabaseSearcher")
public class DocumentsDatabaseSearcherImpl implements DocumentsDatabaseSearcher {
    private final Map<String, Query> cachedQueries = new HashMap<String, Query>();
    @Inject
    @Autowired
    DocumentRepository documentRepository;

    @Inject
    @Autowired
    ConfigurationRepository configRepository;

    public DocumentsDatabaseSearcherImpl() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.plexobject.docusearch.docs.DocumentsDatabaseSearcher#query(java.lang.String
     * , java.lang.String, java.lang.String, boolean, int, int)
     */
    public Collection<Document> query(final String database,
            final String owner, final String keywords,
            final boolean includeSuggestions, final int startKey,
            final int limit) {
        final File dir = new File(LuceneUtils.INDEX_DIR, database);
        return query(database, LuceneUtils.toFSDirectory(dir), owner, keywords,
                includeSuggestions, startKey, limit);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.plexobject.docusearch.docs.DocumentsDatabaseSearcher#query(java.lang.String
     * , org.apache.lucene.store.Directory, java.lang.String, java.lang.String,
     * boolean, int, int)
     */
    public Collection<Document> query(final String database,
            final Directory dir, final String owner, final String keywords,
            final boolean includeSuggestions, final int startKey,
            final int limit) {
        final Timer timer = Metric.newTimer("DocumentsDatabaseSearcher.query");
        final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                keywords).setOwner(owner).build();
        final Query query = newQuery(dir, database);
        QueryPolicy queryPolicy = configRepository.getQueryPolicy(database);

        SearchDocList results = query.search(criteria, null, queryPolicy,
                includeSuggestions, startKey, limit);
        Collection<Document> docs = new ArrayList<Document>();
        for (SearchDoc result : results) {
            Document doc = documentRepository.getDocument(database, result
                    .getId());
            docs.add(doc);
        }
        timer.stop();
        return docs;
    }

    /**
     * @return the documentRepository
     */
    public DocumentRepository getDocumentRepository() {
        return documentRepository;
    }

    /**
     * @param documentRepository
     *            the documentRepository to set
     */
    public void setDocumentRepository(DocumentRepository documentRepository) {
        this.documentRepository = documentRepository;
    }

    /**
     * @return the configRepository
     */
    public ConfigurationRepository getConfigRepository() {
        return configRepository;
    }

    /**
     * @param configRepository
     *            the configRepository to set
     */
    public void setConfigRepository(ConfigurationRepository configRepository) {
        this.configRepository = configRepository;
    }

    synchronized Query newQuery(final Directory dir, final String database) {
        Query query = cachedQueries.get(database);
        if (query == null) {
            query = new QueryImpl(dir, database);
            cachedQueries.put(database, query);
        }
        return query;

    }

}

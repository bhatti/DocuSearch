package com.plexobject.docusearch.docs.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.docs.DocumentsDatabaseIndexer;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.index.Indexer;
import com.plexobject.docusearch.index.lucene.IndexerImpl;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.DocumentsIterator;
import com.plexobject.docusearch.persistence.SimpleDocumentsIterator;
import com.sun.jersey.spi.inject.Inject;

@Component("documentsDatabaseIndexer")
public class DocumentsDatabaseIndexerImpl implements DocumentsDatabaseIndexer {
    interface DocumentTransformer {
        List<Document> transform(List<Document> docs);
    }

    class TransformableDocumentsIterator implements Iterator<List<Document>> {
        private final Iterator<List<Document>> docIt;
        private final DocumentTransformer transformer;

        public TransformableDocumentsIterator(final String database,
                final int limit, final DocumentTransformer transformer) {
            this(database, null, limit, transformer);
        }

        public TransformableDocumentsIterator(final String database,
                final String startKey, final int limit,
                final DocumentTransformer transformer) {
            this.docIt = new DocumentsIterator(documentRepository, database,
                    startKey, limit);
            this.transformer = transformer;
        }

        @Override
        public boolean hasNext() {
            return docIt.hasNext();
        }

        @Override
        public List<Document> next() {
            return transformer.transform(docIt.next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    @SuppressWarnings("unused")
    private static final Logger LOGGER = Logger
            .getLogger(DocumentsDatabaseIndexerImpl.class);
    @Autowired
    @Inject
    DocumentRepository documentRepository;

    @Autowired
    @Inject
    ConfigurationRepository configRepository;

    private final Map<File, Indexer> cachedIndexers = new HashMap<File, Indexer>();

    public DocumentsDatabaseIndexerImpl() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.plexobject.docusearch.docs.DocumentsDatabaseIndexer#indexUsingPrimaryDatabase
     * (java.lang.String)
     */
    public int indexUsingPrimaryDatabase(final String index,
            final String policyName) {
        if (GenericValidator.isBlankOrNull(index)) {
            throw new IllegalArgumentException("index not specified");
        }
        final Timer timer = Metric
                .newTimer("DocumentsDatabaseIndexer.indexUsingPrimaryDatabase");
        final File dir = new File(LuceneUtils.INDEX_DIR, index);
        if (!dir.mkdirs() && !dir.exists()) {
            throw new RuntimeException("Failed to create directory " + dir);
        }
        final IndexPolicy policy = configRepository
                .getIndexPolicy(policyName == null ? index : policyName);

        final Iterator<List<Document>> docsIt = new DocumentsIterator(
                documentRepository, index, Configuration.getInstance()
                        .getPageSize());

        final int succeeded = indexDocuments(dir, policy, docsIt, true);

        timer.stop("succeeded indexing " + succeeded + " documents");
        return succeeded;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.plexobject.docusearch.docs.DocumentsDatabaseIndexer#indexUsingSecondaryDatabase
     * (java.lang.String, java.lang.String, java.lang.String, java.lang.String,
     * java.lang.String)
     */
    public int indexUsingSecondaryDatabase(final String index,
            final String policyName, final String sourceDatabase,
            final String joinDatabase, final String indexIdInJoinDatabase,
            final String sourceIdInJoinDatabase) {
        if (GenericValidator.isBlankOrNull(index)) {
            throw new IllegalArgumentException("index not specified");
        }
        if (GenericValidator.isBlankOrNull(sourceDatabase)) {
            throw new IllegalArgumentException("sourceDatabase not specified");
        }
        if (GenericValidator.isBlankOrNull(joinDatabase)) {
            throw new IllegalArgumentException("index joinDatabase specified");
        }
        if (GenericValidator.isBlankOrNull(indexIdInJoinDatabase)) {
            throw new IllegalArgumentException(
                    "indexIdInJoinDatabase not specified");
        }
        if (GenericValidator.isBlankOrNull(sourceIdInJoinDatabase)) {
            throw new IllegalArgumentException(
                    "sourceIdInJoinDatabase not specified");
        }
        final Timer timer = Metric
                .newTimer("DocumentsDatabaseIndexer.indexUsingSecondaryDatabase");

        final File dir = new File(LuceneUtils.INDEX_DIR, index);
        if (!dir.mkdirs() && !dir.exists()) {
            throw new RuntimeException("Failed to create directory " + dir);
        }
        final IndexPolicy policy = configRepository
                .getIndexPolicy(policyName == null ? (index + "_" + sourceDatabase)
                        : policyName);
        policy.add(sourceIdInJoinDatabase, true, false, false, 0.0F, false,
                false, false);

        final TransformableDocumentsIterator docsIt = new TransformableDocumentsIterator(
                joinDatabase, Configuration.getInstance().getPageSize(),
                new DocumentTransformer() {

                    @Override
                    public List<Document> transform(List<Document> docs) {
                        return createSecondaryDocuments(sourceDatabase,
                                indexIdInJoinDatabase, sourceIdInJoinDatabase,
                                docs);
                    }
                });

        final int succeeded = indexDocuments(dir, policy, docsIt, true);
        timer.stop("succeeded indexing " + succeeded + " documents");

        return succeeded;

    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.plexobject.docusearch.docs.DocumentsDatabaseIndexer#
     * updateIndexUsingPrimaryDatabase(java.lang.String, java.lang.String[])
     */
    public int updateIndexUsingPrimaryDatabase(final String index,
            final String policyName, final String[] docIds) {
        return updateIndexUsingPrimaryDatabase(index, policyName,
                getDocumentsById(index, docIds));
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.plexobject.docusearch.docs.DocumentsDatabaseIndexer#
     * updateIndexUsingPrimaryDatabase(java.lang.String, java.util.List)
     */
    public int updateIndexUsingPrimaryDatabase(final String index,
            final String policyName, final List<Document> docs) {
        if (GenericValidator.isBlankOrNull(index)) {
            throw new IllegalArgumentException("index not specified");
        }
        final Timer timer = Metric
                .newTimer("DocumentsDatabaseIndexer.updateIndexUsingPrimaryDatabase");

        final File dir = new File(LuceneUtils.INDEX_DIR, index);
        if (!dir.mkdirs() && !dir.exists()) {
            throw new RuntimeException("Failed to create directory " + dir);
        }
        final IndexPolicy policy = configRepository
                .getIndexPolicy(policyName == null ? index : policyName);

        final SimpleDocumentsIterator docsIt = new SimpleDocumentsIterator(docs);
        int succeeded = indexDocuments(dir, policy, docsIt, true);

        timer.stop(" succeeded indexing " + succeeded + "/" + docs.size()
                + " records of " + index + " with policy " + policy);
        return succeeded;
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.plexobject.docusearch.docs.DocumentsDatabaseIndexer#
     * updateIndexUsingSecondaryDatabase(java.lang.String, java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
     */
    public int updateIndexUsingSecondaryDatabase(final String index,
            final String policyName, final String sourceDatabase,
            final String joinDatabase, final String indexIdInJoinDatabase,
            final String sourceIdInJoinDatabase, final String[] docIds) {
        if (GenericValidator.isBlankOrNull(index)) {
            throw new IllegalArgumentException("index not specified");
        }
        if (GenericValidator.isBlankOrNull(sourceDatabase)) {
            throw new IllegalArgumentException("sourceDatabase not specified");
        }
        if (GenericValidator.isBlankOrNull(joinDatabase)) {
            throw new IllegalArgumentException("index joinDatabase specified");
        }
        if (GenericValidator.isBlankOrNull(indexIdInJoinDatabase)) {
            throw new IllegalArgumentException(
                    "indexIdInJoinDatabase not specified");
        }
        if (GenericValidator.isBlankOrNull(sourceIdInJoinDatabase)) {
            throw new IllegalArgumentException(
                    "sourceIdInJoinDatabase not specified");
        }
        final File dir = new File(LuceneUtils.INDEX_DIR, index);
        if (!dir.mkdirs() && !dir.exists()) {
            throw new RuntimeException("Failed to create directory " + dir);
        }

        final Timer timer = Metric
                .newTimer("DocumentsDatabaseIndexer.updateIndexUsingSecondaryDatabase");

        final List<Document> docs = getDocumentsById(joinDatabase, docIds);
        final List<Document> docsToIndex = createSecondaryDocuments(
                sourceDatabase, indexIdInJoinDatabase, sourceIdInJoinDatabase,
                docs);

        final IndexPolicy policy = configRepository
                .getIndexPolicy(policyName == null ? (index + "_" + sourceDatabase)
                        : policyName);
        final SimpleDocumentsIterator docsIt = new SimpleDocumentsIterator(
                docsToIndex);
        int succeeded = indexDocuments(dir, policy, docsIt, true);
        timer.stop("succeeded indexing " + succeeded + " documents");

        return succeeded;
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

    private List<Document> getDocumentsById(final String index,
            final String[] docIds) {
        final List<Document> docs = new ArrayList<Document>();
        for (String id : docIds) {
            Document doc = documentRepository.getDocument(index, id);
            docs.add(doc);
        }
        return docs;
    }

    private List<Document> createSecondaryDocuments(
            final String sourceDatabase, final String indexIdInJoinDatabase,
            final String sourceIdInJoinDatabase, final List<Document> docs) {
        final List<Document> docsToIndex = new ArrayList<Document>();

        for (Document doc : docs) {
            final String indexIdInJoinDatabaseValue = doc
                    .getProperty(indexIdInJoinDatabase);
            if (GenericValidator.isBlankOrNull(indexIdInJoinDatabaseValue)) {
                throw new IllegalArgumentException(indexIdInJoinDatabase
                        + " not specified in " + doc);
            }
            final String sourceIdInJoinDatabaseValue = doc
                    .getProperty(sourceIdInJoinDatabase);
            if (GenericValidator.isBlankOrNull(sourceIdInJoinDatabaseValue)) {
                throw new IllegalArgumentException(sourceIdInJoinDatabase
                        + " not specified in " + doc);
            }
            final Document sourceDoc = documentRepository.getDocument(
                    sourceDatabase, sourceIdInJoinDatabaseValue);
            final Document docToIndex = new DocumentBuilder(sourceDoc).setId(
                    indexIdInJoinDatabaseValue).put(sourceIdInJoinDatabase,
                    sourceIdInJoinDatabaseValue).build();
            docsToIndex.add(docToIndex);
        }
        return docsToIndex;
    }

    private int indexDocuments(final File dir, final IndexPolicy policy,
            Iterator<List<Document>> docsIt, final boolean deleteExisting) {
        final Indexer indexer = newIndexer(dir);

        return indexer.index(policy, docsIt, deleteExisting);
    }

    synchronized Indexer newIndexer(File dir) {
        Indexer indexer = cachedIndexers.get(dir);
        if (indexer == null) {
            indexer = new IndexerImpl(dir);
            cachedIndexers.put(dir, indexer);
        }
        return indexer;
    }
}

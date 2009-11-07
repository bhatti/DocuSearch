package com.plexobject.docusearch.docs;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.plexobject.docusearch.Configuration;
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
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.persistence.SimpleDocumentsIterator;

public class DocumentsDatabaseIndexer {
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
            this.docIt = new DocumentsIterator(repository, database, startKey,
                    limit);
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

    private static final Logger LOGGER = Logger
            .getLogger(DocumentsDatabaseIndexer.class);
    private final DocumentRepository repository;
    private final ConfigurationRepository configRepository;

    public DocumentsDatabaseIndexer(final RepositoryFactory repositoryFactory) {
        if (repositoryFactory == null) {
            throw new NullPointerException("RepositoryFactory not specified");
        }
        this.repository = repositoryFactory.getDocumentRepository();
        this.configRepository = repositoryFactory.getConfigurationRepository();
    }

    public void indexAllDatabases() {
        final String[] dbs = repository.getAllDatabases();
        indexDatabases(dbs);
    }

    public void indexDatabases(final String[] dbs) {
        for (String db : dbs) {
            indexUsingPrimaryDatabase(db);
        }

    }

    public int indexUsingPrimaryDatabase(final String index) {
        if (GenericValidator.isBlankOrNull(index)) {
            throw new IllegalArgumentException("index not specified");
        }
        final Timer timer = Metric
                .newTimer("DocumentsDatabaseIndexer.indexUsingPrimaryDatabase");
        final File dir = new File(LuceneUtils.INDEX_DIR, index);
        if (!dir.mkdirs() && !dir.exists()) {
            throw new RuntimeException("Failed to create directory " + dir);
        }
        final IndexPolicy policy = configRepository.getIndexPolicy(index);

        final Iterator<List<Document>> docsIt = new DocumentsIterator(
                repository, index, Configuration.getInstance().getPageSize());

        final int succeeded = indexDocuments(dir, policy, docsIt, true);

        timer.stop("succeeded indexing " + succeeded + " documents");
        return succeeded;
    }

    public int indexUsingSecondaryDatabase(final String index,
            final String sourceDatabase, final String joinDatabase,
            final String indexIdInJoinDatabase,
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
        final IndexPolicy policy = configRepository.getIndexPolicy(index + "_"
                + sourceDatabase);
        policy.add(sourceIdInJoinDatabase, true, false, false, 0.0F);

        final TransformableDocumentsIterator docsIt = new TransformableDocumentsIterator(
                joinDatabase, Configuration.getInstance().getPageSize(),
                new DocumentTransformer() {

                    @Override
                    public List<Document> transform(List<Document> docs) {
                        final List<Document> docsToIndex = new ArrayList<Document>();
                        for (Document doc : docs) {
                            final String indexIdInJoinDatabaseValue = doc
                                    .getProperty(indexIdInJoinDatabase);
                            if (GenericValidator
                                    .isBlankOrNull(indexIdInJoinDatabaseValue)) {
                                throw new IllegalArgumentException(
                                        indexIdInJoinDatabase
                                                + " not specified in " + doc);
                            }
                            final String sourceIdInJoinDatabaseValue = doc
                                    .getProperty(sourceIdInJoinDatabase);
                            if (GenericValidator
                                    .isBlankOrNull(sourceIdInJoinDatabaseValue)) {
                                throw new IllegalArgumentException(
                                        sourceIdInJoinDatabase
                                                + " not specified in " + doc);
                            }
                            final Document sourceDoc = repository
                                    .getDocument(sourceDatabase,
                                            sourceIdInJoinDatabaseValue);
                            final Document docToIndex = new DocumentBuilder(
                                    sourceDoc)
                                    .setId(indexIdInJoinDatabaseValue).put(
                                            sourceIdInJoinDatabase,
                                            sourceIdInJoinDatabaseValue)
                                    .build();
                            docsToIndex.add(docToIndex);
                        }
                        return docsToIndex;
                    }
                });

        final int succeeded = indexDocuments(dir, policy, docsIt, true);
        timer.stop("succeeded indexing " + succeeded + " documents");

        return succeeded;

    }

    public int updateIndexUsingPrimaryDatabase(final String index,
            final String[] docIds) {
        if (GenericValidator.isBlankOrNull(index)) {
            throw new IllegalArgumentException("index not specified");
        }
        final Timer timer = Metric
                .newTimer("DocumentsDatabaseIndexer.updateIndexUsingPrimaryDatabase");

        final File dir = new File(LuceneUtils.INDEX_DIR, index);
        if (!dir.mkdirs() && !dir.exists()) {
            throw new RuntimeException("Failed to create directory " + dir);
        }
        final IndexPolicy policy = configRepository.getIndexPolicy(index);

        final List<Document> docs = new ArrayList<Document>();
        for (String id : docIds) {
            Document doc = repository.getDocument(index, id);
            docs.add(doc);
        }
        final SimpleDocumentsIterator docsIt = new SimpleDocumentsIterator(docs);
        int succeeded = indexDocuments(dir, policy, docsIt, true);

        timer.stop(" succeeded indexing " + succeeded + "/" + docIds.length
                + " records of " + index + " with policy " + policy);
        return succeeded;
    }

    public int updateIndexUsingSecondaryDatabase(final String index,
            final String sourceDatabase, final String joinDatabase,
            final String indexIdInJoinDatabase,
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
        final Timer timer = Metric
                .newTimer("DocumentsDatabaseIndexer.updateIndexUsingSecondaryDatabase");
        final Map<String, Boolean> idsMap = new HashMap<String, Boolean>();
        for (String id : docIds) {
            idsMap.put(id, Boolean.TRUE);
        }
        final File dir = new File(LuceneUtils.INDEX_DIR, index);
        if (!dir.mkdirs() && !dir.exists()) {
            throw new RuntimeException("Failed to create directory " + dir);
        }
        IndexPolicy policy = null;
        try {
            policy = configRepository.getIndexPolicy(index + sourceDatabase);
        } catch (RuntimeException e) {
            policy = configRepository.getIndexPolicy(index);

        }
        final TransformableDocumentsIterator docsIt = new TransformableDocumentsIterator(
                joinDatabase, Configuration.getInstance().getPageSize(),
                new DocumentTransformer() {

                    @Override
                    public List<Document> transform(List<Document> docs) {
                        final List<Document> docsToIndex = new ArrayList<Document>();
                        for (Document doc : docs) {
                            final String indexIdInJoinDatabaseValue = doc
                                    .getProperty(indexIdInJoinDatabase);
                            if (GenericValidator
                                    .isBlankOrNull(indexIdInJoinDatabaseValue)) {
                                throw new IllegalArgumentException(
                                        indexIdInJoinDatabase
                                                + " not specified in " + doc);
                            }
                            final String sourceIdInJoinDatabaseValue = doc
                                    .getProperty(sourceIdInJoinDatabase);
                            if (GenericValidator
                                    .isBlankOrNull(sourceIdInJoinDatabaseValue)) {
                                throw new IllegalArgumentException(
                                        sourceIdInJoinDatabase
                                                + " not specified in " + doc);
                            }
                            if (idsMap.containsKey(sourceIdInJoinDatabaseValue)) {
                                final Document sourceDoc = repository
                                        .getDocument(sourceDatabase,
                                                sourceIdInJoinDatabaseValue);
                                final Document docToIndex = new DocumentBuilder(
                                        sourceDoc).setId(
                                        indexIdInJoinDatabaseValue).put(
                                        sourceIdInJoinDatabase,
                                        sourceIdInJoinDatabaseValue).build();
                                docsToIndex.add(docToIndex);
                            }
                        }
                        return docsToIndex;
                    }
                });
        final int succeeded = indexDocuments(dir, policy, docsIt, true);
        timer.stop("succeeded indexing " + succeeded + " documents");

        return succeeded;
    }

    private int indexDocuments(final File dir, final IndexPolicy policy,
            Iterator<List<Document>> docsIt, final boolean deleteExisting) {
        final Indexer indexer = new IndexerImpl(dir);

        return indexer.index(policy, docsIt, deleteExisting);
    }

    /**
     * @param args
     */
    public static void main(final String[] args) {
        Logger root = Logger.getRootLogger();
        root.setLevel(Level.INFO);

        root.addAppender(new ConsoleAppender(new PatternLayout(
                PatternLayout.TTCC_CONVERSION_PATTERN)));

        new DocumentsDatabaseIndexer(new RepositoryFactory())
                .indexAllDatabases();
        LOGGER.info("Indexed all documents");
    }
}

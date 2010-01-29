package com.plexobject.docusearch.docs;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.cache.CacheFlusher;
import com.plexobject.docusearch.docs.impl.DocumentsDatabaseIndexerImpl;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PagedList;

public class DocumentsDatabaseIndexerTest {
    private static Logger LOGGER = Logger.getRootLogger();
    private static final String DB_NAME = "DocumentsDatabaseIndexerTestDB";
    private static final String SECONDARY_TEST_DATUM_ID = "secondary_test_datum_id";
    private static final String TEST_DATUM_ID = "test_datum_id";
    private static final String TEST_DB_SECONDARY_TEST_DB = "test_data_secondary_test_data";
    private static final String SECONDARY_TEST_DB = "secondary_test_data";
    private static final int LIMIT = Configuration.getInstance().getPageSize();

    private DocumentRepository repository;
    private ConfigurationRepository configRepository;
    private DocumentsDatabaseIndexerImpl indexer;

    @Before
    public void setUp() throws Exception {
        LOGGER.setLevel(Level.INFO);

        LOGGER.addAppender(new ConsoleAppender(new PatternLayout(
                PatternLayout.TTCC_CONVERSION_PATTERN)));
        repository = EasyMock.createMock(DocumentRepository.class);
        configRepository = EasyMock.createMock(ConfigurationRepository.class);
        indexer = new DocumentsDatabaseIndexerImpl();
        indexer.setDocumentRepository(repository);
        indexer.setConfigRepository(configRepository);
        CacheFlusher.getInstance().flushCaches();
    }

    @After
    public void tearDown() throws Exception {
        CacheFlusher.getInstance().flushCaches();
    }

    @Test
    public final void testIndexDatabase() {
        EasyMock.expect(configRepository.getIndexPolicy(DB_NAME)).andReturn(
                new IndexPolicy());
        EasyMock.expect(
                repository.getAllDocuments(DB_NAME, null, null, LIMIT + 1))
                .andReturn(PagedList.<Document> emptyList());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        indexer.indexUsingPrimaryDatabase(DB_NAME, null, null);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
    }

    @Test
    public final void testIndexSecondaryDatabase() {
        EasyMock.expect(
                configRepository.getIndexPolicy(DB_NAME + "_" + SECONDARY_TEST_DB))
                .andReturn(new IndexPolicy());
        EasyMock.expect(
                repository.getAllDocuments(TEST_DB_SECONDARY_TEST_DB, null, null,
                        LIMIT + 1)).andReturn(PagedList.<Document> emptyList());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        indexer.indexUsingSecondaryDatabase(DB_NAME, null, SECONDARY_TEST_DB,
                TEST_DB_SECONDARY_TEST_DB, TEST_DATUM_ID, SECONDARY_TEST_DATUM_ID);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
    }
}

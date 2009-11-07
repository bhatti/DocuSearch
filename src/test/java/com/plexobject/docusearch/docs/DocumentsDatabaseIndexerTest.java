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
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PagedList;
import com.plexobject.docusearch.persistence.RepositoryFactory;

public class DocumentsDatabaseIndexerTest {
    private static Logger LOGGER = Logger.getRootLogger();
    private static final String DB_NAME = "MYDB";
    private static final String SECONDARY_ID = "secondary_id";
    private static final String MASTER_ID = "master_id";
    private static final String MASTER_SECONDARies = "master_secondaries";
    private static final String SECONDARies = "secondaries";
    private static final int LIMIT = Configuration.getInstance().getPageSize();

    private DocumentRepository repository;
    private ConfigurationRepository configRepository;
    private DocumentsDatabaseIndexer indexer;

    @Before
    public void setUp() throws Exception {
        LOGGER.setLevel(Level.INFO);

        LOGGER.addAppender(new ConsoleAppender(new PatternLayout(
                PatternLayout.TTCC_CONVERSION_PATTERN)));
        repository = EasyMock.createMock(DocumentRepository.class);
        configRepository = EasyMock.createMock(ConfigurationRepository.class);
        indexer = new DocumentsDatabaseIndexer(new RepositoryFactory(
                repository, configRepository));
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testIndexAllDatabases() {
        EasyMock.expect(configRepository.getIndexPolicy(DB_NAME)).andReturn(
                new IndexPolicy());
        EasyMock.expect(repository.getAllDatabases()).andReturn(
                new String[] { DB_NAME });
        EasyMock.expect(
                repository.getAllDocuments(DB_NAME, null, null, LIMIT + 1))
                .andReturn(PagedList.<Document> emptyList());
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        indexer.indexAllDatabases();
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);

    }

    @Test
    public final void testIndexDatabases() {
        EasyMock.expect(configRepository.getIndexPolicy(DB_NAME)).andReturn(
                new IndexPolicy());
        EasyMock.expect(
                repository.getAllDocuments(DB_NAME, null, null, LIMIT + 1))
                .andReturn(PagedList.<Document> emptyList());
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        indexer.indexDatabases(new String[] { DB_NAME });
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);

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

        indexer.indexUsingPrimaryDatabase(DB_NAME);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
    }

    @Test
    public final void testIndexSecondaryDatabase() {
        EasyMock.expect(
                configRepository.getIndexPolicy(DB_NAME + "_" + SECONDARies))
                .andReturn(new IndexPolicy());
        EasyMock.expect(
                repository.getAllDocuments(MASTER_SECONDARies, null, null,
                        LIMIT + 1)).andReturn(PagedList.<Document> emptyList());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        indexer.indexUsingSecondaryDatabase(DB_NAME, SECONDARies,
                MASTER_SECONDARies, MASTER_ID, SECONDARY_ID);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
    }
}

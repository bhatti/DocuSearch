package com.plexobject.docusearch.service.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.core.Response;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.docs.DocumentsDatabaseIndexer;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PagedList;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.service.IndexService;

public class IndexServiceImplTest {
    private static final String SECONDARY_ID = "secondary_id";
    private static final String MASTER_ID = "master_id";
    private static final String MASTER_SECONDARYS = "master_secondaries";
    private static final String SECONDARYS = "secondaries";
    private static final String MASTER = "test_master";
    private static final String DB_NAME = "MYDB";
    private static final int LIMIT = Configuration.getInstance().getPageSize();

    private static Logger LOGGER = Logger.getRootLogger();

    private DocumentRepository repository;
    private ConfigurationRepository configRepository;
    private DocumentsDatabaseIndexer indexer;
    private int idCount;
    private IndexService service;

    @Before
    public void setUp() throws Exception {
        LOGGER.setLevel(Level.INFO);

        LOGGER.addAppender(new ConsoleAppender(new PatternLayout(
                PatternLayout.TTCC_CONVERSION_PATTERN)));
        repository = EasyMock.createMock(DocumentRepository.class);
        configRepository = EasyMock.createMock(ConfigurationRepository.class);
        indexer = new DocumentsDatabaseIndexer(new RepositoryFactory(
                repository, configRepository));
        service = new IndexServiceImpl(indexer);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testCreate() {
        EasyMock.expect(
                repository.getAllDocuments(MASTER, null, null, LIMIT + 1))
                .andReturn(newDocuments());

        EasyMock.expect(configRepository.getIndexPolicy(MASTER)).andReturn(
                newIndexPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.createIndexUsingPrimaryDatabase(MASTER);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        Assert.assertEquals(RestClient.OK_CREATED, response.getStatus());
        Assert.assertTrue("unexpected response "
                + response.getEntity().toString(), response.getEntity()
                .toString().contains("rebuilt index for " + MASTER));
    }

    @Test
    public final void testCreateSecondaryWithNullIndexer() {
        Response response = service.createIndexUsingSecondaryDatabase(null,
                SECONDARYS, MASTER_SECONDARYS, MASTER_ID,
                SECONDARY_ID);
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
        response = service.createIndexUsingSecondaryDatabase(MASTER, null,
                MASTER_SECONDARYS, MASTER_ID, SECONDARY_ID);
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
        response = service.createIndexUsingSecondaryDatabase(MASTER,
                SECONDARYS, null, MASTER_ID, SECONDARY_ID);
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
    }

    @Test
    public final void testCreateSecondary() {
        EasyMock.expect(
                repository.getAllDocuments(MASTER_SECONDARYS, null, null,
                        LIMIT + 1)).andReturn(newDocuments());

        EasyMock.expect(repository.getDocument(SECONDARYS, "2")).andReturn(
                newDocument());

        EasyMock
                .expect(
                        configRepository.getIndexPolicy(MASTER + "_"
                                + SECONDARYS)).andReturn(newIndexPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.createIndexUsingSecondaryDatabase(
                MASTER, SECONDARYS, MASTER_SECONDARYS, MASTER_ID,
                SECONDARY_ID);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        Assert.assertEquals(RestClient.OK_CREATED, response.getStatus());
        Assert.assertTrue("unexpected response "
                + response.getEntity().toString(), response.getEntity()
                .toString().contains("rebuilt index for " + MASTER));
    }

    @Test
    public final void testUpdate() {
        EasyMock.expect(repository.getDocument(MASTER, "id")).andReturn(
                newDocument());
        EasyMock.expect(configRepository.getIndexPolicy(MASTER)).andReturn(
                newIndexPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.updateIndexUsingPrimaryDatabase(MASTER,
                "id");
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertTrue("unexpected response "
                + response.getEntity().toString(), response.getEntity()
                .toString().contains(
                        "updated 1 documents in index for " + MASTER
                                + " with ids id"));
    }

    @Test
    public final void testUpdateSecondary() {
        EasyMock.expect(
                repository.getAllDocuments(MASTER_SECONDARYS, null, null,
                        LIMIT + 1)).andReturn(newDocuments());
        EasyMock.expect(repository.getDocument(SECONDARYS, "2")).andReturn(
                newDocument());
        EasyMock.expect(
                configRepository.getIndexPolicy(MASTER + SECONDARYS))
                .andReturn(newIndexPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.updateIndexUsingSecondaryDatabase(
                MASTER, SECONDARYS, MASTER_SECONDARYS, MASTER_ID,
                SECONDARY_ID, "2");
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertTrue("unexpected response "
                + response.getEntity().toString(), response.getEntity()
                .toString().contains(
                        "updated 1 documents in index for " + MASTER
                                + " with ids 2"));
    }

    @Test
    public final void testUpdateSecondaryNotFound() {
        EasyMock.expect(
                repository.getAllDocuments(MASTER_SECONDARYS, null, null,
                        LIMIT + 1)).andReturn(newDocuments());

        EasyMock.expect(
                configRepository.getIndexPolicy(MASTER + SECONDARYS))
                .andReturn(newIndexPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.updateIndexUsingSecondaryDatabase(
                MASTER, SECONDARYS, MASTER_SECONDARYS, MASTER_ID,
                SECONDARY_ID, "0");
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertTrue("unexpected response "
                + response.getEntity().toString(), response.getEntity()
                .toString().contains(
                        "updated 0 documents in index for " + MASTER));
    }

    private PagedList<Document> newDocuments() {
        return PagedList.asList(newDocument());

    }

    @SuppressWarnings("unchecked")
    private Document newDocument() {
        final Map<String, String> map = new TreeMap<String, String>();
        map.put("Y", "8");
        map.put("Z", "9");
        final List<? extends Object> arr = Arrays.asList(11, "12", 13, "14");
        final Document doc = new DocumentBuilder(DB_NAME).setId(
                String.valueOf(++idCount)).put("master_id", "1").put(
                "secondary_id", "2").put("C", map).put("D", arr).build();

        return doc;
    }

    private static IndexPolicy newIndexPolicy() {
        final IndexPolicy policy = new IndexPolicy();
        policy.setScore(10);
        policy.setBoost(20.5F);
        for (int i = 0; i < 10; i++) {
            policy.add("name" + i, i % 2 == 0, i % 2 == 1, i % 2 == 1, 1.1F);
        }
        return policy;
    }
}

package com.plexobject.docusearch.persistence.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;
import java.util.Map;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.converter.Constants;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.plexobject.docusearch.query.LookupPolicy;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.QueryPolicy.FieldType;

public class ConfigurationRepositoryImplTest {
    private static final String DB_NAME = "ConfigurationRepositoryImplTestDB";

    private DocumentRepository repository;

    @Before
    public void setUp() throws Exception {
        repository = EasyMock.createMock(DocumentRepository.class);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testFailedCreateDatabase() {
        EasyMock.expect(repository.createDatabase(DB_NAME)).andThrow(
                new PersistenceException(""));
        EasyMock.replay(repository);

        ConfigurationRepositoryImpl configRepository = new ConfigurationRepositoryImpl(
                DB_NAME);
        configRepository.setDocumentRepository(repository);
        EasyMock.verify(repository);
    }

    @Test
    public final void testGetIndexPolicy() {
        EasyMock.expect(
                repository.createDatabase(Configuration.getInstance()
                        .getConfigDatabase())).andReturn(true);

        EasyMock.expect(
                repository.getDocument(Configuration.getInstance()
                        .getConfigDatabase(), "index_policy_for_" + DB_NAME))
                .andReturn(newIndexPolicyDocument());

        EasyMock.replay(repository);
        ConfigurationRepositoryImpl configRepository = new ConfigurationRepositoryImpl(
                Configuration.getInstance().getConfigDatabase());
        configRepository.setDocumentRepository(repository);
        IndexPolicy policy = configRepository.getIndexPolicy(DB_NAME);

        Assert.assertEquals(newIndexPolicy(), policy);
        EasyMock.verify(repository);

    }

    @Test
    public final void testGetQueryPolicy() {
        EasyMock.expect(
                repository.createDatabase(Configuration.getInstance()
                        .getConfigDatabase())).andReturn(true);

        EasyMock.expect(
                repository.getDocument(Configuration.getInstance()
                        .getConfigDatabase(), "query_policy_for_" + DB_NAME))
                .andReturn(newQueryPolicyDocument());

        EasyMock.replay(repository);
        ConfigurationRepositoryImpl configRepository = new ConfigurationRepositoryImpl(
                Configuration.getInstance().getConfigDatabase());
        configRepository.setDocumentRepository(repository);
        QueryPolicy policy = configRepository.getQueryPolicy(DB_NAME);

        Assert.assertEquals(newQueryPolicy(), policy);
        EasyMock.verify(repository);
    }

    @Test
    public final void testGetLookupPolicy() {
        EasyMock.expect(
                repository.createDatabase(Configuration.getInstance()
                        .getConfigDatabase())).andReturn(true);

        EasyMock.expect(
                repository.getDocument(Configuration.getInstance()
                        .getConfigDatabase(), "lookup_policy_for_" + DB_NAME))
                .andReturn(newLookupPolicyDocument());

        EasyMock.replay(repository);
        ConfigurationRepositoryImpl configRepository = new ConfigurationRepositoryImpl(
                Configuration.getInstance().getConfigDatabase());
        configRepository.setDocumentRepository(repository);
        LookupPolicy policy = configRepository.getLookupPolicy(DB_NAME);

        Assert.assertEquals(newLookupPolicy(), policy);
        EasyMock.verify(repository);
    }

    @Test
    public final void testSaveIndexPolicy() {
        EasyMock.expect(
                repository.createDatabase(Configuration.getInstance()
                        .getConfigDatabase())).andReturn(true);
        final Document doc = newIndexPolicyDocument();
        EasyMock.expect(
                repository.getDocument(Configuration.getInstance()
                        .getConfigDatabase(), "index_policy_for_" + DB_NAME))
                .andReturn(newIndexPolicyDocument());
        EasyMock.expect(repository.saveDocument(doc, true)).andReturn(doc);

        EasyMock.replay(repository);
        ConfigurationRepositoryImpl configRepository = new ConfigurationRepositoryImpl(
                Configuration.getInstance().getConfigDatabase());
        configRepository.setDocumentRepository(repository);
        IndexPolicy policy = configRepository.saveIndexPolicy(DB_NAME,
                newIndexPolicy());

        Assert.assertEquals(newIndexPolicy(), policy);
        EasyMock.verify(repository);
    }

    @Test
    public final void testSaveQueryPolicy() {
        EasyMock.expect(
                repository.createDatabase(Configuration.getInstance()
                        .getConfigDatabase())).andReturn(true);
        final Document doc = newQueryPolicyDocument();
        EasyMock.expect(
                repository.getDocument(Configuration.getInstance()
                        .getConfigDatabase(), "query_policy_for_" + DB_NAME))
                .andReturn(newQueryPolicyDocument());
        EasyMock.expect(repository.saveDocument(doc, true)).andReturn(doc);

        EasyMock.replay(repository);
        ConfigurationRepositoryImpl configRepository = new ConfigurationRepositoryImpl(
                Configuration.getInstance().getConfigDatabase());
        configRepository.setDocumentRepository(repository);
        QueryPolicy policy = configRepository.saveQueryPolicy(DB_NAME,
                newQueryPolicy());

        Assert.assertEquals(newQueryPolicy(), policy);
        EasyMock.verify(repository);
    }

    @Test
    public final void testSaveLookupPolicy() {
        EasyMock.expect(
                repository.createDatabase(Configuration.getInstance()
                        .getConfigDatabase())).andReturn(true);
        final Document doc = newLookupPolicyDocument();
        EasyMock.expect(
                repository.getDocument(Configuration.getInstance()
                        .getConfigDatabase(), "lookup_policy_for_" + DB_NAME))
                .andReturn(newQueryPolicyDocument());
        EasyMock.expect(repository.saveDocument(doc, true)).andReturn(doc);

        EasyMock.replay(repository);
        ConfigurationRepositoryImpl configRepository = new ConfigurationRepositoryImpl(
                Configuration.getInstance().getConfigDatabase());
        configRepository.setDocumentRepository(repository);
        LookupPolicy policy = configRepository.saveLookupPolicy(DB_NAME,
                newLookupPolicy());

        Assert.assertEquals(newLookupPolicy(), policy);
        EasyMock.verify(repository);
    }

    private static IndexPolicy newIndexPolicy() {
        final IndexPolicy policy = new IndexPolicy();
        policy.setScore(10);
        policy.setBoost(20.5F);
        for (int i = 0; i < 10; i++) {
            policy.add("name" + i, i % 2 == 0, null, i % 2 == 1, i % 2 == 1, 1.1F,
                    false, false, false);
        }
        return policy;
    }

    private static Document newIndexPolicyDocument() {
        return new DocumentBuilder(Configuration.getInstance()
                .getConfigDatabase()).setId("index_policy_for_" + DB_NAME)
                .putAll(newIndexPolicyMap()).build();
    }

    private static Map<String, Object> newIndexPolicyMap() {
        final Map<String, Object> map = new TreeMap<String, Object>();
        map.put(Constants.SCORE, 10);
        map.put(Constants.BOOST, 20.5);
        final Collection<Map<String, Object>> fields = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 10; i++) {
            final Map<String, Object> field = new TreeMap<String, Object>();
            field.put(Constants.NAME, "name" + i);
            field.put(Constants.STORE_IN_INDEX, i % 2 == 0);
            field.put(Constants.ANALYZE, i % 2 == 1);
            field.put(Constants.BOOST, 1.1);
            fields.add(field);
        }
        map.put(Constants.FIELDS, fields);
        return map;
    }

    private static QueryPolicy newQueryPolicy() {
        final QueryPolicy policy = new QueryPolicy();
        for (int i = 0; i < 10; i++) {
            policy.add("name" + i);
        }
        return policy;
    }

    private static LookupPolicy newLookupPolicy() {
        final LookupPolicy policy = new LookupPolicy();
        policy.setFieldToReturn("fieldToReturn");
        for (int i = 0; i < 10; i++) {
            policy.add("name" + i, i, i % 2 == 1, 1.1F, FieldType.STRING);
        }
        return policy;
    }

    private static Document newQueryPolicyDocument() {
        return new DocumentBuilder(Configuration.getInstance()
                .getConfigDatabase()).setId("query_policy_for_" + DB_NAME)
                .putAll(newQueryPolicyMap()).build();
    }

    private static Document newLookupPolicyDocument() {
        return new DocumentBuilder(Configuration.getInstance()
                .getConfigDatabase()).setId("lookup_policy_for_" + DB_NAME)
                .putAll(newLookupPolicyMap()).build();
    }

    private static Map<String, Object> newQueryPolicyMap() {
        final Map<String, Object> map = new TreeMap<String, Object>();

        final Collection<Map<String, Object>> fields = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 10; i++) {
            final Map<String, Object> field = new TreeMap<String, Object>();
            field.put(Constants.NAME, "name" + i);
            field.put(Constants.SORT_ORDER, i);
            field.put(Constants.ASCENDING_ORDER, i % 2 == 1);
            field.put(Constants.BOOST, "1.1");
            fields.add(field);
        }
        map.put(Constants.FIELDS, fields);
        return map;
    }

    private static Map<String, Object> newLookupPolicyMap() {
        final Map<String, Object> map = newQueryPolicyMap();
        map.put(Constants.FIELD_TO_RETURN, "fieldToReturn");
        return map;
    }
}

package com.plexobject.docusearch.service.impl;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.cache.CacheFlusher;
import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.NotFoundException;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.plexobject.docusearch.query.LookupPolicy;
import com.plexobject.docusearch.query.QueryPolicy;

public class ConfigurationServiceImplTest {
    private static final String DB_NAME = "test_db_delete_me";
    private DocumentRepository repository;
    private ConfigurationRepository configRepository;
    private ConfigurationServiceImpl service;
    private static boolean INTEGRATION_TEST = false;

    @Before
    public void setUp() throws Exception {
        CacheFlusher.getInstance().flushCaches();
        repository = EasyMock.createMock(DocumentRepository.class);
        configRepository = EasyMock.createMock(ConfigurationRepository.class);
        service = new ConfigurationServiceImpl();

        if (!INTEGRATION_TEST) {
            service.configRepository = configRepository;
        }

    }

    @After
    public void tearDown() throws Exception {
        CacheFlusher.getInstance().flushCaches();
    }

    @Test
    public void testDefaultConstructor() {
        new ConfigurationServiceImpl();
    }

    @Test
    public final void testGetWithBadId() throws JSONException {
        Response response = service.getIndexPolicy("");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testGetWithPersistenceException() throws JSONException {
        EasyMock.expect(configRepository.getIndexPolicy(DB_NAME)).andThrow(
                new PersistenceException(""));

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.getIndexPolicy(DB_NAME);
        verifyMock();

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testSaveWithBadDatabase() throws JSONException {
        Response response = service.saveIndexPolicy("", "{}");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testSavetWithBadBody() throws JSONException {
        Response response = service.saveIndexPolicy(DB_NAME, "");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testSaveWithPersistenceException() throws JSONException {
        final IndexPolicy policy = newIndexPolicy();

        EasyMock.expect(configRepository.saveIndexPolicy(DB_NAME, policy))
                .andThrow(new PersistenceException("test error"));

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        final String jsonOriginal = Converters.getInstance().getConverter(
                IndexPolicy.class, JSONObject.class).convert(policy).toString();
        Response response = service.saveIndexPolicy(DB_NAME, jsonOriginal
                .toString());
        verifyMock();

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testGetBadIndexPolicy() throws JSONException {
        EasyMock.expect(configRepository.getIndexPolicy(DB_NAME)).andThrow(
                new NotFoundException("test error"));

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.getIndexPolicy(DB_NAME);
        verifyMock();

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testGetBadIndexPolicyAgain() throws JSONException {
        EasyMock.expect(configRepository.getIndexPolicy(DB_NAME)).andThrow(
                new RuntimeException("test error"));

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.getIndexPolicy(DB_NAME);
        verifyMock();

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testSaveAndGetIndexPolicy() throws JSONException {
        final IndexPolicy policy = newIndexPolicy();
        EasyMock.expect(configRepository.saveIndexPolicy(DB_NAME, policy))
                .andReturn(policy);

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        final String jsonOriginal = Converters.getInstance().getConverter(
                IndexPolicy.class, JSONObject.class).convert(policy).toString();
        Response response = service.saveIndexPolicy(DB_NAME, jsonOriginal
                .toString());
        verifyMock();

        Assert.assertEquals(201, response.getStatus());
        JSONObject jsonDoc = new JSONObject(response.getEntity().toString());
        Assert.assertEquals(jsonOriginal.toString(), jsonDoc.toString());

        // verify read
        EasyMock.reset(repository);
        EasyMock.reset(configRepository);
        EasyMock.expect(configRepository.getIndexPolicy(DB_NAME)).andReturn(
                newIndexPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        response = service.getIndexPolicy(DB_NAME);
        verifyMock();

        Assert.assertEquals(200, response.getStatus());
        jsonDoc = new JSONObject(response.getEntity().toString());
        Assert.assertEquals(jsonOriginal.toString(), jsonDoc.toString());
    }

    @Test
    public final void testGetNullQueryPolicy() throws JSONException {
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.getQueryPolicy(null);
        verifyMock();

        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
    }

    @Test
    public final void testGetBadQueryPolicy() throws JSONException {
        EasyMock.expect(configRepository.getQueryPolicy(DB_NAME)).andThrow(
                new NotFoundException("test error"));
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.getQueryPolicy(DB_NAME);
        verifyMock();

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testGetBadQueryPolicyAgain() throws JSONException {
        EasyMock.expect(configRepository.getQueryPolicy(DB_NAME)).andThrow(
                new RuntimeException("test error"));
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.getQueryPolicy(DB_NAME);
        verifyMock();

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testSaveQueryPolicyWithBadDB() throws JSONException {
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        Response response = service.saveQueryPolicy(null, null);
        verifyMock();

        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
    }

    @Test
    public final void testSaveQueryPolicyWithBadPolicy() throws JSONException {
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        Response response = service.saveQueryPolicy(DB_NAME, null);
        verifyMock();

        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
    }

    @Test
    public final void testSaveAndGetQueryPolicy() throws JSONException {
        final QueryPolicy policy = newQueryPolicy();

        EasyMock.expect(configRepository.saveQueryPolicy(DB_NAME, policy))
                .andReturn(policy);

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        final String jsonOriginal = Converters.getInstance().getConverter(
                QueryPolicy.class, JSONObject.class).convert(policy).toString();
        Response response = service.saveQueryPolicy(DB_NAME, jsonOriginal
                .toString());
        verifyMock();

        Assert.assertEquals(201, response.getStatus());
        JSONObject jsonDoc = new JSONObject(response.getEntity().toString());
        Assert.assertEquals(jsonOriginal.toString(), jsonDoc.toString());

        // verify read
        EasyMock.reset(repository);
        EasyMock.reset(configRepository);
        EasyMock.expect(configRepository.getQueryPolicy(DB_NAME)).andReturn(
                newQueryPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        response = service.getQueryPolicy(DB_NAME);
        verifyMock();

        Assert.assertEquals(200, response.getStatus());
        jsonDoc = new JSONObject(response.getEntity().toString());
        Assert.assertEquals(jsonOriginal.toString(), jsonDoc.toString());
    }

    @Test
    public final void testGetNullLookupPolicy() throws JSONException {
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.getLookupPolicy(null);
        verifyMock();

        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
    }

    @Test
    public final void testGetBadLookupPolicy() throws JSONException {
        EasyMock.expect(configRepository.getLookupPolicy(DB_NAME)).andThrow(
                new NotFoundException("test error"));
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.getLookupPolicy(DB_NAME);
        verifyMock();

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testGetBadLookupPolicyAgain() throws JSONException {
        EasyMock.expect(configRepository.getLookupPolicy(DB_NAME)).andThrow(
                new RuntimeException("test error"));
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.getLookupPolicy(DB_NAME);
        verifyMock();

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testSaveLookupPolicyWithBadDB() throws JSONException {
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        Response response = service.saveLookupPolicy(null, null);
        verifyMock();

        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
    }

    @Test
    public final void testSaveLookupPolicyWithBadPolicy() throws JSONException {
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        Response response = service.saveLookupPolicy(DB_NAME, null);
        verifyMock();

        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
    }

    @Test
    public final void testSaveAndGetLookupPolicy() throws JSONException {
        final LookupPolicy policy = newLookupPolicy();

        EasyMock.expect(configRepository.saveLookupPolicy(DB_NAME, policy))
                .andReturn(policy);

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        final String jsonOriginal = Converters.getInstance().getConverter(
                LookupPolicy.class, JSONObject.class).convert(policy)
                .toString();
        Response response = service.saveLookupPolicy(DB_NAME, jsonOriginal
                .toString());
        verifyMock();

        Assert.assertEquals(201, response.getStatus());
        JSONObject jsonDoc = new JSONObject(response.getEntity().toString());
        Assert.assertEquals(
                "expected " + jsonOriginal + ", but was " + jsonDoc,
                jsonOriginal.toString(), jsonDoc.toString());

        // verify read
        EasyMock.reset(repository);
        EasyMock.reset(configRepository);
        EasyMock.expect(configRepository.getLookupPolicy(DB_NAME)).andReturn(
                newLookupPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        response = service.getLookupPolicy(DB_NAME);
        verifyMock();

        Assert.assertEquals(200, response.getStatus());
        jsonDoc = new JSONObject(response.getEntity().toString());
        Assert.assertEquals(jsonOriginal.toString(), jsonDoc.toString());
    }


    private static IndexPolicy newIndexPolicy() {
        final IndexPolicy policy = new IndexPolicy();
        policy.setScore(10);
        policy.setBoost(20.5F);
        policy.setAddToDictionary(true);
        policy.setOwner("shahbhat");
        for (int i = 0; i < 10; i++) {
            policy.add("name" + i, i % 2 == 0, null, i % 2 == 1, i % 2 == 1,
                    1.1F, true, false, false);
        }
        return policy;
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
        for (int i = 0; i < 3; i++) {
            policy.add("name" + i);
        }
        policy.setFieldToReturn("return");
        return policy;
    }

    private void verifyMock() {
        if (!INTEGRATION_TEST) {
            EasyMock.verify(repository);
            EasyMock.verify(configRepository);
        }
    }

}

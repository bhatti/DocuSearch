package com.plexobject.docusearch.service.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.service.RepositoryService;

public class RepositoryServiceImplTest {
    private static final String DB_NAME = "test_db_delete_me";
    private static final long START_ID = System.currentTimeMillis();
    private DocumentRepository repository;
    private ConfigurationRepository configRepository;
    private RepositoryService service;
    private static boolean INTEGRATION_TEST = false;

    @Before
    public void setUp() throws Exception {
        repository = EasyMock.createMock(DocumentRepository.class);
        configRepository = EasyMock.createMock(ConfigurationRepository.class);
        if (INTEGRATION_TEST) {
            service = new RepositoryServiceImpl();
        } else {
            service = new RepositoryServiceImpl(new RepositoryFactory(
                    repository, configRepository));
        }

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testDefaultConstructor() {
        new RepositoryServiceImpl();
    }

    @Test
    public final void testDeleteWithNullDatabase() throws JSONException {
        Response response = service.delete(null, "id", "ver");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testDeleteWithBadDatabase() throws JSONException {
        Response response = service.delete("bad\"", "id", "ver");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testDeleteWithBadId() throws JSONException {
        Response response = service.delete(DB_NAME, "", "ver");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testDeleteWithBadVersion() throws JSONException {
        Response response = service.delete(DB_NAME, "id", "");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testDeleteWithPersistenceException() throws JSONException {
        EasyMock.expect(repository.deleteDocument(DB_NAME, "id", "1.0"))
                .andThrow(new PersistenceException("error"));

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.delete(DB_NAME, "id", "1.0");
        verifyMock();

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testGetWithNullDatabase() throws JSONException {
        Response response = service.get(null, "id");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testGetWithBadDatabase() throws JSONException {
        Response response = service.get("bad\"", "id");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testGetWithBadId() throws JSONException {
        Response response = service.get(DB_NAME, "");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testGetWithPersistenceException() throws JSONException {
        EasyMock.expect(repository.getDocument(DB_NAME, "id")).andThrow(
                new PersistenceException("error"));

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.get(DB_NAME, "id");
        verifyMock();

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testPostWithNullDatabase() throws JSONException {
        Response response = service.post(null, "body");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testPostWithBadDatabase() throws JSONException {
        Response response = service.post("bad\"", "body");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testPostWithBadBody() throws JSONException {
        Response response = service.post(DB_NAME, "");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testPostWithPersistenceException() throws JSONException {
        final Document original = newDocument(START_ID + 10, null);
        final String jsonOriginal = Converters.getInstance().getConverter(
                Object.class, JSONObject.class).convert(original).toString();

        EasyMock.expect(repository.saveDocument(original)).andThrow(
                new PersistenceException("error"));

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.post(DB_NAME, jsonOriginal);
        verifyMock();

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testPutWithNullDatabase() throws JSONException {
        Response response = service.put(null, "id", "ver", "body");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testPutWithBadDatabase() throws JSONException {
        Response response = service.put("bad\"", "id", "ver", "body");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testPuttWithBadId() throws JSONException {
        Response response = service.put(DB_NAME, "", "ver", "");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testPuttWithBadVer() throws JSONException {
        Response response = service.put(DB_NAME, "id", "", "body");
        Assert.assertEquals(RestClient.SERVER_INTERNAL_ERROR, response
                .getStatus());

    }

    @Test
    public final void testPuttWithBadBody() throws JSONException {
        Response response = service.put(DB_NAME, "id", "ver", "");
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testPutWithPersistenceException() throws JSONException {
        final Document original = newDocument(START_ID + 100, "2.0");
        final String jsonOriginal = Converters.getInstance().getConverter(
                Object.class, JSONObject.class).convert(original).toString();

        EasyMock.expect(repository.saveDocument(original)).andThrow(
                new PersistenceException("error"));

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.put(DB_NAME,
                String.valueOf(START_ID + 100), "2.0", jsonOriginal);
        verifyMock();

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testPostAndGet() throws JSONException {
        final Document original = newDocument(START_ID + 1, null);
        final Document saved = newDocument(START_ID + 1, "1.0");

        EasyMock.expect(repository.saveDocument(original)).andReturn(saved);

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        final String jsonOriginal = Converters.getInstance().getConverter(
                Object.class, JSONObject.class).convert(original).toString();
        Response response = service.post(DB_NAME, jsonOriginal);
        verifyMock();

        Assert.assertEquals(201, response.getStatus());
        JSONObject jsonDoc = new JSONObject(response.getEntity().toString());
        assertDocument(jsonDoc);

        // verify read
        EasyMock.reset(repository);
        EasyMock.reset(configRepository);
        EasyMock.expect(
                repository.getDocument(DB_NAME, String.valueOf(START_ID + 1)))
                .andReturn(original);

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        response = service.get(DB_NAME, String.valueOf(START_ID + 1));
        verifyMock();

        Assert.assertEquals(200, response.getStatus());
        jsonDoc = new JSONObject(response.getEntity().toString());
        assertDocument(jsonDoc);

    }

    @Test
    public final void testPutAndDelete() throws JSONException {
        final Document original = newDocument(START_ID + 2, null);
        final Document saved = newDocument(START_ID + 2, "1.0");

        EasyMock.expect(repository.saveDocument(original)).andReturn(saved);

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        final String jsonOriginal = Converters.getInstance().getConverter(
                Object.class, JSONObject.class).convert(original).toString();
        Response response = service.put(DB_NAME, String.valueOf(START_ID + 2),
                null, jsonOriginal);
        verifyMock();

        Assert.assertEquals(201, response.getStatus());
        JSONObject jsonDoc = new JSONObject(response.getEntity().toString());
        assertDocument(jsonDoc);

        // verify read
        EasyMock.reset(repository);
        EasyMock.reset(configRepository);
        EasyMock.expect(
                repository.deleteDocument(DB_NAME,
                        String.valueOf(START_ID + 2), "1.0")).andReturn(true);

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        response = service.delete(DB_NAME, jsonDoc.getString(Document.ID),
                jsonDoc.getString(Document.REVISION));
        verifyMock();

        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public final void testUpdate() throws JSONException {
        final Document original = newDocument(START_ID + 3, null);
        Document saved = newDocument(START_ID + 3, "1.0");

        EasyMock.expect(repository.saveDocument(original)).andReturn(saved);

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        final String jsonOriginal = Converters.getInstance().getConverter(
                Object.class, JSONObject.class).convert(original).toString();
        Response response = service.put(DB_NAME, String.valueOf(START_ID + 3),
                null, jsonOriginal);
        verifyMock();

        Assert.assertEquals(201, response.getStatus());
        JSONObject jsonDoc = new JSONObject(response.getEntity().toString());
        assertDocument(jsonDoc);

        // verify read
        EasyMock.reset(repository);
        EasyMock.reset(configRepository);
        final Document originalVer = new DocumentBuilder(original).put("_rev",
                "1.1").build();
        EasyMock.expect(
                repository.getDocument(DB_NAME, String.valueOf(START_ID + 3)))
                .andReturn(originalVer);

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        response = service.get(DB_NAME, String.valueOf(START_ID + 3));
        verifyMock();

        Assert.assertEquals(200, response.getStatus());
        jsonDoc = new JSONObject(response.getEntity().toString());
        saved = Converters.getInstance().getConverter(JSONObject.class,
                Document.class).convert(jsonDoc);
        assertDocument(jsonDoc);

        // update
        EasyMock.reset(repository);
        EasyMock.reset(configRepository);
        final Document modified = new DocumentBuilder(saved).put("newkey",
                "newvalue").build();

        EasyMock.expect(repository.saveDocument(modified)).andReturn(modified);

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        final String jsonModifiedl = Converters.getInstance().getConverter(
                Object.class, JSONObject.class).convert(modified).toString();
        response = service.put(DB_NAME, String.valueOf(START_ID + 3), modified
                .getRevision(), jsonModifiedl);
        verifyMock();

        Assert.assertEquals(200, response.getStatus());
        jsonDoc = new JSONObject(response.getEntity().toString());
        assertDocument(jsonDoc);
    }

    private void assertDocument(JSONObject jsonDoc) throws JSONException {
        Assert.assertEquals("1", jsonDoc.getString("A"));
        Assert.assertEquals("2", jsonDoc.getString("B"));
        Assert
                .assertEquals("{\"Y\":\"8\",\"Z\":\"9\"}", jsonDoc
                        .getString("C"));
        Assert.assertEquals("[11,\"12\",13,\"14\"]", jsonDoc.getString("D"));
    }

    @SuppressWarnings("unchecked")
    private Document newDocument(final long id, final String rev) {
        final Map<String, String> map = new TreeMap<String, String>();
        map.put("Y", "8");
        map.put("Z", "9");
        final List<? extends Object> arr = Arrays.asList(11, "12", 13, "14");

        final DocumentBuilder docBuilder = new DocumentBuilder(DB_NAME).setId(
                String.valueOf(id)).put("A", "1").put("B", "2").put("C", map)
                .put("D", arr);
        if (rev != null) {
            docBuilder.put("_rev", rev);
        }
        return docBuilder.build();
    }

    private void verifyMock() {
        if (!INTEGRATION_TEST) {
            EasyMock.verify(repository);
            EasyMock.verify(configRepository);
        }
    }
}

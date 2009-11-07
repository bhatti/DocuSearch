package com.plexobject.docusearch.persistence.couchdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;
import org.codehaus.jettison.json.JSONObject;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.domain.Tuple;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.plexobject.docusearch.persistence.couchdb.DocumentRepositoryCouchdb;

/**
 * @author Shahzad Bhatti
 * 
 */
public class DocumentRepositoryCouchdbTest {
    private static final String NON_EXISTING_COUCH_DB_URL = "http://127.0.0.1:3984";
    private static final int LIMIT = Configuration.getInstance().getPageSize();

    private static final int NUM_DOCS = 10;
    private static final String DB_NAME = "x" + System.currentTimeMillis();
    private DocumentRepository repository;
    private static boolean INTEGRATION_TEST = false;
    private RestClient httpClient;

    @Before
    public void setUp() throws Exception {
        httpClient = EasyMock.createMock(RestClient.class);

        if (INTEGRATION_TEST) {
            repository = new DocumentRepositoryCouchdb();
        } else {
            repository = new DocumentRepositoryCouchdb(httpClient);
        }
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testQuery() throws Exception {
        final Map<String, Document> documents = createDocuments();
        final String req = "{\"map\":\"function(doc) {if (doc.B == '2' && doc.deleted != true) {emit(null, doc);}}\"}";

        EasyMock.expect(httpClient.post(DB_NAME + "/_temp_view", req))
                .andReturn(
                        new Tuple(RestClient.OK, prepareJsonReply(documents
                                .values())));
        EasyMock.replay(httpClient);

        Map<String, String> criteria = new HashMap<String, String>();
        criteria.put("B", "2");

        Map<String, Document> saved = repository.query(DB_NAME, criteria);

        verifyMock();
        Assert.assertEquals(NUM_DOCS, saved.size());
        for (Document doc : saved.values()) {
            Assert.assertEquals(documents.get(doc.getId()), doc);
        }

    }

    @Test
    public void testQueryNotFound() throws Exception {
        createDocuments();
        final String req = "{\"map\":\"function(doc) {if (doc.Z == '9' && doc.deleted != true) {emit(null, doc);}}\"}";

        EasyMock.expect(httpClient.post(DB_NAME + "/_temp_view", req))
                .andReturn(
                        new Tuple(RestClient.OK,
                                prepareJsonReply(new ArrayList<Document>())));
        EasyMock.replay(httpClient);

        Map<String, String> criteria = new HashMap<String, String>();
        criteria.put("Z", "9");

        Map<String, Document> saved = repository.query(DB_NAME, criteria);

        verifyMock();
        Assert.assertEquals(0, saved.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullServer() throws Exception {
        repository = new DocumentRepositoryCouchdb(null, null, null);
    }

    @Test(expected = PersistenceException.class)
    public void testUnAuthorizeCreate() throws Exception {
        EasyMock.expect(httpClient.put(DB_NAME, null)).andReturn(
                new Tuple(401, ""));
        EasyMock.replay(httpClient);
        repository = new DocumentRepositoryCouchdb(NON_EXISTING_COUCH_DB_URL,
                "bad", "bad");
        repository.createDatabase(DB_NAME);
        verifyMock();
    }

    @Test(expected = PersistenceException.class)
    public void testGetAllDatabasesWithBadServer() throws Exception {
        repository = new DocumentRepositoryCouchdb(NON_EXISTING_COUCH_DB_URL,
                null, null);
        repository.getAllDatabases();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateDatabasesWithNullDatabase() throws Exception {
        repository.createDatabase(null);
    }

    @Test(expected = PersistenceException.class)
    public void testCreateDatabasesWithBadDatabase() throws Exception {
        EasyMock.expect(httpClient.put("__", null)).andThrow(new IOException());
        EasyMock.replay(httpClient);
        repository.createDatabase("__");
        verifyMock();
    }

    @Test
    public void testCreateGetAllDatabases() throws Exception {
        EasyMock.expect(httpClient.put(DB_NAME, null)).andReturn(
                new Tuple(RestClient.OK_CREATED, ""));

        EasyMock.replay(httpClient);

        Assert.assertTrue(repository.createDatabase(DB_NAME));
        verifyMock();

        EasyMock.reset(httpClient);
        EasyMock.expect(httpClient.get("_all_dbs")).andReturn(
                new Tuple(RestClient.OK, "[\"" + DB_NAME + "\"]"));
        EasyMock.replay(httpClient);

        Assert.assertTrue(Arrays.asList(repository.getAllDatabases()).contains(
                DB_NAME));
        verifyMock();
    }

    @Test(expected = PersistenceException.class)
    public void testDuplicateDatabases() throws Exception {
        EasyMock.expect(httpClient.put(DB_NAME, null)).andThrow(
                new IOException());
        EasyMock.replay(httpClient);

        repository.createDatabase(DB_NAME);
        verifyMock();
    }

    @Test(expected = NullPointerException.class)
    public void testSaveNullDocument() throws Exception {
        repository.saveDocument(null);
    }

    @Test
    public void testSaveGetDocument() throws Exception {
        Document original = newDocument(false);
        final String jsonOriginal = Converters.getInstance().getConverter(
                Object.class, JSONObject.class).convert(original).toString();
        EasyMock.expect(httpClient.post(DB_NAME, jsonOriginal)).andReturn(
                new Tuple(RestClient.OK_CREATED,
                        "{\"ok\":true,\"id\":\"1\",\"rev\":\"1\"}"));
        EasyMock.replay(httpClient);

        original = repository.saveDocument(original);
        verifyMock();

        EasyMock.reset(httpClient);
        EasyMock.expect(httpClient.get(DB_NAME + "/1")).andReturn(
                new Tuple(RestClient.OK, "{\"_id\":\"1\",\"_rev\":\"1\","
                        + jsonOriginal.substring(1)));
        EasyMock.replay(httpClient);

        Document saved = repository.getDocument(DB_NAME, original.getId());
        Assert.assertEquals(original, saved);
        verifyMock();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetDocumentsNullDatabase() throws Exception {
        repository.getDocuments(null, new String[0]);
    }

    @Test(expected = NullPointerException.class)
    public void testGetDocumentsNullIds() throws Exception {
        repository.getDocuments(DB_NAME, (String[]) null);
    }

    @Test
    public void testSaveGetDocuments() throws Exception {
        final Map<String, Document> documents = createDocuments();

        EasyMock.expect(
                httpClient.post(DB_NAME + "/_all_docs?include_docs=true",
                        prepareJsonRequest(documents.keySet()))).andReturn(
                new Tuple(RestClient.OK, prepareJsonReply(documents.values())));
        EasyMock.replay(httpClient);

        Map<String, Document> saved = repository.getDocuments(DB_NAME,
                documents.keySet());
        verifyMock();
        Assert.assertEquals(NUM_DOCS, saved.size());
        for (Document doc : saved.values()) {
            Assert.assertEquals(documents.get(doc.getId()), doc);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAllDocumentsByLimitWithNullDatabase() throws Exception {
        repository.getAllDocuments(null, 0, NUM_DOCS * 10);
    }

    @Test
    public void testSaveGetAllDocumentsByLimit() throws Exception {
        final Map<String, Document> documents = createDocuments();

        EasyMock
                .expect(
                        httpClient
                                .get(DB_NAME
                                        + "/_all_docs_by_seq?startkey=0&limit=100&include_docs=true"))
                .andReturn(
                        new Tuple(RestClient.OK, prepareJsonReply(documents
                                .values())));
        EasyMock.replay(httpClient);

        List<Document> saved = repository.getAllDocuments(DB_NAME, 0,
                NUM_DOCS * 10);
        verifyMock();
        for (Document doc : saved) {
            final Document next = documents.get(doc.getId());
            if (next != null) {
                Assert.assertEquals(next, doc);
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAllDocumentsByEndKeyWithNullDatabase() throws Exception {
        repository.getAllDocuments(null, "", "", LIMIT);
    }

    @Test
    public void testSaveGetAllDocumentsByEndKey() throws Exception {
        final Map<String, Document> documents = createDocuments();

        String first = null;
        String last = null;
        for (String id : documents.keySet()) {
            if (first == null) {
                first = id;
            }
            last = id;
        }

        EasyMock.expect(
                httpClient.get(DB_NAME + "/_all_docs?limit=" + LIMIT
                        + "&startkey=%22" + first + "%22&endkey=%22" + last
                        + "%22&include_docs=true")).andReturn(
                new Tuple(RestClient.OK, prepareJsonReply(documents.values())));
        EasyMock.replay(httpClient);

        List<Document> saved = repository.getAllDocuments(DB_NAME, first, last,
                LIMIT);
        verifyMock();
        Assert.assertEquals(NUM_DOCS, saved.size());
        for (Document doc : saved) {
            final Document next = documents.get(doc.getId());
            Assert.assertEquals(next, doc);
        }
    }

    @Test
    public void testGetInfo() throws Exception {
        EasyMock.expect(httpClient.get(DB_NAME)).andReturn(
                new Tuple(RestClient.OK, "{'db_name': '" + DB_NAME + "'}"));
        EasyMock.replay(httpClient);

        Map<String, String> info = repository.getInfo(DB_NAME);
        verifyMock();
        Assert.assertEquals(DB_NAME, info.get("db_name"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetInfoWithNull() throws Exception {
        repository.getInfo(null);
    }

    @Test
    public void testDeleteDatabases() throws Exception {
        EasyMock.expect(httpClient.delete(DB_NAME)).andReturn(RestClient.OK);

        EasyMock.replay(httpClient);

        Assert.assertTrue("failed to delete", repository
                .deleteDatabase(DB_NAME));
    }

    @Test
    public void testDeleteDatabasesAgain() throws Exception {
        EasyMock.expect(httpClient.delete(DB_NAME)).andReturn(419);

        EasyMock.replay(httpClient);

        Assert.assertFalse(repository.deleteDatabase(DB_NAME));
        verifyMock();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteDatabasesWithNull() throws Exception {
        repository.deleteDatabase(null);
    }

    @SuppressWarnings("unchecked")
    private Document newDocument(final boolean specifyId) {
        final String id = specifyId ? "x" + System.nanoTime() : null;
        final List<? extends Object> arr = Arrays.asList(11, "12", 13, "14");

        final Map<String, String> map = new TreeMap<String, String>();
        map.put("Y", "8");
        map.put("Z", "9");

        final Document doc = new DocumentBuilder(DB_NAME).setId(id).put("A",
                "1").put("B", "2").put("C", map).put("D", arr).build();

        return doc;
    }

    private Map<String, Document> createDocuments() throws IOException {
        Map<String, Document> documents = new TreeMap<String, Document>();
        for (int i = 0; i < NUM_DOCS; i++) {
            final Document original = newDocument(true);
            final String jsonOriginal = Converters.getInstance().getConverter(
                    Object.class, JSONObject.class).convert(original)
                    .toString();
            documents.put(original.getId(), original);

            EasyMock.expect(
                    httpClient.put(DB_NAME + "/" + original.getId(),
                            jsonOriginal)).andReturn(
                    new Tuple(RestClient.OK_CREATED,
                            "{\"ok\":true,\"id\":\"1\",\"rev\":\"1\"}"));
            EasyMock.replay(httpClient);

            repository.saveDocument(original);
            EasyMock.reset(httpClient);

        }
        return documents;
    }

    private String prepareJsonReply(final Collection<Document> documents) {
        final StringBuilder jsonReply = new StringBuilder(
                "{\"total_rows\":\"21\",\"offset\":\"0\",\"rows\":[");

        boolean first = true;
        for (Document doc : documents) {
            if (!first) {
                jsonReply.append(",");
            }

            jsonReply.append("{\"doc\":");
            final String jsonDoc = Converters.getInstance().getConverter(
                    Object.class, JSONObject.class).convert(doc).toString();
            jsonReply.append(jsonDoc).append("}");

            first = false;
        }

        jsonReply.append("]}");
        return jsonReply.toString();

    }

    private String prepareJsonRequest(Collection<String> ids) {

        final StringBuilder jsonReq = new StringBuilder("{\"keys\":[");
        boolean first = true;
        for (String id : ids) {
            if (!first) {
                jsonReq.append(",");
            }

            jsonReq.append("\"" + id + "\"");
            first = false;

        }
        jsonReq.append("]}");
        return jsonReq.toString();
    }

    private void verifyMock() {
        if (!INTEGRATION_TEST) {
            EasyMock.verify(httpClient);
        }
    }
}

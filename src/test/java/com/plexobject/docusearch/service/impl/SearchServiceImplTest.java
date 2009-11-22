package com.plexobject.docusearch.service.impl;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.RankedTerm;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.SearchDocList;
import com.plexobject.docusearch.service.SearchService;

public class SearchServiceImplTest {
    private static final String TEST_DB = "test_test_data";
    private static final String DB_NAME = "MYDB";
    private static final int MAX_LIMIT = Configuration.getInstance()
            .getPageSize();

    static class StubSearchServiceImpl extends SearchServiceImpl {
        final Query stubQuery;

        StubSearchServiceImpl(final DocumentRepository docRepository,
                final ConfigurationRepository configRepository,
                final Query stubQuery) {
            super(new RepositoryFactory(docRepository, configRepository));
            this.stubQuery = stubQuery;
        }

        @Override
        protected Query newQueryImpl(final File dir) {
            return stubQuery;
        }
    }

    private DocumentRepository repository;
    private ConfigurationRepository configRepository;
    private Query query;
    private SearchService service;

    @Before
    public void setUp() throws Exception {
        repository = EasyMock.createMock(DocumentRepository.class);
        configRepository = EasyMock.createMock(ConfigurationRepository.class);
        query = EasyMock.createMock(Query.class);
        service = new StubSearchServiceImpl(repository, configRepository, query);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testDefaultConstructor() {
        new SearchServiceImpl();
    }

    @Test
    public void testQueryWithNullIndex() {
        service.query(null, "owner", "keywords", true, 0, MAX_LIMIT, true);
    }

    @Test
    public void testQueryWithBadIndex() {
        service.query("name\"", "owner", "keywords", true, 0, MAX_LIMIT, true);
    }

    @Test
    public void testQueryWithNoKeywordsIndex() {
        service.query(TEST_DB, "owner", "", true, 0, MAX_LIMIT, true);
    }

    @Test
    public final void testQuery() throws JSONException {
        final QueryPolicy policy = newQueryPolicy();
        final QueryCriteria criteria = new QueryCriteria().setKeywords(
                "keywords").setOwner("shahbhat");
        EasyMock.expect(configRepository.getQueryPolicy(TEST_DB)).andReturn(
                policy);
        EasyMock.expect(query.search(criteria, policy, true, 0, MAX_LIMIT))
                .andReturn(newDocuments());
        EasyMock.expect(repository.getDocument(TEST_DB, "id")).andReturn(
                newDocument());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        EasyMock.replay(query);

        Response response = service.query(TEST_DB, "shahbhat", "keywords",
                true, 0, MAX_LIMIT, true);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        EasyMock.verify(query);

        Assert.assertEquals(200, response.getStatus());

        JSONObject jsonResponse = new JSONObject(response.getEntity()
                .toString());
        Assert.assertEquals("keywords", jsonResponse.getString("keywords"));
        Assert.assertEquals("0", jsonResponse.getString("start"));
        Assert.assertEquals(String.valueOf(MAX_LIMIT), jsonResponse
                .getString("limit"));
        Assert.assertEquals("1", jsonResponse.getString("totalHits"));
        JSONArray jsonDocs = jsonResponse.getJSONArray("docs");
        JSONObject jsonDoc = jsonDocs.getJSONObject(0);
        Assert.assertEquals("1", jsonDoc.getString("A"));
        Assert.assertEquals("2", jsonDoc.getString("B"));
        Assert
                .assertEquals("{\"Y\":\"8\",\"Z\":\"9\"}", jsonDoc
                        .getString("C"));
        Assert.assertEquals("[11,\"12\",13,\"14\"]", jsonDoc.getString("D"));
    }

    @Test
    public final void testQueryWithoutDetails() throws JSONException {
        final QueryPolicy policy = newQueryPolicy();
        final QueryCriteria criteria = new QueryCriteria().setKeywords(
                "keywords").setOwner("shahbhat");
        EasyMock.expect(configRepository.getQueryPolicy(TEST_DB)).andReturn(
                policy);
        EasyMock.expect(query.search(criteria, policy, false, 0, MAX_LIMIT))
                .andReturn(newDocuments());
        EasyMock.expect(repository.getDocument(TEST_DB, "id")).andReturn(
                newDocument());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        EasyMock.replay(query);

        Response response = service.query(TEST_DB, "shahbhat", "keywords",
                false, 0, MAX_LIMIT, true);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        EasyMock.verify(query);

        Assert.assertEquals(200, response.getStatus());

        JSONObject jsonResponse = new JSONObject(response.getEntity()
                .toString());
        Assert.assertEquals("keywords", jsonResponse.getString("keywords"));
        Assert.assertEquals("0", jsonResponse.getString("start"));
        Assert.assertEquals(String.valueOf(MAX_LIMIT), jsonResponse
                .getString("limit"));
        Assert.assertEquals("1", jsonResponse.getString("totalHits"));
        JSONArray jsonDocs = jsonResponse.getJSONArray("docs");
        JSONObject jsonDoc = jsonDocs.getJSONObject(0);
        Assert.assertEquals("1", jsonDoc.getString("A"));
        Assert.assertEquals("2", jsonDoc.getString("B"));
        Assert
                .assertEquals("{\"Y\":\"8\",\"Z\":\"9\"}", jsonDoc
                        .getString("C"));
        Assert.assertEquals("[11,\"12\",13,\"14\"]", jsonDoc.getString("D"));
    }

    @Test
    public final void testQueryWithException() throws JSONException {
        EasyMock.expect(configRepository.getQueryPolicy(TEST_DB)).andThrow(
                new IllegalArgumentException());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        EasyMock.replay(query);

        Response response = service.query(TEST_DB, "owner", "keywords",
                false, 0, MAX_LIMIT, true);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        EasyMock.verify(query);

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testMoreLikeThis() throws JSONException {
        final QueryPolicy policy = newQueryPolicy();

        EasyMock.expect(configRepository.getQueryPolicy(TEST_DB)).andReturn(
                policy);
        EasyMock.expect(query.moreLikeThis("1", 1, policy, 0, MAX_LIMIT))
                .andReturn(newDocuments());
        EasyMock.expect(repository.getDocument(TEST_DB, "id")).andReturn(
                newDocument());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        EasyMock.replay(query);

        Response response = service.moreLikeThis(TEST_DB, "1", 1, 0,
                MAX_LIMIT, true);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        EasyMock.verify(query);

        Assert.assertEquals(200, response.getStatus());

        JSONObject jsonResponse = new JSONObject(response.getEntity()
                .toString());
        Assert.assertEquals("1", jsonResponse.getString("luceneId"));
        Assert.assertEquals("1", jsonResponse.getString("externalId"));

        Assert.assertEquals("0", jsonResponse.getString("start"));
        Assert.assertEquals(String.valueOf(MAX_LIMIT), jsonResponse
                .getString("limit"));
        Assert.assertEquals("1", jsonResponse.getString("totalHits"));
        JSONArray jsonDocs = jsonResponse.getJSONArray("docs");

        Assert.assertEquals(1, jsonDocs.length());
    }

    @Test
    public final void testMoreLikeThisWithoutDetails() throws JSONException {
        final QueryPolicy policy = newQueryPolicy();

        EasyMock.expect(configRepository.getQueryPolicy(TEST_DB)).andReturn(
                policy);
        EasyMock.expect(query.moreLikeThis("1", 1, policy, 0, MAX_LIMIT))
                .andReturn(newDocuments());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        EasyMock.replay(query);

        Response response = service.moreLikeThis(TEST_DB, "1", 1, 0,
                MAX_LIMIT, false);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        EasyMock.verify(query);

        Assert.assertEquals(200, response.getStatus());

        JSONObject jsonResponse = new JSONObject(response.getEntity()
                .toString());
        Assert.assertEquals("1", jsonResponse.getString("luceneId"));
        Assert.assertEquals("1", jsonResponse.getString("externalId"));
        Assert.assertEquals("0", jsonResponse.getString("start"));
        Assert.assertEquals(String.valueOf(MAX_LIMIT), jsonResponse
                .getString("limit"));
        Assert.assertEquals("1", jsonResponse.getString("totalHits"));
        JSONArray jsonDocs = jsonResponse.getJSONArray("docs");

        Assert.assertEquals(1, jsonDocs.length());
    }

    @Test
    public final void testMoreLikeThisWithException() throws JSONException {
        final QueryPolicy policy = newQueryPolicy();

        EasyMock.expect(configRepository.getQueryPolicy(TEST_DB)).andReturn(
                policy);
        EasyMock.expect(query.moreLikeThis("1", 1, policy, 0, MAX_LIMIT))
                .andThrow(new IllegalArgumentException());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        EasyMock.replay(query);

        Response response = service.moreLikeThis(TEST_DB, "1", 1, 0,
                MAX_LIMIT, true);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        EasyMock.verify(query);

        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public final void testExplain() throws JSONException {
        final QueryPolicy policy = newQueryPolicy();
        final QueryCriteria criteria = new QueryCriteria()
                .setKeywords("keywords");
        EasyMock.expect(configRepository.getQueryPolicy(TEST_DB)).andReturn(
                policy);
        EasyMock.expect(query.explain(criteria, policy, 0, MAX_LIMIT))
                .andReturn(Arrays.asList("explain1", "explain2"));

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        EasyMock.replay(query);

        Response response = service
                .explain(TEST_DB, "keywords", 0, MAX_LIMIT);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        EasyMock.verify(query);

        Assert.assertEquals(200, response.getStatus());

        JSONArray arr = new JSONArray(response.getEntity().toString());
        Assert.assertEquals(2, arr.length());
    }

    @Test
    public final void testExplainWithNullIndex() throws JSONException {
        Response response = service.explain(null, "keywords", 0, MAX_LIMIT);
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
    }

    @Test
    public final void testExplainWithBadIndex() throws JSONException {
        Response response = service
                .explain("index\"", "keywords", 0, MAX_LIMIT);
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testExplainWithBadKeywords() throws JSONException {
        Response response = service.explain(TEST_DB, "", 0, MAX_LIMIT);
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testTopTerms() throws JSONException {
        final QueryPolicy policy = newQueryPolicy();

        EasyMock.expect(configRepository.getQueryPolicy(TEST_DB)).andReturn(
                policy);
        EasyMock.expect(query.getTopRankingTerms(policy, MAX_LIMIT)).andReturn(
                Arrays.asList(new RankedTerm("name1", "value1", 1),
                        new RankedTerm("name2", "value2", 2)));

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        EasyMock.replay(query);

        Response response = service.getTopRankingTerms(TEST_DB, MAX_LIMIT);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        EasyMock.verify(query);

        Assert.assertEquals(200, response.getStatus());

        JSONArray arr = new JSONArray(response.getEntity().toString());
        Assert.assertEquals(2, arr.length());
    }

    private static QueryPolicy newQueryPolicy() {
        final QueryPolicy policy = new QueryPolicy();

        for (int i = 0; i < 10; i++) {
            policy.add("name" + i);
        }
        return policy;
    }

    @SuppressWarnings("unchecked")
    private Document newDocument() {
        final Map<String, String> map = new TreeMap<String, String>();
        map.put("Y", "8");
        map.put("Z", "9");
        final List<? extends Object> arr = Arrays.asList(11, "12", 13, "14");

        final Document doc = new DocumentBuilder(DB_NAME).setId("id").put("A",
                "1").put("B", "2").put("C", map).put("D", arr).build();
        return doc;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> newMapDocument() {
        final Map<String, Object> doc = new TreeMap<String, Object>();
        doc.put(Document.DATABASE, DB_NAME);
        doc.put(Document.ID, "id");

        doc.put("A", "1");
        doc.put("B", "2");
        final Map<String, String> map = new TreeMap<String, String>();
        map.put("Y", "8");
        map.put("Z", "9");
        doc.put("C", map);
        final List<? extends Object> arr = Arrays.asList(11, "12", 13, "14");
        doc.put("D", arr);

        return doc;
    }

    private SearchDocList newDocuments() {
        final SearchDoc result = new SearchDoc(newMapDocument());

        return new SearchDocList(0, 10, 1, Arrays.asList(result), Collections
                .<String> emptyList());

    }
}

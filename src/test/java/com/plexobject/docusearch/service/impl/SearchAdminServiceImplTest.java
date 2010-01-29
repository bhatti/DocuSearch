package com.plexobject.docusearch.service.impl;

import java.io.File;
import java.util.Arrays;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.cache.CacheFlusher;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.query.CriteriaBuilder;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.RankedTerm;
import com.plexobject.docusearch.service.SearchAdminService;

public class SearchAdminServiceImplTest {
    private static final String TEST_DB = "test_test_data";
    private static final int MAX_LIMIT = Configuration.getInstance()
            .getPageSize();

    static class StubSearchAdminServiceImpl extends SearchAdminServiceImpl {
        final Query stubQuery;

        StubSearchAdminServiceImpl(final DocumentRepository docRepository,
                final ConfigurationRepository configRepository,
                final Query stubQuery) {
            this.documentRepository = docRepository;
            this.configRepository = configRepository;
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
    private SearchAdminService service;

    @Before
    public void setUp() throws Exception {
        CacheFlusher.getInstance().flushCaches();
        repository = EasyMock.createMock(DocumentRepository.class);
        configRepository = EasyMock.createMock(ConfigurationRepository.class);
        query = EasyMock.createMock(Query.class);
        service = new StubSearchAdminServiceImpl(repository, configRepository,
                query);
    }

    @After
    public void tearDown() throws Exception {
        EasyMock.reset(repository);
        EasyMock.reset(configRepository);
        EasyMock.reset(query);
        CacheFlusher.getInstance().flushCaches();
    }

    @Test
    public void testDefaultConstructor() {
        new SearchServiceImpl();
    }

    @Test
    public final void testExplain() throws JSONException {
        final QueryPolicy queryPolicy = newQueryPolicy();
        final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                "keywords").build();
        EasyMock.expect(configRepository.getIndexPolicy(TEST_DB)).andReturn(
                null);
        EasyMock.expect(configRepository.getQueryPolicy(TEST_DB)).andReturn(
                queryPolicy);
        EasyMock.expect(
                query.explainSearch(criteria, null, queryPolicy, 0, MAX_LIMIT))
                .andReturn(Arrays.asList("explain1", "explain2"));

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        EasyMock.replay(query);

        Response response = service.explainSearch(TEST_DB, null, "keywords",
                null, null, null, null, null, 0, null, true, 0, MAX_LIMIT);

        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        EasyMock.verify(query);

        Assert.assertEquals(200, response.getStatus());

        JSONArray arr = new JSONArray(response.getEntity().toString());
        Assert.assertEquals(2, arr.length());
    }

    @Test
    public final void testExplainWithNullIndex() throws JSONException {
        Response response = service.explainSearch(null, null, "keywords", null,
                null, null, null, null, 0, null, false, MAX_LIMIT, 0);
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
    }

    @Test
    public final void testExplainWithBadIndex() throws JSONException {
        Response response = service.explainSearch("index\"", null, "keywords",
                null, null, null, null, null, 0, null, false, MAX_LIMIT, 0);
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());

    }

    @Test
    public final void testExplainWithBadKeywords() throws JSONException {
        Response response = service.explainSearch(TEST_DB, null, "", null,
                null, null, null, null, 0, null, false, MAX_LIMIT, 0);
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

}

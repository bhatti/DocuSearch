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
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.query.CriteriaBuilder;
import com.plexobject.docusearch.query.LookupPolicy;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.service.SuggestionService;

public class SuggestionServiceImplTest {
    private static final String TEST_DB = "test_test_data";
    private static final int MAX_LIMIT = Configuration.getInstance()
            .getPageSize();

    static class StubSuggestionServiceImpl extends SuggestionServiceImpl {
        final Query stubQuery;

        StubSuggestionServiceImpl(final DocumentRepository docRepository,
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
    private SuggestionService service;

    @Before
    public void setUp() throws Exception {
        CacheFlusher.getInstance().flushCaches();
        repository = EasyMock.createMock(DocumentRepository.class);
        configRepository = EasyMock.createMock(ConfigurationRepository.class);
        query = EasyMock.createMock(Query.class);
        service = new StubSuggestionServiceImpl(repository, configRepository,
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
    public void testQueryWithNullIndex() {
        service.autocomplete(null, "keywords", null, MAX_LIMIT);
    }

    @Test
    public void testQueryWithBadIndex() {
        service.autocomplete("name\"", "keywords", null, MAX_LIMIT);
    }

    @Test
    public void testQueryWithNoKeywordsIndex() {
        service.autocomplete("name", "", null, MAX_LIMIT);
    }

    @Test
    public final void testQuery() throws JSONException {
        final LookupPolicy lookupPolicy = newLookupPolicy();
        final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                "keywords").build(); // *
        EasyMock.expect(configRepository.getLookupPolicy(TEST_DB)).andReturn(
                lookupPolicy);
        EasyMock.expect(configRepository.getIndexPolicy(TEST_DB)).andReturn(
                null);
        System.out.println("calling partial lookup with " + criteria
                + " and policy " + lookupPolicy);
        EasyMock.expect(
                query.partialLookup(criteria, null, lookupPolicy, MAX_LIMIT))
                .andReturn(Arrays.asList("one", "two", "three"));

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        EasyMock.replay(query);

        Response response = service.autocomplete(TEST_DB, "keywords", null,
                MAX_LIMIT);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        EasyMock.verify(query);

        Assert.assertEquals(200, response.getStatus());

        JSONArray jsonResponse = new JSONArray(response.getEntity().toString());
        Assert.assertEquals(3, jsonResponse.length());
        Assert.assertEquals("one", jsonResponse.get(0));
        Assert.assertEquals("two", jsonResponse.get(1));
        Assert.assertEquals("three", jsonResponse.get(2));
    }

    private static LookupPolicy newLookupPolicy() {
        final LookupPolicy policy = new LookupPolicy();

        for (int i = 0; i < 10; i++) {
            policy.add("name" + i);
        }
        return policy;
    }

}

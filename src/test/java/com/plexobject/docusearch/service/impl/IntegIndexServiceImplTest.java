package com.plexobject.docusearch.service.impl;

import java.io.File;

import javax.ws.rs.core.Response;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;

import com.plexobject.docusearch.cache.CacheFlusher;
import com.plexobject.docusearch.service.ConfigurationService;
import com.plexobject.docusearch.service.IndexService;
import com.plexobject.docusearch.service.SearchService;

public class IntegIndexServiceImplTest {
    private static final Logger LOGGER = Logger
            .getLogger(IntegIndexServiceImplTest.class);
    private static final String POLICY_NAME = null;

    private static final String INDEX_NAME = "integ_test_index";
    private static final String DB_NAME = "integ_test_db";
    private static final boolean INTEGRATION_TEST = false;
    private static final String[] TAGS = new String[] {
            "Green Mtn Coffee Roasters In", "Peets Coffee & Tea Inc",
            "Farmers Cap Bk Corp", "Farmer Bros Co", "General Mills",
            "Hershey Co", "Nike Inc", "Cavco Inds Inc Del",
            "Cadbury Schweppes Plc", "Csg Sys Intl Inc", "Quixote Corp",
            "Decorator Industries Inc", "Lindsay Corporation", "Middleby Corp",
            "Flowers Foods Inc", "Panera Bread Co" };

    private IndexService indexService;

    private SearchService searchService;

    private ConfigurationService configService;

    @Before
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(new File("lucene", INDEX_NAME));
        CacheFlusher.getInstance().flushCaches();
        XmlBeanFactory factory = new XmlBeanFactory(new FileSystemResource(
                "src/main/webapp/WEB-INF/applicationContext.xml"));
        factory.getBean("documentRepository");
        indexService = (IndexService) factory.getBean("indexService");
        searchService = (SearchService) factory.getBean("searchService");
        configService = (ConfigurationService) factory.getBean("configService");

    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File("lucene", INDEX_NAME));
    }

    @Test
    public void testIndexQuery() throws Exception {
        if (INTEGRATION_TEST) {
            LOGGER.debug("testing index and query");
            // indexWithoutCreatingPolicy();
            index();
            query();
        }
    }

    private void query() {
        Response response = configService.saveQueryPolicy(INDEX_NAME,
                "{fields:[{name:" + DB_NAME + ".contents}]}");
        Assert.assertEquals(201, response.getStatus());
        for (String tag : TAGS) {
            String index = INDEX_NAME;
            String owner = null;
            String keywords = tag;
            String zipCode = null;
            String city = null;
            String state = null;
            String country = null;
            String region = null;
            float radius = 0;
            String sortBy = null;
            boolean sortAscending = false;
            boolean includeSuggestions = false;
            int start = 0;
            int limit = 20;
            boolean detailedResults = false;

            response = searchService.query(index, owner, keywords, zipCode,
                    city, state, country, region, radius, sortBy,
                    sortAscending, includeSuggestions, start, limit,
                    detailedResults);
            Assert.assertEquals(tag, 200, response.getStatus());

            Assert.assertEquals(tag, "", response.getEntity());
        }
    }

    @SuppressWarnings("unused")
    private void indexWithoutCreatingPolicy() throws Exception {
        StringBuilder docs = new StringBuilder("[");
        for (int i = 0; i < TAGS.length; i++) {
            if (i > 0) {
                docs.append(",");
            }
            docs.append("{_id:" + i + ",contents:'" + TAGS[i] + "'}");
        }
        docs.append("]");
        Response response = indexService.updateIndexUsingPrimaryDatabase(
                INDEX_NAME, DB_NAME, POLICY_NAME, docs.toString());
        Assert.assertEquals(500, response.getStatus());

    }

    private void index() throws Exception {
        Response response = configService.saveIndexPolicy(INDEX_NAME,
                "{fields:[{name:contents}]}");
        Assert.assertEquals(201, response.getStatus());

        StringBuilder docs = new StringBuilder("[");
        for (int i = 0; i < TAGS.length; i++) {
            if (i > 0) {
                docs.append(",");
            }
            docs.append("{_id:" + i + ",contents:'" + TAGS[i] + "'}");
        }
        docs.append("]");
        response = indexService.updateIndexUsingPrimaryDatabase(INDEX_NAME,
                DB_NAME, POLICY_NAME, docs.toString());
        Assert.assertEquals(200, response.getStatus());

        Assert.assertEquals("updated " + TAGS.length + "/" + TAGS.length
                + " documents in index for " + INDEX_NAME, response.getEntity()
                .toString().trim());
    }
}

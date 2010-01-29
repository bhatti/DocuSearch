package com.plexobject.docusearch.service.impl;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import com.plexobject.docusearch.cache.CacheDisposer;
import com.plexobject.docusearch.cache.CacheLoader;
import com.plexobject.docusearch.cache.CachedMap;
import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.jmx.JMXRegistrar;
import com.plexobject.docusearch.jmx.impl.ServiceJMXBeanImpl;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.SearchDocList;
import com.plexobject.docusearch.query.lucene.QueryImpl;
import com.plexobject.docusearch.util.SpatialLookup;
import com.sun.jersey.spi.inject.Inject;

public class BaseSearchServiceImpl implements InitializingBean {
    final Logger LOGGER = Logger.getLogger(getClass());
    private static final long INDEFINITE = 0;

    final Map<File, Query> cachedQueries = new CachedMap<File, Query>(
            INDEFINITE, 24, new CacheLoader<File, Query>() {
                @Override
                public Query get(File dir) {
                    return newQueryImpl(dir);
                }
            }, new CacheDisposer<Query>() {
                @Override
                public void dispose(Query q) {
                    try {
                        q.close();
                    } catch (Exception e) {
                        LOGGER.error("Failed to close " + q, e);
                    }
                }
            });

    @Autowired
    @Inject
    ConfigurationRepository configRepository;

    @Autowired
    @Inject
    DocumentRepository documentRepository;

    @Autowired
    @Inject
    SpatialLookup spatialLookup;

    @Context
    UriInfo uriInfo;

    final ServiceJMXBeanImpl mbean;

    public BaseSearchServiceImpl() {
        mbean = JMXRegistrar.getInstance().register(getClass());
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (configRepository == null) {
            throw new IllegalStateException("configRepository not set");
        }
        if (documentRepository == null) {
            throw new IllegalStateException("documentRepository not set");
        }
        if (spatialLookup == null) {
            throw new IllegalStateException("spatialLookup not set");
        }
    }

    /**
     * @return the configRepository
     */
    public ConfigurationRepository getConfigRepository() {
        return configRepository;
    }

    /**
     * @param configRepository
     *            the configRepository to set
     */
    public void setConfigRepository(ConfigurationRepository configRepository) {
        this.configRepository = configRepository;
    }

    /**
     * @return the documentRepository
     */
    public DocumentRepository getDocumentRepository() {
        return documentRepository;
    }

    /**
     * @param documentRepository
     *            the documentRepository to set
     */
    public void setDocumentRepository(DocumentRepository documentRepository) {
        this.documentRepository = documentRepository;
    }

    /**
     * @return the spatialLookup
     */
    public SpatialLookup getSpatialLookup() {
        return spatialLookup;
    }

    /**
     * @param spatialLookup
     *            the spatialLookup to set
     */
    public void setSpatialLookup(SpatialLookup spatialLookup) {
        this.spatialLookup = spatialLookup;
    }

    @SuppressWarnings("unchecked")
    protected JSONArray docsToJson(final String index,
            final boolean detailedResults, SearchDocList results)
            throws JSONException {
        JSONArray docs = new JSONArray();
        for (SearchDoc result : results) {
            JSONObject resultJson = Converters.getInstance().getConverter(
                    Object.class, JSONObject.class).convert(result);
            if (detailedResults) {
                Document doc = documentRepository.getDocument(index, result
                        .getId());
                JSONObject jsonDoc = Converters.getInstance().getConverter(
                        Object.class, JSONObject.class).convert(doc);
                Iterator<String> it = jsonDoc.keys();
                while (it.hasNext()) {
                    final String key = it.next();
                    final Object value = jsonDoc.get(key);
                    resultJson.put(key, value);
                }
            }
            docs.put(resultJson);
        }
        return docs;
    }

    protected final synchronized Query getQueryImpl(final File dir) {
        Query query = cachedQueries.get(dir);
        if (query == null) {
            query = newQueryImpl(dir);
            cachedQueries.put(dir, query);
        }
        return query;
    }

    protected Query newQueryImpl(final File dir) {
        return new QueryImpl(dir);
    }
}

package com.plexobject.docusearch.persistence.impl;

import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.cache.CacheLoader;
import com.plexobject.docusearch.cache.CachedMap;
import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.plexobject.docusearch.query.LookupPolicy;
import com.plexobject.docusearch.query.QueryPolicy;
import com.sun.jersey.spi.inject.Inject;

@Component("configRepository")
public class ConfigurationRepositoryImpl implements ConfigurationRepository,
        InitializingBean {
    private static final String LOOKUP_POLICY = "lookup_policy_for_";
    private static final String QUERY_POLICY = "query_policy_for_";
    private static final String INDEX_POLICY = "index_policy_for_";
    private static final long INDEFINITE = 0;

    private final String database;
    private Map<String, QueryPolicy> cachedQueryPolicies = new CachedMap<String, QueryPolicy>(
            INDEFINITE, 24, new CacheLoader<String, QueryPolicy>() {
                @Override
                public QueryPolicy get(String id) {
                    return fetchQueryPolicy(id);

                }
            }, null);
    private Map<String, IndexPolicy> cachedIndexPolicies = new CachedMap<String, IndexPolicy>(
            INDEFINITE, 24, new CacheLoader<String, IndexPolicy>() {
                @Override
                public IndexPolicy get(String id) {
                    return fetchIndexPolicy(id);

                }
            }, null);
    private Map<String, LookupPolicy> cachedLookupPolicies = new CachedMap<String, LookupPolicy>(
            INDEFINITE, 24, new CacheLoader<String, LookupPolicy>() {
                @Override
                public LookupPolicy get(String id) {
                    return fetchLookupPolicy(id);

                }
            }, null);

    @Autowired
    @Inject
    DocumentRepository documentRepository;

    public ConfigurationRepositoryImpl() {
        this(Configuration.getInstance().getConfigDatabase());
    }

    public ConfigurationRepositoryImpl(final String database) {
        this.database = database;
    }

    @Override
    public IndexPolicy getIndexPolicy(String id) throws PersistenceException {
        IndexPolicy policy = null;
        synchronized (cachedIndexPolicies) {
            policy = cachedIndexPolicies.get(id);
            if (policy == null) {
                policy = fetchIndexPolicy(id);
                cachedIndexPolicies.put(id, policy);
            }
        }
        return policy;
    }

    private IndexPolicy fetchIndexPolicy(String id) {
        IndexPolicy policy;
        Document doc = documentRepository.getDocument(database,
                toIndexPolicyId(id));
        policy = Converters.getInstance().getConverter(Map.class,
                IndexPolicy.class).convert(doc);
        return policy;
    }

    @Override
    public QueryPolicy getQueryPolicy(String id) throws PersistenceException {
        QueryPolicy policy = null;
        synchronized (cachedQueryPolicies) {
            policy = cachedQueryPolicies.get(id);
            if (policy == null) {
                policy = fetchQueryPolicy(id);
                cachedQueryPolicies.put(id, policy);
            }
        }
        return policy;
    }

    private QueryPolicy fetchQueryPolicy(String id) {
        QueryPolicy policy;
        Document doc = documentRepository.getDocument(database,
                toQueryPolicyId(id));
        policy = Converters.getInstance().getConverter(Map.class,
                QueryPolicy.class).convert(doc);
        return policy;
    }

    @Override
    public LookupPolicy getLookupPolicy(String id) throws PersistenceException {
        LookupPolicy policy = null;

        synchronized (cachedLookupPolicies) {
            policy = cachedLookupPolicies.get(id);
            if (policy == null) {
                policy = fetchLookupPolicy(id);
                cachedLookupPolicies.put(id, policy);
            }
        }
        return policy;
    }

    private LookupPolicy fetchLookupPolicy(String id) {
        LookupPolicy policy;
        Document doc = documentRepository.getDocument(database,
                toLookupPolicyId(id));
        policy = Converters.getInstance().getConverter(Map.class,
                LookupPolicy.class).convert(doc);
        return policy;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IndexPolicy saveIndexPolicy(String id, IndexPolicy policy)
            throws PersistenceException {
        synchronized (cachedIndexPolicies) {
            cachedIndexPolicies.put(id, policy);
        }
        Map map = Converters.getInstance().getConverter(IndexPolicy.class,
                Map.class).convert(policy);
        String rev = null;
        try {
            rev = documentRepository.getDocument(database, toIndexPolicyId(id))
                    .getRevision();
        } catch (PersistenceException e) {
        }

        Document doc = new DocumentBuilder(database).putAll(map).setId(
                toIndexPolicyId(id)).setRevision(rev).build();
        doc = documentRepository.saveDocument(doc, true);
        return Converters.getInstance().getConverter(Map.class,
                IndexPolicy.class).convert(doc);

    }

    @SuppressWarnings("unchecked")
    @Override
    public QueryPolicy saveQueryPolicy(String id, QueryPolicy policy)
            throws PersistenceException {
        synchronized (cachedQueryPolicies) {
            cachedQueryPolicies.put(id, policy);
        }
        Map map = Converters.getInstance().getConverter(QueryPolicy.class,
                Map.class).convert(policy);
        String rev = null;
        try {
            rev = documentRepository.getDocument(database, toQueryPolicyId(id))
                    .getRevision();

        } catch (PersistenceException e) {
        }

        Document doc = new DocumentBuilder(database).putAll(map).setId(
                toQueryPolicyId(id)).setRevision(rev).build();

        doc = documentRepository.saveDocument(doc, true);
        return Converters.getInstance().getConverter(Map.class,
                QueryPolicy.class).convert(doc);

    }

    @SuppressWarnings("unchecked")
    @Override
    public LookupPolicy saveLookupPolicy(String id, LookupPolicy policy)
            throws PersistenceException {
        synchronized (cachedLookupPolicies) {
            cachedLookupPolicies.put(id, policy);
        }

        Map map = Converters.getInstance().getConverter(LookupPolicy.class,
                Map.class).convert(policy);
        String rev = null;
        try {
            rev = documentRepository
                    .getDocument(database, toLookupPolicyId(id)).getRevision();

        } catch (PersistenceException e) {
        }

        Document doc = new DocumentBuilder(database).putAll(map).setId(
                toLookupPolicyId(id)).setRevision(rev).build();

        doc = documentRepository.saveDocument(doc, true);
        return Converters.getInstance().getConverter(Map.class,
                LookupPolicy.class).convert(doc);
    }

    /**
     * @return the repository
     */
    public DocumentRepository getDocumentRepository() {
        return documentRepository;
    }

    /**
     * @param repository
     *            the repository to set
     */
    public void setDocumentRepository(DocumentRepository repository) {
        this.documentRepository = repository;
        try {
            repository.createDatabase(database);
        } catch (PersistenceException e) {
        }
    }

    private static String toIndexPolicyId(final String id) {
        return id.startsWith(INDEX_POLICY) ? id : INDEX_POLICY + id;
    }

    private static String toQueryPolicyId(final String id) {
        return id.startsWith(QUERY_POLICY) ? id : QUERY_POLICY + id;
    }

    private static String toLookupPolicyId(final String id) {
        return id.startsWith(LOOKUP_POLICY) ? id : LOOKUP_POLICY + id;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (documentRepository == null) {
            throw new RuntimeException("documentRepository is not set");
        }
    }
}

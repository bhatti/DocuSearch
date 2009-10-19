package com.plexobject.docusearch.persistence.impl;

import java.util.Map;

import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.plexobject.docusearch.query.QueryPolicy;

public class ConfigurationRepositoryImpl implements ConfigurationRepository {
	private static final String QUERY_POLICY = "query_policy_for_";
	private static final String INDEX_POLICY = "index_policy_for_";
	private final String database;
	private final DocumentRepository repository;

	public ConfigurationRepositoryImpl(final String database,
			final DocumentRepository repository) {
		this.database = database;
		this.repository = repository;
		try {
			repository.createDatabase(database);
		} catch (PersistenceException e) {
		}
	}

	@Override
	public IndexPolicy getIndexPolicy(String id) throws PersistenceException {
		Document doc = repository.getDocument(database, toIndexPolicyId(id));
		return Converters.getInstance().getConverter(Map.class,
				IndexPolicy.class).convert(doc);

	}

	@Override
	public QueryPolicy getQueryPolicy(String id) throws PersistenceException {
		Document doc = repository.getDocument(database, toQueryPolicyId(id));
		return Converters.getInstance().getConverter(Map.class,
				QueryPolicy.class).convert(doc);
	}

	@SuppressWarnings("unchecked")
	@Override
	public IndexPolicy saveIndexPolicy(String id, IndexPolicy policy)
			throws PersistenceException {
		Map map = Converters.getInstance().getConverter(IndexPolicy.class,
				Map.class).convert(policy);
		String rev = null;
		try {
			rev = repository.getDocument(database, toIndexPolicyId(id))
					.getRevision();
		} catch (PersistenceException e) {
		}

		Document doc = new DocumentBuilder(database).putAll(map).setId(
				toIndexPolicyId(id)).setRevision(rev).build();
		doc = repository.saveDocument(doc);
		return Converters.getInstance().getConverter(Map.class,
				IndexPolicy.class).convert(doc);

	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryPolicy saveQueryPolicy(String id, QueryPolicy policy)
			throws PersistenceException {
		Map map = Converters.getInstance().getConverter(QueryPolicy.class,
				Map.class).convert(policy);
		String rev = null;
		try {
			rev = repository.getDocument(database, toQueryPolicyId(id))
					.getRevision();

		} catch (PersistenceException e) {
		}

		Document doc = new DocumentBuilder(database).putAll(map).setId(
				toQueryPolicyId(id)).setRevision(rev).build();

		doc = repository.saveDocument(doc);
		return Converters.getInstance().getConverter(Map.class,
				QueryPolicy.class).convert(doc);

	}

	private static String toIndexPolicyId(final String id) {
		return INDEX_POLICY + id;
	}

	private static String toQueryPolicyId(final String id) {
		return QUERY_POLICY + id;
	}
}

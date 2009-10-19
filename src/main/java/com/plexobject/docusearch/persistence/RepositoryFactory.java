package com.plexobject.docusearch.persistence;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.persistence.couchdb.DocumentRepositoryCouchdb;
import com.plexobject.docusearch.persistence.impl.ConfigurationRepositoryImpl;

public class RepositoryFactory {
	private static final String COUCHDB = DocumentRepositoryCouchdb.class
			.getName();
	private static final String REPOSITORY_IMPL_CLASS = Configuration
			.getInstance().getProperty("repository.impl.class", COUCHDB);
	private final DocumentRepository documentRepository;
	private final ConfigurationRepository configurationRepository;

	public RepositoryFactory() {
		this(REPOSITORY_IMPL_CLASS);
	}

	public RepositoryFactory(final String repositoryImplClass) {
		this(instantiateDocumentRepository(repositoryImplClass));
	}

	public RepositoryFactory(final DocumentRepository documentRepository) {
		this(documentRepository, new ConfigurationRepositoryImpl(Configuration
				.getInstance().getConfigDatabase(), documentRepository));
	}

	public RepositoryFactory(final DocumentRepository documentRepository,
			final ConfigurationRepository configurationRepository) {
		this.documentRepository = documentRepository;
		this.configurationRepository = configurationRepository;
	}

	public DocumentRepository getDocumentRepository() {
		return documentRepository;
	}

	public ConfigurationRepository getConfigurationRepository() {
		return configurationRepository;
	}

	private static DocumentRepository instantiateDocumentRepository(
			final String repositoryImplClass) {
		try {
			return (DocumentRepository) Class.forName(repositoryImplClass)
					.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(
					"Failed to instantiate document repository "
							+ repositoryImplClass, e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(
					"Failed to instantiate document repository "
							+ repositoryImplClass, e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(
					"Failed to instantiate document repository "
							+ repositoryImplClass, e);
		}
	}

}

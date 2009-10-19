package com.plexobject.docusearch.docs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.docs.DocumentsDatabaseIndexer;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.RepositoryFactory;

public class DocumentsDatabaseIndexerTest {
	private static Logger LOGGER = Logger.getRootLogger();
	private static final String DB_NAME = "MYDB";
	private DocumentRepository repository;
	private ConfigurationRepository configRepository;
	private DocumentsDatabaseIndexer indexer;

	@Before
	public void setUp() throws Exception {
		LOGGER.setLevel(Level.INFO);

		LOGGER.addAppender(new ConsoleAppender(new PatternLayout(
				PatternLayout.TTCC_CONVERSION_PATTERN)));
		repository = EasyMock.createMock(DocumentRepository.class);
		configRepository = EasyMock.createMock(ConfigurationRepository.class);
		indexer = new DocumentsDatabaseIndexer(new RepositoryFactory(
				repository, configRepository));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testIndexAllDatabases() {
		EasyMock.expect(repository.getAllDatabases()).andReturn(
				new String[] { DB_NAME });
		EasyMock.expect(configRepository.getIndexPolicy(DB_NAME)).andReturn(
				new IndexPolicy());
		EasyMock.expect(repository.getAllDocuments(DB_NAME, null, null))
		.andReturn(new ArrayList<Document>());
		

		EasyMock.replay(repository);
		EasyMock.replay(configRepository);

		indexer.indexAllDatabases();
		EasyMock.verify(repository);
		EasyMock.verify(configRepository);

	}

	@Test
	public final void testIndexDatabases() {
		
		EasyMock.expect(configRepository.getIndexPolicy(DB_NAME)).andReturn(
				new IndexPolicy());
		EasyMock.expect(repository.getAllDocuments(DB_NAME, null, null))
		.andReturn(new ArrayList<Document>());
		
		EasyMock.replay(repository);
		EasyMock.replay(configRepository);

		indexer.indexDatabases(new String[] { DB_NAME });
		EasyMock.verify(repository);
		EasyMock.verify(configRepository);

	}

	@Test
	public final void testIndexDatabase() {
		EasyMock.expect(configRepository.getIndexPolicy(DB_NAME)).andReturn(
				new IndexPolicy());
		EasyMock.expect(repository.getAllDocuments(DB_NAME, null, null))
				.andReturn(new ArrayList<Document>());

		EasyMock.replay(repository);
		EasyMock.replay(configRepository);

		indexer.indexDatabase(DB_NAME);
		EasyMock.verify(repository);
		EasyMock.verify(configRepository);
	}

	@Test
	public final void testIndexDocumentsFileIndexPolicyCollectionOfDocument() {
		EasyMock.expect(configRepository.getIndexPolicy(DB_NAME)).andReturn(
				new IndexPolicy());

		EasyMock.replay(repository);
		EasyMock.replay(configRepository);

		final Collection<Document> docs = Arrays.asList(new DocumentBuilder(
				DB_NAME).setId("ID").build());
		indexer.indexDocuments(DB_NAME, docs);
		EasyMock.verify(repository);
		EasyMock.verify(configRepository);
	}

}

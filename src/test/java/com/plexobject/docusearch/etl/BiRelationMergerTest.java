package com.plexobject.docusearch.etl;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;

public class BiRelationMergerTest {
	private static final String DB_NAME = "MYDB";
	private static final String TO_DB_NAME = "TO_MYDB";

	private DocumentRepository repository;
	private ConfigurationRepository configRepository;
	private final Document fromDoc1 = new DocumentBuilder(DB_NAME).setId("1")
			.put("symbol", "ibm").put("ticker_id", "x1").build();
	private final Document fromDoc2 = new DocumentBuilder(DB_NAME).setId("2")
			.put("symbol", "java").put("ticker_id", "x2").build();
	private final Document toDoc1 = new DocumentBuilder(TO_DB_NAME).setId("3")
			.put("symbol", "ibm").put("name", "john").build();
	private final Document toDoc2 = new DocumentBuilder(TO_DB_NAME).setId("4")
			.put("symbol", "java").put("name", "sally").build();
	private final Document mergedDoc1 = new DocumentBuilder(TO_DB_NAME).setId(
			"3").put("symbol", "ibm").put("ticker_id", "x1")
			.put("name", "john").build();
	private final Document mergedDoc2 = new DocumentBuilder(TO_DB_NAME).setId(
			"4").put("symbol", "java").put("ticker_id", "x2").put("name",
			"sally").build();

	@Before
	public void setUp() throws Exception {
		repository = EasyMock.createMock(DocumentRepository.class);
		configRepository = EasyMock.createMock(ConfigurationRepository.class);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testMergeListOfDocument() {
	}

	@Test
	public final void testMergeDocument() {
	}

	@Test(expected = NullPointerException.class)
	public final void testCreateMergerWithNullRepository() {
		new BiRelationMerger(null, (Properties) null);

	}

	@Test(expected = NullPointerException.class)
	public final void testCreateMergerWithNullProperties() {
		new BiRelationMerger(repository, (Properties) null);

	}

	@Test(expected = IllegalArgumentException.class)
	public final void testCreateMergerWithFile() throws IOException {
		// fromDatabase will throw exception
		new BiRelationMerger(repository, File.createTempFile("test", "test"));

	}

	@Test(expected = NullPointerException.class)
	public final void testCreateMergerWithNullFile() throws IOException {
		new BiRelationMerger(repository, (File) null);

	}

	@Test(expected = IllegalArgumentException.class)
	public final void testCreateMergerWithoutProperties() {
		Properties props = new Properties();
		BiRelationMerger merger = new BiRelationMerger(repository, props);
		Assert.assertNotNull(merger);
	}

	@Test(expected = IllegalArgumentException.class)
	public final void testNoFromDatabase() {
		final Properties props = new Properties();
		new BiRelationMerger(repository, props);

	}

	@Test(expected = IllegalArgumentException.class)
	public final void testNoToDatabase() {
		final Properties props = new Properties();
		props.put("from.database", DB_NAME);

		new BiRelationMerger(repository, props);

	}

	@Test(expected = IllegalArgumentException.class)
	public final void testNoFromIdDatabase() {
		final Properties props = new Properties();
		props.put("from.database", DB_NAME);
		props.put("to.database", TO_DB_NAME);

		new BiRelationMerger(repository, props);

	}

	@Test(expected = IllegalArgumentException.class)
	public final void testNoToIdDatabase() {
		final Properties props = new Properties();
		props.put("from.database", DB_NAME);
		props.put("to.database", TO_DB_NAME);
		props.put("from.id", "symbol");
		// props.put("to.id", "symbol");
		// props.put("to.relation.name", "ticker_id");
		// props.put("from.merge.columns", "ticker_id");
		// props.put("relation.type", "atom");

		new BiRelationMerger(repository, props);

	}

	@Test(expected = IllegalArgumentException.class)
	public final void testNoRelationNameDatabase() {
		final Properties props = new Properties();
		props.put("from.database", DB_NAME);
		props.put("to.database", TO_DB_NAME);
		props.put("from.id", "symbol");
		props.put("to.id", "symbol");
		// props.put("to.relation.name", "ticker_id");
		// props.put("from.merge.columns", "ticker_id");

		new BiRelationMerger(repository, props);

	}

	@Test(expected = NullPointerException.class)
	public final void testColumnsDatabase() {
		final Properties props = new Properties();
		props.put("from.database", DB_NAME);
		props.put("to.database", TO_DB_NAME);
		props.put("from.id", "symbol");
		props.put("to.id", "symbol");
		props.put("to.relation.name", "ticker_id");

		new BiRelationMerger(repository, props).run();
	}

	@Test
	public final void testRun() {
		run("atom");
		run("hash");
		run("array");
	}

	@SuppressWarnings("serial")
	private final void run(final String type) {
		EasyMock.reset(repository);
		EasyMock.reset(configRepository);
		EasyMock.expect(repository.getAllDocuments(DB_NAME, null, null))
				.andReturn(Arrays.asList(fromDoc1, fromDoc2));
		EasyMock.expect(repository.getAllDocuments(DB_NAME, "2", null))
				.andReturn(Collections.<Document> emptyList());
		EasyMock.expect(
				repository.query(TO_DB_NAME, new HashMap<String, String>() {
					{
						put("symbol", "ibm");
					}
				})).andReturn(new HashMap<String, Document>() {
			{
				put("3", toDoc1);
			}
		});
		EasyMock.expect(repository.saveDocument(mergedDoc1)).andReturn(
				mergedDoc1);

		EasyMock.expect(
				repository.query(TO_DB_NAME, new HashMap<String, String>() {
					{
						put("symbol", "java");
					}
				})).andReturn(new HashMap<String, Document>() {
			{
				put("4", toDoc2);
			}
		});
		EasyMock.expect(repository.saveDocument(mergedDoc2)).andReturn(
				mergedDoc2);

		final Properties props = new Properties();
		props.put("from.database", DB_NAME);
		props.put("to.database", TO_DB_NAME);
		props.put("from.id", "symbol");
		props.put("to.id", "symbol");
		props.put("to.relation.name", "ticker_id");
		props.put("from.merge.columns", "ticker_id");
		props.put("relation.type", type);
		EasyMock.replay(repository);
		EasyMock.replay(configRepository);
		final BiRelationMerger merger = new BiRelationMerger(repository, props);
		merger.run();
		EasyMock.verify(repository);
		EasyMock.verify(configRepository);
	}

}


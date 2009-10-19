/**
 * 
 */
package com.plexobject.docusearch.index.lucene;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.index.lucene.IndexerImpl;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.lucene.analyzer.DiacriticAnalyzer;
import com.plexobject.docusearch.lucene.analyzer.MetaphoneReplacementAnalyzer;
import com.plexobject.docusearch.lucene.analyzer.PorterAnalyzer;
import com.plexobject.docusearch.lucene.analyzer.SynonymAnalyzer;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.lucene.QueryImpl;

import org.apache.log4j.BasicConfigurator;
import org.apache.solr.analysis.SynonymMap;

public class IndexerImplTest {
	private static final String DB_NAME = "MYDB";
	private Directory ram;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		BasicConfigurator.configure();
		ram = new RAMDirectory();

	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testIndexWithScoreQuery() throws Exception {
		Analyzer saved = LuceneUtils.getDefaultAnalyzer();
		query(new MetaphoneReplacementAnalyzer());
		query(new SynonymAnalyzer(new SynonymMap()));
		query(new PorterAnalyzer());
		query(new DiacriticAnalyzer());
		LuceneUtils.setDefaultAnalyzer(saved);

	}

	private void query(final Analyzer analyzer) throws Exception {
		LuceneUtils.setDefaultAnalyzer(analyzer);
		int succeeded = index();
		final QueryCriteria criteria = new QueryCriteria();
		criteria.setScoreQuery();
		final QueryImpl query = new QueryImpl(ram);

		List<SearchDoc> results = query.search(criteria, 1, 10);
		System.out.println("Analyzer " + analyzer + ", results " + results);
		Assert.assertEquals(2, succeeded);
		Assert.assertEquals(2, results.size());
		Assert.assertEquals(1, results.get(0).getHitDocumentNumber());
		Assert.assertEquals(42, (int) results.get(0).getScore());
		Assert.assertEquals(0, results.get(1).getHitDocumentNumber());
		Assert.assertEquals(7, (int) results.get(1).getScore());
	}

	@Test
	public void testIndexWithKeywordsAndRecency() throws Exception {
		int succeeded = index();
		final QueryCriteria criteria = new QueryCriteria();
		criteria.setKeywords("hat");
		criteria.setRecencyFactor(365, 2);
		final QueryImpl query = new QueryImpl(ram);
		List<SearchDoc> results = query.search(criteria, 1, 10);

		Assert.assertEquals(2, succeeded);
		Assert.assertEquals(2, results.size());
	}

	@Test
	public void testIndexWithMultwordKewords() throws Exception {
		int succeeded = index("1", 7, new String[] { "name",
				"this hat is green", "title", "luke for lucene", "tags",
				"[{name:tag1, rank:1}, {name:tag2, rank:2}]", "date",
				"{'month':'10', 'day':'9', 'year':'2009'}" }, new String[] {
				"name", "title", "tags[name]", "date{year}" });
		Assert.assertEquals(1, succeeded);

		succeeded = index("2", 42, new String[] { "name", "this hat is blue",
				"title", "indxing search", "tags",
				"[{name:tag1, rank:1}, {name:tag2, rank:2}]", "date",
				"{'month':'10', 'day':'9', 'year':'2009'}" }, new String[] {
				"name", "title", "tags[name]", "date{year}" });
		Assert.assertEquals(1, succeeded);

		final QueryCriteria criteria = new QueryCriteria();
		criteria.setKeywords("hat");
		criteria.addFields("name", "title", "tags[name]", "date{year}");
		final QueryImpl query = new QueryImpl(ram);
		List<SearchDoc> results = query.search(criteria, 1, 10);
		Assert.assertEquals(2, results.size());
	}

	private int index() throws Exception {
		ram = new RAMDirectory();

		int succeeded = 0;
		succeeded = index("1", 7,
				new String[] { "content", "this hat is green" }, null);
		succeeded += index("2", 42, new String[] { "content",
				"this hat is blue" }, null);
		return succeeded;
	}

	private int index(String id, final int score, String[] fields,
			String[] indexFields) throws Exception {

		final Map<String, Object> attrs = new HashMap<String, Object>();
		for (int i = 0; i < fields.length - 1; i += 2) {
			attrs.put(fields[i], fields[i + 1]);
		}

		final Document doc = new DocumentBuilder(DB_NAME).putAll(attrs).setId(
				id).setRevision("REV").build();
		final IndexPolicy policy = new IndexPolicy();
		if (indexFields == null) {
			for (int i = 0; i < fields.length - 1; i += 2) {
				policy.add(fields[i]);
			}
		} else {
			for (int i = 0; i < indexFields.length; i++) {
				policy.add(indexFields[i]);
			}
		}
		policy.setScore(score);
		final IndexerImpl indexer = new IndexerImpl(ram);
		return indexer.index(policy, Arrays.asList(doc));
	}
}

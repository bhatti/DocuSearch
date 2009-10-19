package com.plexobject.docusearch.lucene.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Assert;
import org.junit.Test;

public class MetaphoneAnalyzerTest {
	@SuppressWarnings("deprecation")
	@Test
	public void testKoolKat() throws Exception {
		RAMDirectory directory = new RAMDirectory();
		Analyzer analyzer = new MetaphoneReplacementAnalyzer();

		TokenStream.setOnlyUseNewAPI(true);

		IndexWriter writer = new IndexWriter(directory, analyzer, true,
				IndexWriter.MaxFieldLength.UNLIMITED);

		Document doc = new Document();
		doc.add(new Field("contents", "cool cat", Field.Store.YES,
				Field.Index.ANALYZED));
		writer.addDocument(doc);
		writer.close();

		IndexSearcher searcher = new IndexSearcher(directory);
		Query query = new QueryParser("contents", analyzer).parse("kool kat");

		TopScoreDocCollector collector = TopScoreDocCollector.create(1, true);
		searcher.search(query, collector);
		Assert.assertEquals(1, collector.getTotalHits());
		int docID = collector.topDocs().scoreDocs[0].doc;
		doc = searcher.doc(docID);
		Assert.assertEquals("cool cat", doc.get("contents"));

		searcher.close();
	}

}

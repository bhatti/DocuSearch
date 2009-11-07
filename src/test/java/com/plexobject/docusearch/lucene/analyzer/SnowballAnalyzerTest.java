package com.plexobject.docusearch.lucene.analyzer;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.snowball.SnowballAnalyzer;
import org.junit.Test;


public class SnowballAnalyzerTest {

	@Test
	public void testEnglish() throws Exception {
		@SuppressWarnings("unused")
        Analyzer analyzer = new SnowballAnalyzer("English");
		//LuceneTestUtils.assertAnalyzesTo(analyzer, "stemming algorithms", new String[] { "stem", "algorithm" });
	}
}

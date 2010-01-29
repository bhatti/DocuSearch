package com.plexobject.docusearch.lucene.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.snowball.SnowballAnalyzer;
import org.junit.Test;

import com.plexobject.docusearch.lucene.LuceneTestUtils;

public class SnowballAnalyzerTest {

    @SuppressWarnings("deprecation")
    @Test
    public void testEnglish() throws Exception {
        Analyzer analyzer = new SnowballAnalyzer("English");
        LuceneTestUtils.assertAnalyzesTo(analyzer, "stemming algorithms",
                new String[] { "stem", "algorithm" }, true);
    }
}

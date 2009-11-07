package com.plexobject.docusearch.lucene;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.junit.Assert;

public class LuceneTestUtils {

    @SuppressWarnings("deprecation")
    public static void assertAnalyzesTo(Analyzer analyzer, String input,
            String[] output) throws Exception {
        TokenStream stream = analyzer.tokenStream("field", new StringReader(
                input));

        if (true) {
            TermAttribute termAttr = (TermAttribute) stream
                    .addAttribute(TermAttribute.class);
            for (int i = 0; i < output.length; i++) {
                Assert.assertTrue(stream.incrementToken());
                Assert.assertEquals(output[i], termAttr.term());
            }
            Assert.assertFalse(stream.incrementToken());
        } else {
            Token reusableToken = new Token();
            for (int i = 0; i < output.length; i++) {
                Token t = stream.next(reusableToken);
                Assert.assertTrue(t != null);
                Assert.assertEquals(output[i], t.term());
            }
            Assert.assertTrue(stream.next(reusableToken) == null);
        }
        stream.close();
    }

    public static int hitCount(IndexSearcher searcher, Query query,
            Filter filter) throws IOException {
        return searcher.search(query, filter, 1).totalHits;
    }

    public static int hitCount(IndexSearcher searcher, Query query)
            throws IOException {
        return searcher.search(query, 1).totalHits;
    }

}

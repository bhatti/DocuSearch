package com.plexobject.docusearch.lucene.analyzer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.analysis.SynonymFilter;
import org.apache.solr.analysis.SynonymMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.lucene.LuceneTestUtils;
import com.plexobject.docusearch.lucene.LuceneUtils;

public class SynonymAnalyzerTest {
    private static final String INPUT = "The quick brown fox jumps over the lazy dogs";
    private IndexSearcher searcher;
    private SynonymMap synonymMap;
    private SynonymAnalyzer analyzer;
    private static Logger LOGGER = Logger.getRootLogger();

    @Before
    public void setUp() throws Exception {
        LOGGER.setLevel(Level.INFO);

        LOGGER.addAppender(new ConsoleAppender(new PatternLayout(
                PatternLayout.TTCC_CONVERSION_PATTERN)));
    }

    private void setupDefaultSynonymMap() throws Exception {
        analyzer = new SynonymAnalyzer();
        analyzer.synonymMap.add(Arrays.asList("jumps"), SynonymMap
                .makeTokens(Arrays.asList("jumps", "hops", "leaps")), false,
                true);
        analyzer.synonymMap.add(Arrays.asList("hops"), SynonymMap
                .makeTokens(Arrays.asList("jumps", "hops", "leaps")), false,
                true);
        analyzer.synonymMap.add(Arrays.asList("leaps"), SynonymMap
                .makeTokens(Arrays.asList("jumps", "hops", "leaps")), false,
                true);
        synonymMap = analyzer.synonymMap;
    }

    private void setupLocalSynonymMap() throws Exception {
        synonymMap = new SynonymMap(true);

        synonymMap.add(Arrays.asList("jumps"), SynonymMap.makeTokens(Arrays
                .asList("jumps", "hops", "leaps")), false, true);
        synonymMap.add(Arrays.asList("hops"), SynonymMap.makeTokens(Arrays
                .asList("jumps", "hops", "leaps")), false, true);
        synonymMap.add(Arrays.asList("leaps"), SynonymMap.makeTokens(Arrays
                .asList("jumps", "hops", "leaps")), false, true);

        analyzer = new SynonymAnalyzer(synonymMap);
    }

    private void setupSearcher() throws Exception {

        RAMDirectory directory = new RAMDirectory();

        IndexWriter writer = new IndexWriter(directory, analyzer,
                IndexWriter.MaxFieldLength.UNLIMITED);
        Document doc = new Document();
        doc.add(new Field("content", INPUT, Field.Store.YES,
                Field.Index.ANALYZED));
        writer.addDocument(doc);

        writer.close();

        searcher = new IndexSearcher(directory, false);

    }

    @After
    public void tearDown() throws Exception {
        if (searcher != null) {
            searcher.close();
        }
    }

    @Test
    public void testDefaultSynonymMap() {
        SynonymAnalyzer.defaultSynonymMap = null;
        new SynonymAnalyzer(null);
        Assert.assertNotNull(SynonymAnalyzer.defaultSynonymMap);
    }

    @Test
    public void testDefaultTokens() throws Exception {
        setupDefaultSynonymMap();
        setupSearcher();
        String[] keywords = new String[] { "hops", "leaps" };
        for (String keyword : keywords) {
            List<Token> toks = getTokList(keyword, true);
            List<String> strTokens = new ArrayList<String>();
            for (Token t : toks) {
                strTokens.add(t.term());
            }
            Assert.assertTrue(strTokens.contains("jumps"));
            Assert.assertTrue(strTokens.contains("hops"));
            Assert.assertTrue(strTokens.contains("leaps"));
        }
    }

    @Test
    public void testLocalTokens() throws Exception {
        setupLocalSynonymMap();
        setupSearcher();
        String[] keywords = new String[] { "hops", "leaps" };
        for (String keyword : keywords) {
            List<Token> toks = getTokList(keyword, true);
            Assert.assertEquals("unexpected tokens " + toks, 3, toks.size());
            Assert.assertEquals("jumps", toks.get(0).term());
            Assert.assertEquals("hops", toks.get(1).term());
            Assert.assertEquals("leaps", toks.get(2).term());
        }
    }

    @Test
    public void testSearchByAPI() throws Exception {
        setupLocalSynonymMap();
        setupSearcher();
        String[] keywords = new String[] { "jumps", "hops", "leaps" };
        for (String keyword : keywords) {
            TermQuery tq = new TermQuery(new Term("content", keyword));
            Assert.assertEquals("unexpected result for " + keyword + " using "
                    + synonymMap, 1, LuceneTestUtils.hitCount(searcher, tq));

            PhraseQuery pq = new PhraseQuery();
            pq.add(new Term("content", keyword));
            Assert.assertEquals("unexpected result for " + keyword, 1,
                    LuceneTestUtils.hitCount(searcher, pq));
        }
    }

    SynonymFilter getFilter(String input) {
        final List<Token> toks = LuceneUtils.tokens(input);
        TokenStream ts = new TokenStream() {
            Iterator<Token> iter = toks.iterator();

            @Override
            public Token next() throws IOException {
                return iter.hasNext() ? (Token) iter.next() : null;
            }
        };

        return new SynonymFilter(ts, synonymMap);
    }

    List<Token> getTokList(String input, boolean includeOrig)
            throws IOException {
        if (input == null) {
            throw new NullPointerException("input is not specified");
        }
        List<Token> lst = new ArrayList<Token>();

        SynonymFilter sf = getFilter(input);

        Token target = new Token();
        while (true) {
            Token t = sf.next(target);
            if (t == null) {
                break;
            }
            lst.add((Token) t.clone());

        }
        return lst;
    }

}

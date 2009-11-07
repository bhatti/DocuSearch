/**
 * 
 */
package com.plexobject.docusearch.index.lucene;

import java.util.Collection;
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
import com.plexobject.docusearch.persistence.SimpleDocumentsIterator;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.RankedTerm;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.lucene.QueryImpl;

import org.apache.log4j.BasicConfigurator;
import org.apache.solr.analysis.SynonymMap;

/**
 * @author Shahzad Bhatti
 * 
 */
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

        // query(new POSAnalyzer());
        query(new PorterAnalyzer());
        query(new DiacriticAnalyzer());
        LuceneUtils.setDefaultAnalyzer(saved);

    }

    @Test
    public void testIndexWithKeywordsAndRecency() throws Exception {
        int succeeded = index();
        final QueryPolicy policy = newQueryPolicy();

        final QueryCriteria criteria = new QueryCriteria();
        criteria.setKeywords("hat");
        criteria.setRecencyFactor(365, 2);
        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        List<SearchDoc> results = query.search(criteria, policy, true, 1, 10);

        Assert.assertEquals(2, succeeded);
        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testExplanation() throws Exception {
        index();
        final QueryPolicy policy = newQueryPolicy();

        final QueryCriteria criteria = new QueryCriteria();
        criteria.setKeywords("hat");
        criteria.setRecencyFactor(365, 2);
        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        Collection<String> explanation = query.explain(criteria, policy, 1, 10);
        Assert.assertEquals(2, explanation.size());
    }

    @Test
    public void testSimilarDocuments() throws Exception {
        ram = new RAMDirectory();

        int succeeded = 0;
        succeeded = index("1", 7,
                new String[] { "content", "this hat is green" }, null, true);
        succeeded += index("2", 42, new String[] { "content",
                "this hat is green" }, null, true);

        final QueryPolicy policy = new QueryPolicy();
        policy.add(DB_NAME + ".content");
        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        List<SearchDoc> results = query.moreLikeThis(0, policy, 1, 10);

        Assert.assertEquals(0, results.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSimilarDocumentsWithBigNumber() throws Exception {
        index();
        final QueryPolicy policy = newQueryPolicy();

        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        List<SearchDoc> results = query.moreLikeThis(2, policy, 1, 10);

        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testTopTerms() throws Exception {
        index();
        final QueryPolicy policy = newQueryPolicy();

        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        Collection<RankedTerm> terms = query.getTopRankingTerms(policy, 10);
        Assert.assertEquals(3, terms.size());
        for (RankedTerm term : terms) {
            Assert.assertEquals(DB_NAME + ".content", term.getName());
            Assert.assertTrue(term.getFrequency() > 0);
        }
    }

    @Test
    public void testIndexWithMultwordKewords() throws Exception {
        int succeeded = index("1", 7, new String[] { "name",
                "this hat is green", "title", "luke for lucene", "tags",
                "[{name:tag1, rank:1}, {name:tag2, rank:2}]", "date",
                "{'month':'10', 'day':'9', 'year':'2009'}" }, new String[] {
                "name", "title", "tags[name]", "date{year}" }, true);
        Assert.assertEquals(1, succeeded);

        succeeded = index("2", 42, new String[] { "name", "this hat is blue",
                "title", "indxing search", "tags",
                "[{name:tag1, rank:1}, {name:tag2, rank:2}]", "date",
                "{'month':'10', 'day':'9', 'year':'2009'}" }, new String[] {
                "name", "title", "tags[name]", "date{year}" }, false);
        Assert.assertEquals(1, succeeded);

        final QueryCriteria criteria = new QueryCriteria();
        criteria.setKeywords("hat");
        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        final QueryPolicy policy = newQueryPolicy();

        List<SearchDoc> results = query.search(criteria, policy, true, 1, 10);
        Assert.assertEquals(2, results.size());
    }

    private int index() throws Exception {
        ram = new RAMDirectory();

        int succeeded = 0;
        succeeded = index("1", 7,
                new String[] { "content", "this hat is green" }, null, true);
        succeeded += index("2", 42, new String[] { "content",
                "this hat is blue" }, null, false);
        return succeeded;
    }

    private int index(String id, final int score, final String[] fields,
            final String[] indexFields, final boolean deleteExisting)
            throws Exception {

        final Map<String, Object> attrs = new HashMap<String, Object>();
        for (int i = 0; i < fields.length - 1; i += 2) {
            attrs.put(fields[i], fields[i + 1]);
        }

        final Document doc = new DocumentBuilder(DB_NAME).putAll(attrs).setId(
                id).setRevision("REV").build();
        final IndexPolicy policy = new IndexPolicy();
        policy.setAddToDictionary(true);
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
        final IndexerImpl indexer = new IndexerImpl(ram, DB_NAME);
        return indexer.index(policy, new SimpleDocumentsIterator(doc),
                deleteExisting);
    }

    private static QueryPolicy newQueryPolicy() {
        final QueryPolicy policy = new QueryPolicy();
        policy.add(DB_NAME + ".content");

        policy.add(DB_NAME + ".name");
        policy.add(DB_NAME + ".title");
        policy.add(DB_NAME + ".tags[name]");
        policy.add(DB_NAME + ".date{year}");
        return policy;
    }

    private void query(final Analyzer analyzer) throws Exception {
        LuceneUtils.setDefaultAnalyzer(analyzer);
        int succeeded = index();
        final QueryCriteria criteria = new QueryCriteria();
        criteria.setScoreQuery();
        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        final QueryPolicy policy = newQueryPolicy();

        List<SearchDoc> results = query.search(criteria, policy, true, 1, 10);

        Assert.assertEquals(2, succeeded);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(1, results.get(0).getHitDocumentNumber());
        Assert.assertEquals(42, (int) results.get(0).getScore());
        Assert.assertEquals(0, results.get(1).getHitDocumentNumber());
        Assert.assertEquals(7, (int) results.get(1).getScore());
    }
}

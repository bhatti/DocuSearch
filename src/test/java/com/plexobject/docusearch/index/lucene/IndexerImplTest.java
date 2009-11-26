/**
 * 
 */
package com.plexobject.docusearch.index.lucene;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.analysis.SynonymMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.lucene.analyzer.DiacriticAnalyzer;
import com.plexobject.docusearch.lucene.analyzer.MetaphoneReplacementAnalyzer;
import com.plexobject.docusearch.lucene.analyzer.PorterAnalyzer;
import com.plexobject.docusearch.lucene.analyzer.SynonymAnalyzer;
import com.plexobject.docusearch.persistence.SimpleDocumentsIterator;
import com.plexobject.docusearch.query.CriteriaBuilder;
import com.plexobject.docusearch.query.LookupPolicy;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.RankedTerm;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.lucene.QueryImpl;
import com.plexobject.docusearch.query.lucene.QueryUtils;

/**
 * @author Shahzad Bhatti
 * 
 */
public class IndexerImplTest {
    private static final String OWNER = "shahbhat";
    private static final String RANK = "rank";
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
    public final void testSpatialSearch() throws Exception {
        Directory directory = LuceneUtils.toFSDirectory(new File("spatial"));
        final IndexPolicy indexPolicy = new IndexPolicy();
        indexPolicy.add("name", false, false, false, 0.0F, false, false, false);
        indexPolicy.add("lat", false, false, false, 0.0F, true, true, false);
        indexPolicy.add("lng", false, false, false, 0.0F, true, false, true);
        indexSpatial(indexPolicy, directory);

        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.add(DB_NAME + ".name");
        final QueryImpl query = new QueryImpl(directory, DB_NAME);
        final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                "Restaurant").setLatitude(38.8725000).setLongitude(-77.3829000)
                .setRadius(8).build();
        List<SearchDoc> results = query.search(criteria, indexPolicy,
                queryPolicy, true, 0, 10);
        Assert.assertEquals("unexpectes results " + results, 0, results.size());

    }

    private void indexSpatial(final IndexPolicy policy,
            final Directory directory) throws Exception {

        indexSpatial(policy, directory,
                "McCormick & Schmick's Seafood Restaurant", 38.9579000,
                -77.3572000);
        indexSpatial(policy, directory, "Jimmy's Old Town Tavern", 38.9690000,
                -77.3862000);
        indexSpatial(policy, directory, "Ned Devine's", 38.9510000, -77.4107000);
        indexSpatial(policy, directory, "Old Brogue Irish Pub", 38.9955000,
                -77.2884000);
        indexSpatial(policy, directory, "Alf Laylah Wa Laylah", 38.8956000,
                -77.4258000);
        indexSpatial(policy, directory, "Sully's Restaurant & Supper",
                38.9003000, -77.4467000);
        indexSpatial(policy, directory, "TGIFriday", 38.8725000, -77.3829000);
        indexSpatial(policy, directory, "Potomac Swing Dance Club", 38.9027000,
                -77.2639000);
        indexSpatial(policy, directory, "White Tiger Restaurant", 38.9027000,
                -77.2638000);
        indexSpatial(policy, directory, "Jammin' Java", 38.9039000, -77.2622000);
        indexSpatial(policy, directory, "Potomac Swing Dance Club", 38.9027000,
                -77.2639000);
        indexSpatial(policy, directory, "WiseAcres Comedy Club", 38.9248000,
                -77.2344000);
        indexSpatial(policy, directory, "Glen Echo Spanish Ballroom",
                38.9691000, -77.1400000);
        indexSpatial(policy, directory, "Whitlow's on Wilson", 38.8889000,
                -77.0926000);
        indexSpatial(policy, directory, "Iota Club and Cafe", 38.8890000,
                -77.0923000);
        indexSpatial(policy, directory, "Hilton Washington Embassy Row",
                38.9103000, -77.0451000);
        indexSpatial(policy, directory, "HorseFeathers, Bar & Grill",
                39.01220000000001, -77.3942);

    }

    private int indexSpatial(final IndexPolicy policy, Directory directory,
            String name, double lat, double lng) throws Exception {

        final Map<String, Object> attrs = new HashMap<String, Object>();

        attrs.put("name", name);
        attrs.put("lat", lat);
        attrs.put("lng", lng);

        final Document doc = new DocumentBuilder(DB_NAME).setId(name).putAll(
                attrs).build();

        final IndexerImpl indexer = new IndexerImpl(directory, DB_NAME);
        return indexer.index(policy, new SimpleDocumentsIterator(doc), true);
    }

    @Test
    public void testNonexistingOwner() throws Exception {
        for (QueryUtils.SearchScheme scheme : QueryUtils.SearchScheme.values()) {
            QueryUtils.setSearchScheme(scheme);
            int succeeded = index();
            final QueryPolicy queryPolicy = newQueryPolicy();

            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    "hat").setOwner("nonexisting").setRecencyFactor(365, 2)
                    .build();
            final QueryImpl query = new QueryImpl(ram, DB_NAME);
            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 1, 10);

            Assert.assertEquals(2, succeeded);
            Assert.assertEquals("scheme " + scheme + ", unexpectes results "
                    + results.size() + "-->" + results, 0, results.size());
        }
    }

    @Test
    public void testIndexWithKeywordsAndRecency() throws Exception {
        int succeeded = index();
        final QueryPolicy queryPolicy = newQueryPolicy();

        final QueryCriteria criteria = new CriteriaBuilder().setKeywords("hat")
                .setOwner(OWNER).setRecencyFactor(365, 2).build();
        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                true, 0, 10);

        Assert.assertEquals(2, succeeded);
        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testIndexQueryWithoutOwner() throws Exception {
        int succeeded = index();
        final QueryPolicy queryPolicy = newQueryPolicy();

        final QueryCriteria criteria = new CriteriaBuilder().setKeywords("hat")
                .setRecencyFactor(365, 2).build();
        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                true, 0, 10);

        Assert.assertEquals(2, succeeded);
        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testPartialLookup() throws Exception {
        int succeeded = index();
        final LookupPolicy queryPolicy = newLookupPolicy();

        final QueryCriteria criteria = new CriteriaBuilder().setKeywords("ha*")
                .setOwner(OWNER).build();
        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        List<String> results = query.partialLookup(criteria, null, queryPolicy,
                10);

        Assert.assertEquals(2, succeeded);
        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testPartialLookupWithMiddle() throws Exception {
        index();
        final LookupPolicy queryPolicy = newLookupPolicy();

        final QueryCriteria criteria = new CriteriaBuilder().setKeywords("at")
                .setOwner(OWNER).build();
        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        List<String> results = query.partialLookup(criteria, null, queryPolicy,
                10);

        Assert.assertEquals(0, results.size());
    }

    @Test
    public void testExplanation() throws Exception {
        index();
        final QueryPolicy queryPolicy = newQueryPolicy();

        final QueryCriteria criteria = new CriteriaBuilder().setKeywords("hat")
                .setOwner(OWNER).setRecencyFactor(365, 2).build();
        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        Collection<String> explanation = query.explain(criteria, null,
                queryPolicy, 0, 10);
        Assert.assertEquals(2, explanation.size());
    }

    @Test
    public void testSimilarDocuments() throws Exception {
        ram = new RAMDirectory();

        int succeeded = 0;
        succeeded = index("1", OWNER, 7, new String[] { "content",
                "this hat is green" }, null, true);
        succeeded += index("2", null, 42, new String[] { "content",
                "this hat is green" }, null, true);

        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.add(DB_NAME + ".content");
        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        List<SearchDoc> results = query.moreLikeThis("1", 0, null, queryPolicy,
                0, 10);

        Assert.assertEquals(1, results.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSimilarDocumentsWithBigNumber() throws Exception {
        index();
        final QueryPolicy queryPolicy = newQueryPolicy();

        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        List<SearchDoc> results = query.moreLikeThis("1", 2, null, queryPolicy,
                0, 10);

        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testTopTerms() throws Exception {
        index();
        final QueryPolicy policy = newQueryPolicy();

        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        Collection<RankedTerm> terms = query.getTopRankingTerms(policy, 10);
        for (RankedTerm term : terms) {
            Assert.assertTrue(term.getName().matches(
                    DB_NAME + ".(content|rank)"));
            Assert.assertTrue(term.getFrequency() > 0);
        }
    }

    @Test
    public void testIndexWithMultwordKewords() throws Exception {
        int succeeded = index("1", OWNER, 7, new String[] { "name",
                "this hat is green", "title", "luke for lucene", "tags",
                "[{name:tag1, rank:1}, {name:tag2, rank:2}]", "date",
                "{'month':'10', 'day':'9', 'year':'2009'}" }, new String[] {
                "name", "title", "tags[name]", "date{year}" }, true);
        Assert.assertEquals(1, succeeded);

        succeeded = index("2", null, 42, new String[] { "name",
                "this hat is blue", "title", "indxing search", "tags",
                "[{name:tag1, rank:1}, {name:tag2, rank:2}]", "date",
                "{'month':'10', 'day':'9', 'year':'2009'}" }, new String[] {
                "name", "title", "tags[name]", "date{year}" }, false);
        Assert.assertEquals(1, succeeded);

        final QueryCriteria criteria = new CriteriaBuilder().setKeywords("hat")
                .setOwner(OWNER).build();
        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        final QueryPolicy queryPolicy = newQueryPolicy();

        List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                true, 0, 10);
        Assert.assertEquals(2, results.size());
    }

    private int index() throws Exception {
        ram = new RAMDirectory();

        int succeeded = 0;
        succeeded = index("1", OWNER, 7, new String[] { "content",
                "this hat is green" }, null, true);
        succeeded += index("2", null, 42, new String[] { "content",
                "this hat is blue" }, null, false);
        return succeeded;
    }

    private int index(final String id, final String owner, final int score,
            final String[] fields, final String[] indexFields,
            final boolean deleteExisting) throws Exception {

        final Map<String, Object> attrs = new HashMap<String, Object>();
        for (int i = 0; i < fields.length - 1; i += 2) {
            attrs.put(fields[i], fields[i + 1]);
        }
        attrs.put(RANK, 100 - Integer.parseInt(id));

        final Document doc = new DocumentBuilder(DB_NAME).putAll(attrs).setId(
                id).setRevision("REV").build();
        final IndexPolicy policy = new IndexPolicy();
        policy.setAddToDictionary(true);
        policy.add(RANK, false, false, false, 0.0F, false, false, false);
        policy.setOwner(owner);

        if (indexFields == null) {
            for (int i = 0; i < fields.length - 1; i += 2) {
                policy.add(fields[i], true, true, true, 0.0F, false, false,
                        false);
            }
        } else {
            for (int i = 0; i < indexFields.length; i++) {
                policy.add(indexFields[i], true, true, true, 0.0F, false,
                        false, false);
            }
        }
        policy.setScore(score);
        final IndexerImpl indexer = new IndexerImpl(ram, DB_NAME);
        return indexer.index(policy, new SimpleDocumentsIterator(doc),
                deleteExisting);
    }

    private static QueryPolicy newQueryPolicy() {
        final QueryPolicy policy = new QueryPolicy();
        policy.add(DB_NAME + ".content", 0, true, 0.0F,
                QueryPolicy.FieldType.STRING);
        policy.add(DB_NAME + ".rank", 1, true, 0.0F,
                QueryPolicy.FieldType.INTEGER);

        policy.add(DB_NAME + ".name", 0, true, 0.0F,
                QueryPolicy.FieldType.STRING);
        policy.add(DB_NAME + ".title", 0, true, 0.0F,
                QueryPolicy.FieldType.STRING);
        policy.add(DB_NAME + ".tags[name]", 0, true, 0.0F,
                QueryPolicy.FieldType.STRING);
        policy.add(DB_NAME + ".date{year}", 0, true, 0.0F,
                QueryPolicy.FieldType.STRING);
        return policy;
    }

    private LookupPolicy newLookupPolicy() {
        final LookupPolicy policy = new LookupPolicy();
        policy.add(DB_NAME + ".content");
        policy.setFieldToReturn(DB_NAME + ".content");
        return policy;
    }

    private void query(final Analyzer analyzer) throws Exception {
        for (QueryUtils.SearchScheme scheme : QueryUtils.SearchScheme.values()) {
            QueryUtils.setSearchScheme(scheme);
            LuceneUtils.setDefaultAnalyzer(analyzer);
            int succeeded = index();
            final QueryCriteria criteria = new CriteriaBuilder()
                    .setScoreQuery().setOwner(OWNER).build();
            final QueryImpl query = new QueryImpl(ram, DB_NAME);
            final QueryPolicy queryPolicy = newQueryPolicy();

            final List<SearchDoc> results = query.search(criteria, null,
                    queryPolicy, true, 0, 10);

            Assert.assertEquals(2, succeeded);
            Assert.assertEquals("unexpectes results " + results.size() + "-->"
                    + results, 2, results.size());
            Assert.assertEquals("failed with scheme " + scheme + ", results "
                    + results, 1, results.get(0).getHitDocumentNumber());
            Assert.assertEquals("failed with scheme " + scheme + ", results "
                    + results, 42, (int) results.get(0).getScore());
            Assert.assertEquals("failed with scheme " + scheme + ", results "
                    + results, 0, results.get(1).getHitDocumentNumber());
            Assert.assertEquals("failed with scheme " + scheme + ", results "
                    + results, 7, (int) results.get(1).getScore());
        }
    }
}

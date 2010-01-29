/**
 * 
 */
package com.plexobject.docusearch.index.lucene;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.analysis.SynonymMap;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.cache.CacheFlusher;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.domain.Pair;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.index.Indexer;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.lucene.analyzer.DiacriticAnalyzer;
import com.plexobject.docusearch.lucene.analyzer.MetaphoneReplacementAnalyzer;
import com.plexobject.docusearch.lucene.analyzer.PorterAnalyzer;
import com.plexobject.docusearch.lucene.analyzer.SynonymAnalyzer;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PagedList;
import com.plexobject.docusearch.persistence.SimpleDocumentsIterator;
import com.plexobject.docusearch.query.CriteriaBuilder;
import com.plexobject.docusearch.query.LookupPolicy;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.RankedTerm;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.lucene.QueryImpl;
import com.plexobject.docusearch.query.lucene.QueryUtils;
import com.plexobject.docusearch.util.TimeUtils;
import com.plexobject.docusearch.util.TimeUtils.TimeSource;

/**
 * @author Shahzad Bhatti
 * 
 */
public class IndexerImplTest {
    private static final String OWNER = "shahbhat";
    private static final String RANK = "rank";
    protected static final String DB_NAME = "IndexerImplTestDB";
    private Directory ram;
    DocumentRepository documentRepository;
    private File tmpdir = new File(LuceneUtils.INDEX_DIR, DB_NAME);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        tmpdir.mkdirs();
        tmpdir.deleteOnExit();

        CacheFlusher.getInstance().flushCaches();
        BasicConfigurator.configure();
        ram = new RAMDirectory();
        documentRepository = EasyMock.createMock(DocumentRepository.class);

    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        tmpdir.delete();
        FileUtils.deleteDirectory(new File("spatial"));
        FileUtils.deleteDirectory(new File(DB_NAME));
        CacheFlusher.getInstance().flushCaches();
    }

    // @Test
    public void testQueryIndexDateRange() throws Exception {
        ram = new RAMDirectory();

        final IndexerImpl indexer = new IndexerImpl(ram, DB_NAME);
        final List<String> tags = Arrays.asList("one", "two", "three", "four",
                "five", "six", "seven", "eight", "nine", "ten");
        for (int i = 0; i < tags.size(); i++) {
            Assert.assertEquals(1, indexTag(indexer, tags.get(i), i + 1));
        }

        final QueryImpl query = new QueryImpl(ram, DB_NAME);

        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.add(DB_NAME + ".contents");
        final String[] patternArray = { "MM/dd/yyyy" };

        for (int i = 0; i < tags.size(); i++) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tags.get(i)).setIndexDateRange(
                    DateUtils.parseDate("01/01/2009", patternArray),
                    DateUtils.parseDate("01/05/2009", patternArray)).build();
            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals("while searching " + i + " with criteria "
                    + criteria + " found unexpected result " + results,
                    i < 5 ? 1 : 0, results.size());
        }
    }

    @Test
    public void testIndexDefaultConstructor() throws Exception {
        Indexer indexer = new IndexerImpl(tmpdir);
        final IndexPolicy policy = new IndexPolicy();
        policy.add("contents", false, null, false, false, 0.0F, false, false,
                false);
        final String secondaryId = null;
        final boolean deleteExisting = false;
        Assert.assertEquals(0, indexer.index(policy,
                new Iterator<List<Document>>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public List<Document> next() {
                        return null;
                    }

                    @Override
                    public void remove() {

                    }
                }, secondaryId, deleteExisting));
    }

    @Test(expected = NullPointerException.class)
    public void testIndexNullPolicy() throws Exception {
        Indexer indexer = new IndexerImpl(tmpdir);
        final IndexPolicy policy = null;
        final String secondaryId = null;
        final boolean deleteExisting = false;
        Assert.assertEquals(1, indexer.index(policy,
                new Iterator<List<Document>>() {
                    int count = 0;

                    @Override
                    public boolean hasNext() {
                        return ++count <= 1;
                    }

                    @Override
                    public List<Document> next() {
                        return Arrays.asList(new DocumentBuilder().setId(
                                "id" + count).setDatabase(DB_NAME).put(
                                "contents", "bag" + count).build());
                    }

                    @Override
                    public void remove() {

                    }
                }, secondaryId, deleteExisting));
    }

    @Test
    public void testIndexThousandsOfDocuments() throws Exception {
        Indexer indexer = new IndexerImpl(tmpdir);
        final IndexPolicy policy = new IndexPolicy();
        policy.add("contents", false, null, false, false, 0.0F, false, false,
                false);
        final String secondaryId = null;
        final boolean deleteExisting = false;
        Assert.assertEquals(1001, indexer.index(policy,
                new Iterator<List<Document>>() {
                    int count = 0;

                    @Override
                    public boolean hasNext() {
                        return ++count <= 1001;
                    }

                    @Override
                    public List<Document> next() {
                        return Arrays.asList(new DocumentBuilder().setId(
                                "id" + count).setDatabase(DB_NAME).put(
                                "contents", "bag" + count).build());
                    }

                    @Override
                    public void remove() {

                    }
                }, secondaryId, deleteExisting));

        final QueryImpl query = new QueryImpl(DB_NAME);
        final QueryCriteria criteria = new CriteriaBuilder()
                .setKeywords("bag1").build();
        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.add(DB_NAME + ".contents");
        for (int i = 0; i < 100; i++) {
            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals(1, results.size());
        }
    }

    @Test
    public void testRemoveIndex() throws Exception {
        Indexer indexer = new IndexerImpl(tmpdir);
        final IndexPolicy policy = new IndexPolicy();
        policy.add("contents", true, null, false, false, 0.0F, false, false,
                false);
        final String secondaryId = null;
        final boolean deleteExisting = false;
        final List<String> tags = Arrays.asList("one", "two", "three", "four",
                "five", "six", "seven", "eight", "nine", "ten");
        Assert.assertEquals(tags.size(), indexer.index(policy,
                new Iterator<List<Document>>() {
                    int count = 0;

                    @Override
                    public boolean hasNext() {
                        return ++count <= tags.size();
                    }

                    @Override
                    public List<Document> next() {
                        return Arrays.asList(new DocumentBuilder().setId(
                                "id" + count).setDatabase(DB_NAME).put(
                                "contents", tags.get(count - 1)).build());
                    }

                    @Override
                    public void remove() {

                    }
                }, secondaryId, deleteExisting));

        QueryImpl query = new QueryImpl(DB_NAME);

        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.add(DB_NAME + ".contents");
        for (String tag : tags) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tag).build();
            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals("while searching " + criteria
                    + " found unexpected result " + results, 1, results.size());
        }
        final String secondaryIdName = null;
        final Collection<Pair<String, String>> primaryAndSecondaryIds = new ArrayList<Pair<String, String>>();
        for (int i = 0; i < 5; i++) {
            primaryAndSecondaryIds.add(new Pair<String, String>("id" + (i + 1),
                    null));
        }
        final int olderThanDays = 0;
        int deleted = indexer.removeIndexedDocuments(DB_NAME, secondaryIdName,
                primaryAndSecondaryIds, olderThanDays);
        Assert.assertTrue(deleted >= 5);
        query = new QueryImpl(DB_NAME);

        for (int i = 0; i < 5; i++) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tags.get(i)).build();

            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals("while searching " + criteria
                    + " found unexpected result " + results, 0, results.size());
        }
        for (int i = 5; i < tags.size(); i++) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tags.get(i)).build();

            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals(1, results.size());
        }
    }

    @Test
    public void testRemoveIndexWithSecondary() throws Exception {
        Indexer indexer = new IndexerImpl(tmpdir);
        final IndexPolicy policy = new IndexPolicy();
        policy.add("contents", true, null, false, false, 0.0F, false, false,
                false);
        policy.add("secondary", true, "secondary", false, false, 0.0F, false,
                false, false);
        final String secondaryId = null;
        final boolean deleteExisting = false;
        final List<String> tags = Arrays.asList("one", "two", "three", "four",
                "five", "six", "seven", "eight", "nine", "ten");
        Assert.assertEquals(tags.size(), indexer.index(policy,
                new Iterator<List<Document>>() {
                    int count = 0;

                    @Override
                    public boolean hasNext() {
                        return ++count <= tags.size();
                    }

                    @Override
                    public List<Document> next() {
                        return Arrays.asList(new DocumentBuilder().setId(
                                "id" + count).setDatabase(DB_NAME).put(
                                "secondary", "sec" + count).put("contents",
                                tags.get(count - 1)).build());
                    }

                    @Override
                    public void remove() {

                    }
                }, secondaryId, deleteExisting));

        QueryImpl query = new QueryImpl(DB_NAME);

        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.add(DB_NAME + ".contents");
        for (String tag : tags) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tag).build();
            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals("while searching " + criteria
                    + " found unexpected result " + results, 1, results.size());
        }
        final String secondaryIdName = "secondary";
        final Collection<Pair<String, String>> primaryAndSecondaryIds = new ArrayList<Pair<String, String>>();
        for (int i = 0; i < 5; i++) {
            primaryAndSecondaryIds.add(new Pair<String, String>("id" + (i + 1),
                    "sec" + (i + 1)));
        }
        final int olderThanDays = 0;
        int deleted = indexer.removeIndexedDocuments(DB_NAME, secondaryIdName,
                primaryAndSecondaryIds, olderThanDays);
        Assert.assertEquals(5, deleted);
        query = new QueryImpl(DB_NAME);

        for (int i = 0; i < 5; i++) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tags.get(i)).build();

            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals("while searching " + criteria
                    + " found unexpected result " + results, 0, results.size());
        }
        for (int i = 5; i < tags.size(); i++) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tags.get(i)).build();

            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals(1, results.size());
        }
    }

    @Test
    public void testRemoveIndexWithIndexDateAndIds() throws Exception {

        Indexer indexer = new IndexerImpl(tmpdir);
        final List<String> tags = Arrays.asList("one", "two", "three", "four",
                "five", "six", "seven", "eight", "nine", "ten");
        for (int i = 0; i < tags.size(); i++) {
            Assert.assertEquals(1, indexTag(indexer, tags.get(i), i + 1));
        }

        QueryImpl query = new QueryImpl(DB_NAME);

        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.add(DB_NAME + ".contents");
        for (String tag : tags) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tag).build();
            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals("while searching " + criteria
                    + " found unexpected result " + results, 1, results.size());
        }
        final String secondaryIdName = "secondary";
        final Collection<Pair<String, String>> primaryAndSecondaryIds = new ArrayList<Pair<String, String>>();
        for (int i = 0; i < 5; i++) {
            primaryAndSecondaryIds.add(new Pair<String, String>("id" + (i + 1),
                    "sec" + (i + 1)));
        }
        final int olderThanDays = 5;
        int deleted = indexer.removeIndexedDocuments(DB_NAME, secondaryIdName,
                primaryAndSecondaryIds, olderThanDays);
        Assert.assertEquals(5, deleted);
        query = new QueryImpl(DB_NAME);

        for (int i = 0; i < 5; i++) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tags.get(i)).build();

            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals("while searching " + criteria
                    + " found unexpected result " + results, 0, results.size());
        }
        for (int i = 5; i < tags.size(); i++) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tags.get(i)).build();

            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals(1, results.size());
        }
    }

    @Test
    public void testRemoveIndexWithIndexDate() throws Exception {

        Indexer indexer = new IndexerImpl(tmpdir);
        final List<String> tags = Arrays.asList("one", "two", "three", "four",
                "five", "six", "seven", "eight", "nine", "ten");
        for (int i = 0; i < tags.size(); i++) {
            Assert.assertEquals(1, indexTag(indexer, tags.get(i), i + 1));
        }

        QueryImpl query = new QueryImpl(DB_NAME);

        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.add(DB_NAME + ".contents");
        for (String tag : tags) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tag).build();
            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals("while searching " + criteria
                    + " found unexpected result " + results, 1, results.size());
        }
        final String secondaryIdName = "secondary";
        final Collection<Pair<String, String>> primaryAndSecondaryIds = null;
        final int olderThanDays = 5;
        int deleted = indexer.removeIndexedDocuments(DB_NAME, secondaryIdName,
                primaryAndSecondaryIds, olderThanDays);
        Assert.assertEquals(5, deleted);
        query = new QueryImpl(DB_NAME);

        for (int i = 0; i < 5; i++) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tags.get(i)).build();

            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals("while searching " + criteria
                    + " found unexpected result " + results, 0, results.size());
        }
        for (int i = 5; i < tags.size(); i++) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tags.get(i)).build();

            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals(1, results.size());
        }
    }

    @Test
    public void testUpdateIndex() throws Exception {
        Indexer indexer = new IndexerImpl(tmpdir);
        final IndexPolicy policy = new IndexPolicy();
        policy.add("contents", true, null, false, false, 0.0F, false, false,
                false);
        final String secondaryId = null;
        final boolean deleteExisting = false;
        final List<String> tags = Arrays.asList("one", "two", "three", "four",
                "five", "six", "seven", "eight", "nine", "ten");
        Iterator<List<Document>> it = docIteratorForTags(tags);
        Assert.assertEquals(tags.size(), indexer.index(policy, it, secondaryId,
                deleteExisting));

        QueryImpl query = new QueryImpl(DB_NAME);

        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.add(DB_NAME + ".contents");
        for (String tag : tags) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tag).build();
            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals("while searching " + criteria
                    + " found unexpected result " + results, 1, results.size());
        }
        tags.set(0, "eleven");
        tags.set(1, "tweleve");
        tags.set(2, "thirteen");
        it = docIteratorForTags(tags);

        Assert.assertEquals(tags.size(), indexer.index(policy, it, secondaryId,
                deleteExisting));

        query = new QueryImpl(DB_NAME);
        Assert.assertEquals(0, query.search(
                new CriteriaBuilder().setKeywords("one").build(), null,
                queryPolicy, true, 0, 1000).size());
        Assert.assertEquals(0, query.search(
                new CriteriaBuilder().setKeywords("two").build(), null,
                queryPolicy, true, 0, 1000).size());
        Assert.assertEquals(0, query.search(
                new CriteriaBuilder().setKeywords("three").build(), null,
                queryPolicy, true, 0, 1000).size());

        for (String tag : tags) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tag).build();
            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals("while searching " + criteria
                    + " found unexpected result " + results, 1, results.size());
        }
    }

    @Test
    public void testUpdateIndexWithSecondary() throws Exception {
        Indexer indexer = new IndexerImpl(tmpdir);
        final IndexPolicy policy = new IndexPolicy();
        policy.add("contents", true, null, false, false, 0.0F, false, false,
                false);
        policy.add("secondary", true, "secondary", false, false, 0.0F, false,
                false, false);
        final String secondaryId = "secondary";
        final boolean deleteExisting = false;
        final List<String> tags = Arrays.asList("one", "two", "three", "four",
                "five", "six", "seven", "eight", "nine", "ten");
        Iterator<List<Document>> it = docIteratorForTags(tags);
        Assert.assertEquals(tags.size(), indexer.index(policy, it, secondaryId,
                deleteExisting));

        QueryImpl query = new QueryImpl(DB_NAME);

        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.add(DB_NAME + ".contents");
        for (String tag : tags) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tag).build();
            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals("while searching " + criteria
                    + " found unexpected result " + results, 1, results.size());
        }

        tags.set(0, "eleven");
        tags.set(1, "tweleve");
        tags.set(2, "thirteen");
        it = docIteratorForTags(tags);

        Assert.assertEquals(tags.size(), indexer.index(policy, it, secondaryId,
                deleteExisting));

        query = new QueryImpl(DB_NAME);
        Assert.assertEquals(0, query.search(
                new CriteriaBuilder().setKeywords("one").build(), null,
                queryPolicy, true, 0, 1000).size());
        Assert.assertEquals(0, query.search(
                new CriteriaBuilder().setKeywords("two").build(), null,
                queryPolicy, true, 0, 1000).size());
        Assert.assertEquals(0, query.search(
                new CriteriaBuilder().setKeywords("three").build(), null,
                queryPolicy, true, 0, 1000).size());

        for (String tag : tags) {
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    tag).build();
            List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                    true, 0, 1000);
            Assert.assertEquals("while searching " + criteria
                    + " found unexpected result " + results, 1, results.size());
        }
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
    public void testNonexistingOwner() throws Exception {
        for (QueryUtils.SearchScheme scheme : QueryUtils.SearchScheme.values()) {
            EasyMock.reset(documentRepository);

            QueryUtils.setSearchScheme(scheme);
            int succeeded = index();
            final QueryPolicy queryPolicy = newQueryPolicy();

            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    "hat").setOwner("nonexisting").setRecencyFactor(365, 2)
                    .build();
            EasyMock.expect(
                    documentRepository.getAllDocuments("test_data", null, null,
                            257)).andReturn(
                    new PagedList<Document>(Arrays.<Document> asList()));
            EasyMock.replay(documentRepository);

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
        EasyMock.expect(
                documentRepository
                        .getAllDocuments("test_data", null, null, 257))
                .andReturn(new PagedList<Document>(Arrays.<Document> asList()));
        EasyMock.replay(documentRepository);
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
        EasyMock.expect(
                documentRepository
                        .getAllDocuments("test_data", null, null, 257))
                .andReturn(new PagedList<Document>(Arrays.<Document> asList()));
        EasyMock.replay(documentRepository);

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
        final QueryImpl query = new QueryImpl(ram, DB_NAME) {
            @Override
            protected QueryImpl getLookupQuery(LookupPolicy lookupPolicy) {
                return new QueryImpl(ram, lookupPolicy.getDictionaryIndex());
            }
        };

        List<String> results = query.partialLookup(criteria, null, queryPolicy,
                10);

        Assert.assertEquals(2, succeeded);
        Assert.assertEquals("unexpected results " + results, 2, results.size());
    }

    @Test
    public void testPartialLookupWithMiddle() throws Exception {
        index();
        final LookupPolicy queryPolicy = newLookupPolicy();

        final QueryCriteria criteria = new CriteriaBuilder().setKeywords("at")
                .setOwner(OWNER).build();
        EasyMock.replay(documentRepository);

        final QueryImpl query = new QueryImpl(ram, DB_NAME) {
            @Override
            protected QueryImpl getLookupQuery(LookupPolicy lookupPolicy) {
                return new QueryImpl(ram, lookupPolicy.getDictionaryIndex());
            }
        };

        List<String> results = query.partialLookup(criteria, null, queryPolicy,
                10);

        Assert.assertEquals(0, results.size());
    }

    @Test
    public void testSearchExplanation() throws Exception {
        index();
        final QueryPolicy queryPolicy = newQueryPolicy();

        final QueryCriteria criteria = new CriteriaBuilder().setKeywords("hat")
                .setOwner(OWNER).setRecencyFactor(365, 2).build();
        EasyMock.expect(
                documentRepository
                        .getAllDocuments("test_data", null, null, 257))
                .andReturn(new PagedList<Document>(Arrays.<Document> asList()));
        EasyMock.replay(documentRepository);

        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        Collection<String> explanation = query.explainSearch(criteria, null,
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
        EasyMock.replay(documentRepository);

        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        List<SearchDoc> results = query.moreLikeThis("1", 0, null, queryPolicy,
                0, 10);

        Assert.assertEquals(1, results.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSimilarDocumentsWithBigNumber() throws Exception {
        index();
        final QueryPolicy queryPolicy = newQueryPolicy();
        EasyMock.replay(documentRepository);

        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        List<SearchDoc> results = query.moreLikeThis("1", 2, null, queryPolicy,
                0, 10);

        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testTopTerms() throws Exception {
        index();
        final QueryPolicy policy = newQueryPolicy();
        EasyMock.replay(documentRepository);

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

        EasyMock.expect(
                documentRepository
                        .getAllDocuments("test_data", null, null, 257))
                .andReturn(new PagedList<Document>(Arrays.<Document> asList()));

        EasyMock.replay(documentRepository);

        final QueryImpl query = new QueryImpl(ram, DB_NAME);
        final QueryPolicy queryPolicy = newQueryPolicy();

        List<SearchDoc> results = query.search(criteria, null, queryPolicy,
                true, 0, 10);
        Assert.assertEquals(2, results.size());
    }

    private void query(final Analyzer analyzer) throws Exception {
        for (QueryUtils.SearchScheme scheme : QueryUtils.SearchScheme.values()) {
            EasyMock.reset(documentRepository);

            QueryUtils.setSearchScheme(scheme);
            LuceneUtils.setDefaultAnalyzer(analyzer);
            int succeeded = index();
            final QueryCriteria criteria = new CriteriaBuilder()
                    .setScoreQuery().setOwner(OWNER).build();
            EasyMock.expect(
                    documentRepository.getAllDocuments("test_data", null, null,
                            257)).andReturn(
                    new PagedList<Document>(Arrays.<Document> asList()));
            EasyMock.replay(documentRepository);

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

    private int index() throws Exception {
        ram = new RAMDirectory();

        int succeeded = 0;
        succeeded = index("1", OWNER, 7, new String[] { "passion", "energy",
                "content", "this hat is green" }, null, true);
        succeeded += index("2", null, 42, new String[] { "passion",
                "technology", "content", "this hat is blue" }, null, false);
        return succeeded;
    }

    private int index(final String id, final String owner, final int score,
            final String[] fields, final String[] indexFields,
            final boolean deleteExisting) throws Exception {

        final Map<String, Object> attrs = new TreeMap<String, Object>();
        for (int i = 0; i < fields.length - 1; i += 2) {
            attrs.put(fields[i], fields[i + 1]);
        }
        attrs.put(RANK, 100 - Integer.parseInt(id));

        final Document doc = new DocumentBuilder(DB_NAME).putAll(attrs).setId(
                id).setRevision("REV").build();
        final IndexPolicy policy = new IndexPolicy();
        policy.setAddToDictionary(true);
        policy.add(RANK, false, null, false, false, 0.0F, false, false, false);
        policy.setOwner(owner);

        if (indexFields == null) {
            for (int i = 0; i < fields.length - 1; i += 2) {
                policy.add(fields[i], true, null, true, true, 0.0F, false,
                        false, false);
            }
        } else {
            for (int i = 0; i < indexFields.length; i++) {
                policy.add(indexFields[i], true, null, true, true, 0.0F, false,
                        false, false);
            }
        }
        policy.setScore(score);
        final IndexerImpl indexer = new IndexerImpl(ram, DB_NAME);
        return indexer.index(policy, new SimpleDocumentsIterator(doc), null,
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
        policy.setDictionaryIndex(DB_NAME);
        policy.setDictionaryField(DB_NAME + ".content");

        return policy;
    }

    private int indexTag(final Indexer indexer, final String tag, final int day) {
        final Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, 2009);
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DATE, day);
        TimeUtils.setTimeSource(new TimeSource() {
            @Override
            public Date getCurrentTime() {
                return calendar.getTime();
            }
        });
        final IndexPolicy policy = new IndexPolicy();
        policy.add("contents", true, null, false, false, 0.0F, false, false,
                false);
        policy.add("secondary", true, "secondary", false, false, 0.0F, false,
                false, false);
        final String secondaryId = "secondary";
        final boolean deleteExisting = false;
        return indexer.index(policy, new Iterator<List<Document>>() {
            boolean first = true;

            @Override
            public boolean hasNext() {
                if (first) {
                    first = false;
                    return true;
                }
                return false;
            }

            @Override
            public List<Document> next() {
                return Arrays.asList(new DocumentBuilder().setId("id" + day)
                        .setDatabase(DB_NAME).put("secondary", "sec" + day)
                        .put("contents", tag).build());
            }

            @Override
            public void remove() {

            }
        }, secondaryId, deleteExisting);
    }

    private Iterator<List<Document>> docIteratorForTags(final List<String> tags) {
        final Iterator<List<Document>> it = new Iterator<List<Document>>() {
            int count = 0;

            @Override
            public boolean hasNext() {
                return ++count <= tags.size();
            }

            @Override
            public List<Document> next() {
                return Arrays.asList(new DocumentBuilder().setId("id" + count)
                        .setDatabase(DB_NAME).put("secondary", "sec" + count)
                        .put("contents", tags.get(count - 1)).build());
            }

            @Override
            public void remove() {

            }
        };
        return it;
    }
}

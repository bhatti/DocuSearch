package com.plexobject.docusearch.index.lucene;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.lucene.store.Directory;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.cache.CacheFlusher;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PagedList;
import com.plexobject.docusearch.persistence.SimpleDocumentsIterator;
import com.plexobject.docusearch.query.CriteriaBuilder;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.lucene.QueryImpl;

public class GeoSpatialSearchTest {
    protected static final String DB_NAME = "GeoSpatialSearchTest";
    DocumentRepository documentRepository;
    private File tmpdir = new File(LuceneUtils.INDEX_DIR, DB_NAME);

    @Before
    public void setUp() throws Exception {
        tmpdir.mkdirs();
        tmpdir.deleteOnExit();

        CacheFlusher.getInstance().flushCaches();
        BasicConfigurator.configure();
        documentRepository = EasyMock.createMock(DocumentRepository.class);
    }

    @After
    public void tearDown() throws Exception {
        tmpdir.delete();
        FileUtils.deleteDirectory(new File("spatial"));
        FileUtils.deleteDirectory(new File(DB_NAME));
        CacheFlusher.getInstance().flushCaches();
    }

    @Test
    public final void testAlwaysSearch() throws Exception {
        Directory directory = LuceneUtils.toFSDirectory(new File("spatial"));
        final IndexPolicy indexPolicy = new IndexPolicy();
        indexPolicy.add("name", false, null, false, false, 0.0F, false, false,
                false);
        indexPolicy.add("lat", false, null, false, false, 0.0F, true, true,
                false);
        indexPolicy.add("lng", false, null, false, false, 0.0F, true, false,
                true);
        indexSpatial(indexPolicy, directory);
        EasyMock.expect(
                documentRepository
                        .getAllDocuments("test_data", null, null, 257))
                .andReturn(new PagedList<Document>(Arrays.<Document> asList()));
        EasyMock.replay(documentRepository);

        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.add(DB_NAME + ".name");
        final QueryImpl query = new QueryImpl(directory, DB_NAME);

        final QueryCriteria criteria = new CriteriaBuilder().setAlways()
                .build();
        List<SearchDoc> results = query.search(criteria, indexPolicy,
                queryPolicy, true, 0, 10);
        Assert
                .assertEquals("unexpectes results " + results, 10, results
                        .size());

    }

    @Test
    public final void testSpatialSearch() throws Exception {
        Directory directory = LuceneUtils.toFSDirectory(new File("spatial"));
        final IndexPolicy indexPolicy = new IndexPolicy();
        indexPolicy.add("name", false, null, false, false, 0.0F, false, false,
                false);
        indexPolicy.add("lat", false, null, false, false, 0.0F, true, true,
                false);
        indexPolicy.add("lng", false, null, false, false, 0.0F, true, false,
                true);
        indexSpatial(indexPolicy, directory);
        EasyMock.expect(
                documentRepository
                        .getAllDocuments("test_data", null, null, 257))
                .andReturn(new PagedList<Document>(Arrays.<Document> asList()));
        EasyMock.replay(documentRepository);

        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.add(DB_NAME + ".name");
        final QueryImpl query = new QueryImpl(directory, DB_NAME);
        final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                "Restaurant").setLatitude(38.8725000).setLongitude(-77.3829000)
                .setRadius(80).build();

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

        final Map<String, Object> attrs = new TreeMap<String, Object>();

        attrs.put("name", name);
        attrs.put("lat", lat);
        attrs.put("lng", lng);

        final Document doc = new DocumentBuilder(DB_NAME).setId(name).putAll(
                attrs).build();

        final IndexerImpl indexer = new IndexerImpl(directory, DB_NAME);
        return indexer.index(policy, new SimpleDocumentsIterator(doc), null,
                true);
    }
}

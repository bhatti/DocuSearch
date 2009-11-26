package com.plexobject.docusearch.etl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PagedList;

public class TriRelationMergerTest {
    private static final String DB_NAME = "MYDB";
    private static final String TO_DB_NAME = "TO_MYDB";
    private static final String JOIN_DB_NAME = "JOIN_MYDB";
    private static final int LIMIT = Configuration.getInstance().getPageSize();

    private DocumentRepository repository;
    private ConfigurationRepository configRepository;
    private final Document fromDoc1 = new DocumentBuilder(DB_NAME).setId("1")
            .put("tag_id", "microsoft").put("job", "x1").build();
    private final Document fromDoc2 = new DocumentBuilder(DB_NAME).setId("2")
            .put("tag_id", "cisco").put("job", "x2").build();
    private final Document toDoc1 = new DocumentBuilder(TO_DB_NAME).setId("3")
            .put("ticker_id", "msft").put("name", "john").build();
    private final Document toDoc2 = new DocumentBuilder(TO_DB_NAME).setId("4")
            .put("ticker_id", "csco").put("name", "sally").build();
    private final Document joinDoc1 = new DocumentBuilder(TO_DB_NAME)
            .setId("5").put("tag_id", "microsoft").put("ticker_id", "msft")
            .put("rank", "11").build();
    private final Document joinDoc2 = new DocumentBuilder(TO_DB_NAME)
            .setId("6").put("tag_id", "cisco").put("ticker_id", "csco").put(
                    "rank", "12").build();

    private final Document mergedDoc1 = new DocumentBuilder(TO_DB_NAME).setId(
            "3").put("ticker_id", "msft").put("job", "x1").put("rank", "11")
            .put("name", "john").build();
    private final Document mergedDoc2 = new DocumentBuilder(TO_DB_NAME).setId(
            "4").put("ticker_id", "csco").put("job", "x2").put("rank", "12")
            .put("name", "sally").build();

    @Before
    public void setUp() throws Exception {
        repository = EasyMock.createMock(DocumentRepository.class);
        configRepository = EasyMock.createMock(ConfigurationRepository.class);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test(expected = IllegalArgumentException.class)
    public final void testFileConstructor() throws IOException {
        new TriRelationMerger(File.createTempFile("tmp", "tmp"));
    }

    @Test(expected = IllegalArgumentException.class)
    public final void testPropertiesConstructor() throws IOException {
        new TriRelationMerger(new Properties());
    }

    @Test
    public final void testMergeListOfDocument() {
    }

    @Test
    public final void testMergeDocument() {
    }

    @Test(expected = NullPointerException.class)
    public final void testCreateMergerWithNullProperties() {
        new TriRelationMerger(repository, (Properties) null);

    }

    @Test(expected = NullPointerException.class)
    public final void testCreateMergerWithNullFile() throws IOException {
        new TriRelationMerger(repository, (File) null);

    }

    @Test(expected = IllegalArgumentException.class)
    public final void testCreateMergerWithoutProperties() {
        Properties props = new Properties();
        TriRelationMerger merger = new TriRelationMerger(repository, props);
        Assert.assertNotNull(merger);
    }

    @Test(expected = IllegalArgumentException.class)
    public final void testCreateMergerWithoutMergeColumns() {
        Properties props = new Properties();
        props.put("join.database", "join");
        TriRelationMerger merger = new TriRelationMerger(repository, props);
        Assert.assertNotNull(merger);
    }

    public final void testUsage() throws IOException {
        TriRelationMerger.main(new String[0]);
    }

    @SuppressWarnings("serial")
    @Test
    public final void testRun() {
        EasyMock.expect(
                repository.getAllDocuments(JOIN_DB_NAME, null, null, LIMIT))
                .andReturn(PagedList.asList(joinDoc1, joinDoc2));
        EasyMock.expect(
                repository.getAllDocuments(JOIN_DB_NAME, "6", null, LIMIT))
                .andReturn(PagedList.<Document> emptyList());
        EasyMock.expect(repository.getDocument(DB_NAME, "microsoft"))
                .andReturn(fromDoc1); // search by tag_id
        EasyMock.expect(
                repository.query(TO_DB_NAME, new HashMap<String, String>() {
                    {
                        put("ticker_id", "msft");
                    }
                })).andReturn(new HashMap<String, Document>() {
            {
                put("3", toDoc1);
            }
        }); // search by ticker_id
        EasyMock.expect(repository.getDocument(DB_NAME, "cisco")).andReturn(
                fromDoc2); // search by tag_id
        EasyMock.expect(
                repository.query(TO_DB_NAME, new HashMap<String, String>() {
                    {
                        put("ticker_id", "csco");
                    }
                })).andReturn(new HashMap<String, Document>() {
            {
                put("4", toDoc2);
            }
        }); // search by ticker_id
        EasyMock.expect(repository.saveDocument(mergedDoc1, true)).andReturn(
                mergedDoc1);
        EasyMock.expect(repository.saveDocument(mergedDoc2, true)).andReturn(
                mergedDoc2);

        final Properties props = new Properties();
        props.put("from.database", DB_NAME);
        props.put("to.database", TO_DB_NAME);
        props.put("from.id", "tag_id");
        props.put("to.id", "ticker_id");
        props.put("to.relation.name", "tags");
        props.put("from.merge.columns", "job");
        props.put("join.database", JOIN_DB_NAME);
        props.put("join.merge.columns", "rank");
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        final TriRelationMerger merger = new TriRelationMerger(repository,
                props);
        merger.run();
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);

    }
}

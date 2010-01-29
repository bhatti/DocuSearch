package com.plexobject.docusearch.persistence.file;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.persistence.DocumentRepository;

/**
 * @author Shahzad Bhatti
 * 
 */
public class DocumentRepositoryImplTest {
    private static final int NUM_DOCS = 10;
    private static final File TEST_DIR = new File("testdocrepo_delete_me"
            + System.currentTimeMillis());
    private static final String DB_NAME = "x" + System.currentTimeMillis();
    private DocumentRepository repository;

    @Before
    public void setUp() throws Exception {
        repository = new DocumentRepositoryImpl(TEST_DIR);
        FileUtils.deleteDirectory(TEST_DIR);
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(TEST_DIR);
    }

    @Test
    public void testQuery() throws Exception {
        final Map<String, Document> documents = createDocuments();

        Map<String, String> criteria = new TreeMap<String, String>();
        criteria.put("B", "2");

        Map<String, Document> saved = repository.query(DB_NAME, criteria);

        Assert.assertEquals(NUM_DOCS, saved.size());
        for (Document doc : saved.values()) {
            Assert.assertEquals(documents.get(doc.getId()), doc);
        }

    }

    @Test
    public void testQueryNotFound() throws Exception {
        createDocuments();
        Map<String, String> criteria = new TreeMap<String, String>();
        criteria.put("Z", "9");

        Map<String, Document> saved = repository.query(DB_NAME, criteria);

        Assert.assertEquals(0, saved.size());
    }

    @Test(expected = NullPointerException.class)
    public void testNullRestClient() throws Exception {
        repository = new DocumentRepositoryImpl(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateDatabasesWithNullDatabase() throws Exception {
        repository.createDatabase(null);
    }

    @Test
    public void testCreateGetAllDatabases() throws Exception {
        repository.createDatabase(DB_NAME);

        Assert.assertTrue(Arrays.asList(repository.getAllDatabases()).contains(
                DB_NAME));
    }

    @Test
    public void testDuplicateDatabases() throws Exception {
        Assert.assertTrue(repository.createDatabase(DB_NAME));

        Assert.assertFalse(repository.createDatabase(DB_NAME));
    }

    @Test(expected = NullPointerException.class)
    public void testSaveNullDocument() throws Exception {
        repository.saveDocument(null, false);
    }

    @Test
    public void testSaveGetDocument() throws Exception {
        Document original = newDocument(false);
        original = repository.saveDocument(original, false);
        Document saved = repository.getDocument(DB_NAME, original.getId());
        Assert.assertEquals(original, saved);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetDocumentsNullDatabase() throws Exception {
        repository.getDocuments(null, new String[0]);
    }

    @Test(expected = NullPointerException.class)
    public void testGetDocumentsNullIds() throws Exception {
        repository.getDocuments(DB_NAME, (String[]) null);
    }

    @Test
    public void testSaveGetDocuments() throws Exception {
        final Map<String, Document> documents = createDocuments();
        Map<String, Document> saved = repository.getDocuments(DB_NAME,
                documents.keySet());
        Assert.assertEquals(NUM_DOCS, saved.size());
        for (Document doc : saved.values()) {
            Assert.assertEquals(documents.get(doc.getId()), doc);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAllDocumentsByLimitWithNullDatabase() throws Exception {
        repository.getAllDocuments(null, 0, NUM_DOCS * 10);
    }

    @Test
    public void testSaveGetAllDocumentsByLimit() throws Exception {
        final Map<String, Document> documents = createDocuments();
        List<Document> saved = repository.getAllDocuments(DB_NAME, 0,
                NUM_DOCS * 10);
        for (Document doc : saved) {
            final Document next = documents.get(doc.getId());
            if (next != null) {
                Assert.assertEquals(next, doc);
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAllDocumentsByEndKeyWithNullDatabase() throws Exception {
        repository.getAllDocuments(null, "", "", 1);
    }

    @Test
    public void testSaveGetAllDocumentsByEndKey() throws Exception {
        final Map<String, Document> documents = createDocuments();

        String first = null;
        String last = null;
        for (String id : documents.keySet()) {
            if (first == null) {
                first = id;
            }
            last = id;
        }
        List<Document> saved = repository.getAllDocuments(DB_NAME, first, last,
                1);
        Assert.assertEquals(NUM_DOCS, saved.size());
        for (Document doc : saved) {
            final Document next = documents.get(doc.getId());
            Assert.assertEquals(next, doc);
        }
    }

    @Test
    public void testGetInfo() throws Exception {
        Map<String, String> info = repository.getInfo(DB_NAME);
        Assert.assertEquals(DB_NAME, info.get("db_name"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetInfoWithNull() throws Exception {
        repository.getInfo(null);
    }

    @Test
    public void testDeleteDatabases() throws Exception {
        Assert.assertTrue(repository.createDatabase(DB_NAME));

        Assert.assertTrue("failed to delete", repository
                .deleteDatabase(DB_NAME));
    }

    @Test
    public void testDeleteDatabasesAgain() throws Exception {
        Assert.assertTrue(repository.createDatabase(DB_NAME));

        Assert.assertFalse(repository.createDatabase(DB_NAME));

        Assert.assertTrue(repository.deleteDatabase(DB_NAME));

        Assert.assertFalse(repository.deleteDatabase(DB_NAME));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteDatabasesWithNull() throws Exception {
        repository.deleteDatabase(null);
    }

    @SuppressWarnings("unchecked")
    private Document newDocument(final boolean specifyId) {
        final String id = specifyId ? "x" + System.nanoTime() : null;
        final List<? extends Object> arr = Arrays.asList(11, "12", 13, "14");

        final Map<String, String> map = new TreeMap<String, String>();
        map.put("Y", "8");
        map.put("Z", "9");

        final Document doc = new DocumentBuilder(DB_NAME).setId(id).put("A",
                "1").put("B", "2").put("C", map).put("D", arr).build();

        return doc;
    }

    private Map<String, Document> createDocuments() throws IOException {
        Map<String, Document> documents = new TreeMap<String, Document>();
        for (int i = 0; i < NUM_DOCS; i++) {
            final Document original = newDocument(true);
            documents.put(original.getId(), original);
            repository.saveDocument(original, true);
        }
        return documents;
    }
}

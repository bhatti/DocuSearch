package com.plexobject.docusearch.etl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;

import com.plexobject.docusearch.cache.CacheFlusher;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.query.QueryPolicy;

public class DocumentLoaderTest {
    private static final String DB_NAME = "DocumentLoaderTestDB";
    private static Logger LOGGER = Logger.getRootLogger();

    private DocumentRepository repository;
    private ConfigurationRepository configRepository;

    @Before
    public void setUp() throws Exception {
        CacheFlusher.getInstance().flushCaches();
        LOGGER.setLevel(Level.INFO);

        LOGGER.addAppender(new ConsoleAppender(new PatternLayout(
                PatternLayout.TTCC_CONVERSION_PATTERN)));
        repository = EasyMock.createMock(DocumentRepository.class);
        configRepository = EasyMock.createMock(ConfigurationRepository.class);
    }

    @After
    public void tearDown() throws Exception {
        CacheFlusher.getInstance().flushCaches();
    }

    @Test
    public final void testRun() throws IOException {
        EasyMock.expect(repository.createDatabase(DB_NAME)).andReturn(true);

        for (Document doc : newDocuments()) {
            EasyMock.expect(repository.getDocument(DB_NAME, doc.getId()))
                    .andThrow(new RuntimeException("test error"));
            EasyMock.expect(repository.saveDocument(doc, false)).andReturn(doc);
        }

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        final DocumentLoader dataExtractor = newDataLoader();
        dataExtractor.run();

        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
    }

    @Test(expected = NullPointerException.class)
    public final void testHandleNullRow() throws IOException {
        DocumentLoader dataExtractor = newDataLoader();
        dataExtractor.handleRow(0, null);

    }

    @Test
    public final void testHandleRow() throws IOException {
        EasyMock.expect(repository.createDatabase(DB_NAME)).andReturn(true);

        // final IndexPolicy indexPolicy = newIndexPolicy();
        // EasyMock.expect(configRepository.saveIndexPolicy(DB_NAME,
        // indexPolicy))
        // .andReturn(indexPolicy);
        // final QueryPolicy queryPolicy = newQueryPolicy();
        // EasyMock.expect(configRepository.saveQueryPolicy(DB_NAME,
        // queryPolicy))
        // .andReturn(queryPolicy);
        EasyMock.expect(repository.getDocument(DB_NAME, "1")).andThrow(
                new RuntimeException("test error"));

        Document doc = (new DocumentBuilder(DB_NAME).setId("1").put("id", "1")
                .put("name", "john").put("phone", "555-1222").put("email",
                        "john@gmal.com").build());
        EasyMock.expect(repository.saveDocument(doc, false)).andReturn(doc);

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Map<String, String> row = new TreeMap<String, String>();
        row.put("id", "1");
        row.put("name", "john");
        row.put("email", "john@gmail.com");

        DocumentLoader dataExtractor = newDataLoader();
        dataExtractor.handleRow(0, row);
        // Assert.assertEquals(newIndexPolicy(), indexPolicy);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);

    }

    @SuppressWarnings("unused")
    private static QueryPolicy newQueryPolicy() {
        final QueryPolicy policy = new QueryPolicy();
        policy.add("id");
        policy.add("name");
        policy.add("email");
        return policy;
    }

    @SuppressWarnings("unused")
    private static IndexPolicy newIndexPolicy() {
        final IndexPolicy policy = new IndexPolicy();
        policy.add("id");
        policy.add("name");
        policy.add("email");
        return policy;
    }

    private Collection<Document> newDocuments() {
        Collection<Document> docs = new ArrayList<Document>();
        docs.add(new DocumentBuilder(DB_NAME).setId("1").put("id", "1").put(
                "name", "john").put("phone", "555-1222").put("email",
                "john@gmal.com").build());
        docs.add(new DocumentBuilder(DB_NAME).setId("2").put("id", "2").put(
                "name", "sally").put("phone", "555-1223").put("email",
                "sally@gmal.com").build());
        return docs;
    }

    private DocumentLoader newDataLoader() throws IOException {

        File file = File.createTempFile("extract", "dat");
        file.deleteOnExit();
        PrintWriter out = new PrintWriter(new FileWriter(file));
        out.println("id,name,phone,email");
        out.println("1,john,555-1222,john@gmail.com");
        out.println("2,sally,555-1223,sally@gmail.com");
        out.close();
        new XmlBeanFactory(new FileSystemResource(
                "src/main/webapp/WEB-INF/applicationContext.xml"));
        DocumentLoader xtractor = new DocumentLoader(file, ',', DB_NAME, "id",
                "id", "name", "email");

        xtractor.configRepository = configRepository;
        xtractor.documentRepository = repository;
        return xtractor;
    }
}

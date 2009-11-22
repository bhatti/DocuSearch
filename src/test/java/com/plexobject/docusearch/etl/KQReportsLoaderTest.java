package com.plexobject.docusearch.etl;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;

import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.RepositoryFactory;

public class KQReportsLoaderTest {
    private static final String TYPE = "type";

    private static final String MYCONTENTS = "mycontents";

    private static final String CONTENTS = "contents";

    private static final String DB_NAME = "MYDB";

    private DocumentRepository repository;
    private KQReportsLoader loader;

    @Before
    public void setUp() throws Exception {
        repository = EasyMock.createMock(DocumentRepository.class);
        final ConfigurationRepository configRepository = EasyMock
                .createMock(ConfigurationRepository.class);
        final RepositoryFactory repositoryFactory = new RepositoryFactory(
                repository, configRepository);
        loader = new KQReportsLoader(repositoryFactory, new File("."), DB_NAME);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testRun() {
        EasyMock.replay(repository);

        loader.run();

        EasyMock.verify(repository);
    }

    @Test
    public final void testImportFileFile() throws IOException {
        EasyMock.expect(repository.saveDocument(newDocument(10, "10Q")))
                .andReturn(newDocument(10, "10Q"));
        EasyMock.replay(repository);
        File file = File.createTempFile("10Q_10.", "txt");
        FileUtils.writeStringToFile(file, MYCONTENTS);
        Document doc = loader.importFile(file);

        EasyMock.verify(repository);
        Assert.assertEquals(DB_NAME, doc.getDatabase());
        Assert.assertEquals("10", doc.getId());
        Assert.assertEquals("unexpected doc " + doc, MYCONTENTS, doc
                .get(CONTENTS));
    }

    @Test
    public final void testImportDataReaderString() throws IOException {
        EasyMock.expect(repository.saveDocument(newDocument(11, "10K")))
                .andReturn(newDocument(11, "10K"));
        EasyMock.replay(repository);
        final StringReader reader = new StringReader(MYCONTENTS);
        Document doc = loader.importData(reader, "10K_11.txt");

        EasyMock.verify(repository);
        Assert.assertEquals(DB_NAME, doc.getDatabase());
        Assert.assertEquals("11", doc.getId());
        Assert.assertEquals("unexpected doc " + doc, MYCONTENTS, doc
                .get(CONTENTS));
    }

    private Document newDocument(int id, String type) {
        return new DocumentBuilder().setId(String.valueOf(id)).setDatabase(
                DB_NAME).put(CONTENTS, MYCONTENTS).put(TYPE, type).build();
    }
}

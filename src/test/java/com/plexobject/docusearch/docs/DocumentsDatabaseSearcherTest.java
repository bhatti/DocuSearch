package com.plexobject.docusearch.docs;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.docs.impl.DocumentsDatabaseSearcherImpl;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.index.lucene.IndexerImpl;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.SimpleDocumentsIterator;
import com.plexobject.docusearch.query.QueryPolicy;

public class DocumentsDatabaseSearcherTest {
    private static Logger LOGGER = Logger.getRootLogger();
    private static final int MAX_LIMIT = Configuration.getInstance()
            .getPageSize();
    private static final String DB_NAME = "MYDB";

    private DocumentRepository repository;
    private ConfigurationRepository configRepository;
    private DocumentsDatabaseSearcherImpl searcher;

    @Before
    public void setUp() throws Exception {
        LOGGER.setLevel(Level.INFO);

        LOGGER.addAppender(new ConsoleAppender(new PatternLayout(
                PatternLayout.TTCC_CONVERSION_PATTERN)));
        repository = EasyMock.createMock(DocumentRepository.class);
        configRepository = EasyMock.createMock(ConfigurationRepository.class);
        searcher = new DocumentsDatabaseSearcherImpl();
        searcher.setDocumentRepository(repository);
        searcher.setConfigRepository(configRepository);
        index();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testQuery() {
        EasyMock.expect(configRepository.getQueryPolicy(DB_NAME)).andReturn(
                newQueryPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        searcher.query(DB_NAME, "owner", "keywords", true, 0, MAX_LIMIT);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
    }

    @Test
    public final void testQueryFields() {
        EasyMock.expect(configRepository.getQueryPolicy(DB_NAME)).andReturn(
                newQueryPolicy());
        EasyMock.replay(repository);
        EasyMock.replay(configRepository);
        searcher.query(DB_NAME, "owner", "keywords", true, 0, MAX_LIMIT);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
    }

    private int index() throws Exception {
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
        final IndexerImpl indexer = new IndexerImpl(new File(
                LuceneUtils.INDEX_DIR, DB_NAME));
        return indexer.index(policy, new SimpleDocumentsIterator(doc),
                deleteExisting);
    }

    private static QueryPolicy newQueryPolicy() {
        final QueryPolicy policy = new QueryPolicy();
        policy.add("name");
        policy.add("symbol");
        return policy;
    }
}

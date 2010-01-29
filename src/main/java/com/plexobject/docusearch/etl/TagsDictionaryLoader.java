package com.plexobject.docusearch.etl;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.lucene.analyzer.SimilarityHelper;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.DocumentsIterator;
import com.plexobject.docusearch.persistence.PersistenceException;

public class TagsDictionaryLoader implements Runnable {
    private static final String TEST_DB = "test_data";
    private static final String TAG = "tag";
    static final Logger LOGGER = Logger.getLogger(TagsDictionaryLoader.class);
    private static final String TAGS = "the_tags";
    final DocumentRepository repository;

    public TagsDictionaryLoader(final DocumentRepository repository) {
        this.repository = repository;
    }

    @Override
    public void run() {
        try {
            final Timer timer = Metric.newTimer(getClass().getSimpleName()
                    + ".run");
            int total = 0;
            final Iterator<List<Document>> docsIt = new DocumentsIterator(
                    repository, TAGS, Configuration.getInstance().getPageSize());
            while (docsIt.hasNext()) {
                List<Document> docs = docsIt.next();
                total += docs.size();
                timer.lapse("Added " + total + " tags " + TAGS + " into "
                        + TEST_DB);

                for (Document doc : docs) {
                    SimilarityHelper.getInstance().trainSpellChecker(TEST_DB,
                            (String) doc.get(TAG));
                    SimilarityHelper.getInstance().saveTrainingSpellChecker(
                            TEST_DB);
                }
            }
            timer.stop("Added " + total + " tags of " + TAGS + " into "
                    + TEST_DB);
        } catch (PersistenceException e) {
            LOGGER.error("Failed to add tags", e);
        } catch (IOException e) {
            LOGGER.error("Failed to add tags", e);
        }
    }

    public static void main(String[] args) {
        XmlBeanFactory factory = new XmlBeanFactory(new FileSystemResource(
                "src/main/webapp/WEB-INF/applicationContext.xml"));
        final DocumentRepository documentRepository = (DocumentRepository) factory
                .getBean("documentRepository");

        new TagsDictionaryLoader(documentRepository).run();
    }

}

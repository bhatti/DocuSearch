package com.plexobject.docusearch.docs;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.DocumentsIterator;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.plexobject.docusearch.persistence.bdb.DocumentRepositoryBdb;
import com.plexobject.docusearch.persistence.couchdb.DocumentRepositoryCouchdb;

public class RepositoryReplicator {
    private static final Logger LOGGER = Logger
            .getLogger(RepositoryReplicator.class);
    private final DocumentRepository srcRepository;
    private final DocumentRepository dstRepository;

    public RepositoryReplicator(DocumentRepository srcRepository,
            DocumentRepository dstRepository) {
        this.srcRepository = srcRepository;
        this.dstRepository = dstRepository;
    }

    public void copy(final String db) {
        try {
            final Timer timer = Metric.newTimer(getClass().getSimpleName()
                    + ".run");
            int total = 0;
            final Iterator<List<Document>> docsIt = new DocumentsIterator(
                    srcRepository, db, Configuration.getInstance()
                            .getPageSize());
            while (docsIt.hasNext()) {
                List<Document> sourceDocuments = docsIt.next();
                total += sourceDocuments.size();
                for (Document sourceDocument : sourceDocuments) {
                    dstRepository.saveDocument(sourceDocument, true);
                }
            }
            timer.stop("Copied " + total + " records of " + db + " from "
                    + srcRepository + " to " + dstRepository);

        } catch (PersistenceException e) {
            LOGGER.error("Failed to copy " + db, e);
        }
    }

    public static void main(String[] args) {
        for (String db : args) {
            final DocumentRepository srcRepository = new DocumentRepositoryCouchdb();
            final DocumentRepositoryBdb dstRepository = new DocumentRepositoryBdb(
                    new File("search_bdb"));
            LOGGER.info("Copying " + db + " from " + srcRepository.getInfo(db));

            RepositoryReplicator repositoryReplicator = new RepositoryReplicator(
                    srcRepository, dstRepository);
            repositoryReplicator.copy(db);
            LOGGER.info("Copied " + dstRepository + " "
                    + dstRepository.count(db) + " records in " + db);
            try {
                dstRepository.close();
            } catch (Exception e) {
            }
        }
    }
}

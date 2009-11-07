package com.plexobject.docusearch;

import java.util.Collection;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.plexobject.docusearch.docs.DocumentsDatabaseIndexer;
import com.plexobject.docusearch.docs.DocumentsDatabaseSearcher;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.RepositoryFactory;

/**
 * @author Shahzad Bhatti
 * 
 */
public final class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class);
    static final int MAX_LIMIT = Configuration.getInstance().getPageSize();

    public static void main(final String[] args) throws Exception {
        Logger root = Logger.getRootLogger();
        root.setLevel(Level.INFO);
        
        root.addAppender(new ConsoleAppender(new PatternLayout(
                PatternLayout.TTCC_CONVERSION_PATTERN)));
        final RepositoryFactory repositoryFactory = new RepositoryFactory();
        final Timer timer = Metric
        .newTimer("main");
        if (args.length >= 1 && args[0].equals("-index")) {
            new DocumentsDatabaseIndexer(repositoryFactory).indexAllDatabases();
            timer.stop("indexed all databases");

        } else if (args.length >= 1 && args[0].equals("-search")) {
            final String database = args.length > 1 ? args[1] : "database";
            final String keywords = args.length > 2 ? args[2] : "Pope";
            int startKey = 0;
            int i = 0;
            Collection<Document> docs = null;
            final DocumentsDatabaseSearcher searcher = new DocumentsDatabaseSearcher(
                    repositoryFactory);
            while ((docs = searcher.query(database, keywords, true, startKey,
                    MAX_LIMIT)).size() > 0) {
                for (Document doc : docs) {
                    LOGGER.info(i + "th " + doc);
                    i++;
                }
                startKey += docs.size();
            }
            timer.stop("searched " + startKey + " records of " + database);
        }
    }
}

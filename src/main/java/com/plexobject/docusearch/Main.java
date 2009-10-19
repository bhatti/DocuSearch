package com.plexobject.docusearch;

import java.util.Collection;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.plexobject.docusearch.docs.DocumentsDatabaseIndexer;
import com.plexobject.docusearch.docs.DocumentsDatabaseSearcher;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.persistence.RepositoryFactory;

/**
 * @author bhatti@plexobject.com
 * 
 */
public final class Main {
	private static final Logger LOGGER = Logger.getLogger(Main.class);
	public static final int MAX_LIMIT = 2048;

	public static void main(final String[] args) throws Exception {
		Logger root = Logger.getRootLogger();
		root.setLevel(Level.INFO);

		root.addAppender(new ConsoleAppender(new PatternLayout(
				PatternLayout.TTCC_CONVERSION_PATTERN)));
		final RepositoryFactory repositoryFactory = new RepositoryFactory();
		if (args.length >= 1 && args[0].equals("-index")) {
			new DocumentsDatabaseIndexer(repositoryFactory).indexAllDatabases();
		} else if (args.length >= 1 && args[0].equals("-search")) {
			final String database = args.length > 1 ? args[1] : "myindex";
			final String keywords = args.length > 2 ? args[2] : "Pope";
			int startkey = 0;
			int i = 0;
			final long started = System.currentTimeMillis();
			Collection<Document> docs = null;
			final DocumentsDatabaseSearcher searcher = new DocumentsDatabaseSearcher(
					repositoryFactory);
			while ((docs = searcher.query(database, keywords, startkey,
					MAX_LIMIT)).size() > 0) {
				for (Document doc : docs) {
					LOGGER.info(i + "th " + doc);
					i++;
				}
				startkey += docs.size();
			}
			final long elapsed = System.currentTimeMillis() - started;
			LOGGER.info("searched " + startkey + " records of " + database
					+ " in " + elapsed + " milliseconds.");

		}
	}
}

package com.plexobject.docusearch.docs;

import java.io.File;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.index.Indexer;
import com.plexobject.docusearch.index.lucene.IndexerImpl;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.RepositoryFactory;

public class DocumentsDatabaseIndexer {
	private static final Logger LOGGER = Logger
			.getLogger(DocumentsDatabaseIndexer.class);
	private static final int MAX_LIMIT = Configuration.getInstance().getPageSize();

	private final DocumentRepository repository;
	private final ConfigurationRepository configRepository;

	public DocumentsDatabaseIndexer(final RepositoryFactory repositoryFactory) {
		if (repositoryFactory == null) {
			throw new NullPointerException("RepositoryFactory not specified");
		}
		this.repository = repositoryFactory.getDocumentRepository();
		this.configRepository = repositoryFactory.getConfigurationRepository();
	}

	public void indexAllDatabases() {

		final String[] dbs = repository.getAllDatabases();
		indexDatabases(dbs);
	}

	public void indexDatabases(final String[] dbs) {
		for (String db : dbs) {
		 	indexDatabase(db);
		}

	}

	public void indexDatabase(final String db) {
		String startkey = null;
		String endkey = null;
		List<Document> docs = null;
		final File dir = new File(LuceneUtils.INDEX_DIR, db);
		if (!dir.mkdirs() && !dir.exists()) {
			throw new RuntimeException("Failed to create directory " + dir);
		}
		final long started = System.currentTimeMillis();
		final IndexPolicy policy = configRepository.getIndexPolicy(db);
		int total = 0;
		int succeeded = 0;
		int requests = 0;
		while ((docs = repository.getAllDocuments(db, startkey, endkey))
				.size() > 0) {
			requests++;
			total += docs.size();
			startkey = docs.get(docs.size()-1).getId();

			succeeded += indexDocuments(dir, policy, docs);
			if (total > 0 && total % 1000 == 0) {
				final long elapsed = System.currentTimeMillis() - started;
				LOGGER.info("--Indexed " + succeeded + "/" + total + "/"
						+ startkey + " documents in " + elapsed
						+ " milliseconds");
			}

			if (docs.size() < MAX_LIMIT / 10) {
				break;
			}
		}
		if (total > 100) {
		    final long elapsed = System.currentTimeMillis() - started;
		    LOGGER.info("Indexed " + succeeded + "/" + total + " - startkey "
				+ startkey + ", requests " + requests + ", records of " + db
				+ " in " + elapsed + " milliseconds with policy " + policy);
		}
	}

	public int indexDocuments(final File dir, final IndexPolicy policy,
			final Collection<Document> docs) {
		final Indexer indexer = new IndexerImpl(dir);

		return indexer.index(policy, docs);
	}

	public int indexDocuments(String index, final Collection<Document> docs) {
		IndexPolicy policy = configRepository.getIndexPolicy(index);

		final File dir = new File(LuceneUtils.INDEX_DIR, index);
		return indexDocuments(dir, policy, docs);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Logger root = Logger.getRootLogger();
		root.setLevel(Level.INFO);

		root.addAppender(new ConsoleAppender(new PatternLayout(
				PatternLayout.TTCC_CONVERSION_PATTERN)));

		new DocumentsDatabaseIndexer(new RepositoryFactory())
				.indexAllDatabases();
	}
}

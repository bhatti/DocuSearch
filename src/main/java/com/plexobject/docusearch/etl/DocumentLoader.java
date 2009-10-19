package com.plexobject.docusearch.etl;

import java.io.File;
import java.util.Map;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.query.QueryPolicy;

/**
 * @author bhatti@plexobject.com
 * 
 */
public class DocumentLoader extends DelimitedFileParser {
	static Logger LOGGER = Logger.getLogger(DocumentLoader.class);
	private final DocumentRepository repository;
	private final ConfigurationRepository configRepository;

	private final String database;
	private final String idColumn;

	public DocumentLoader(final RepositoryFactory repositoryFactory,
			final File inputFile, final char delimiter, final String database,
			final String idColumn, final String... selectedColumns) {
		super(inputFile, delimiter, selectedColumns);
		this.repository = repositoryFactory.getDocumentRepository();
		this.configRepository = repositoryFactory.getConfigurationRepository();
		this.database = database;
		this.idColumn = idColumn;
	}

	@Override
	protected boolean handleRow(final int rowNum, final Map<String, String> row) {
		if (row == null) {
			throw new NullPointerException("null row " + rowNum);
		}

		if (rowNum == 0) {

			final IndexPolicy indexPolicy = new IndexPolicy();
			for (String field : row.keySet()) {
				indexPolicy.add(field);
			}
			try {
				configRepository.saveIndexPolicy(database, indexPolicy);
			} catch (PersistenceException e) {
				LOGGER.error("Failed to add " + indexPolicy
						+ " with error-code " + e.getErrorCode() + "-" + e);
			}
			final QueryPolicy queryPolicy = new QueryPolicy();
			for (String field : row.keySet()) {
				queryPolicy.add(field);
			}
			try {
				configRepository.saveQueryPolicy(database, queryPolicy);
			} catch (PersistenceException e) {
				LOGGER.error("Failed to add " + queryPolicy
						+ " with error-code " + e.getErrorCode() + "-" + e);
			}
			try {
				repository.createDatabase(database);
			} catch (PersistenceException e) {
			}
		}
		final String id = GenericValidator.isBlankOrNull(idColumn)
				|| "none".equalsIgnoreCase(idColumn) ? null : row.get(idColumn);
		Document doc = new DocumentBuilder(database).setId(id)
				.putAll(row).build();
		try {
			final Document saved = repository.saveDocument(doc);
			if (rowNum % 1000 == 0) {
				if (LOGGER.isInfoEnabled()) {
					LOGGER.info("Adding " + rowNum + "th row " + row + " into "
							+ saved);
				}
			}
		} catch (PersistenceException e) {
			if (e.getErrorCode() == RestClient.CLIENT_ERROR_CONFLICT) {
				final Document oldDoc = repository.getDocument(database, id);
				doc = new DocumentBuilder(oldDoc).setRevision(oldDoc.getRevision()).build();
				try {
					repository.saveDocument(doc);
				} catch (PersistenceException ee) {
					LOGGER.error("Failed to add " + doc + " with error-code "
							+ e.getErrorCode() + "-" + e);
				}
			} else {
				LOGGER.error("Failed to add " + doc + " with error-code "
						+ e.getErrorCode() + "-" + e);
			}
		}
		return true;
	}

	public static void addData(String database, File file, String idColumn,
			String[] columns) {
		DocumentLoader xtractor = new DocumentLoader(new RepositoryFactory(),
				file, '|', database, idColumn, columns);
		xtractor.run();
	}

	private static void usage() {
		System.err
				.println("Usage: <database> <file-name>  <id-column> <columns-separated-by-comma>");
		System.err
				.println("Use none for id-column if there isn't any id column");
		System.exit(1);
	}

	public static void main(String[] args) {
		Logger root = Logger.getRootLogger();
		root.setLevel(Level.INFO);

		root.addAppender(new ConsoleAppender(new PatternLayout(
				PatternLayout.TTCC_CONVERSION_PATTERN)));
		if (args.length != 4) {
			usage();
		}
		try {
			addData(args[0], new File(args[1]), args[2], args[3].split(","));
		} catch (Exception e) {
			LOGGER.error("Failed to import " + args[0] + "/" + args[1], e);
		}
	}
}

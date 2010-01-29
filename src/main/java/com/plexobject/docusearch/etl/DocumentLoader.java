package com.plexobject.docusearch.etl;

import java.io.File;
import java.util.Map;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.plexobject.docusearch.query.QueryPolicy;
import com.sun.jersey.spi.inject.Inject;

/**
 * @author Shahzad Bhatti
 * 
 */
public class DocumentLoader extends DelimitedFileParser {
    private static final boolean CREATE_POLICY = false;

    static Logger LOGGER = Logger.getLogger(DocumentLoader.class);

    @Inject
    @Autowired
    DocumentRepository documentRepository;

    @Inject
    @Autowired
    ConfigurationRepository configRepository;

    private final String database;
    private final String idColumn;

    public DocumentLoader(final File inputFile, final char delimiter,
            final String database, final String idColumn,
            final String... selectedColumns) {
        super(inputFile, delimiter, selectedColumns);
        this.database = database;
        this.idColumn = idColumn;
    }

    @Override
    protected boolean handleRow(final int rowNum, final Map<String, String> row) {
        if (row == null) {
            throw new NullPointerException("null row " + rowNum);
        }

        if (rowNum == 0) {
            if (CREATE_POLICY) {
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
            }
            try {
                documentRepository.createDatabase(database);
            } catch (PersistenceException e) {
            }
        }
        // if (true) {
        // // System.out.println(row.get(idColumn) + "|" + row.get("active"));
        // // System.out.println(row.get(idColumn) + "|" + row.get("province")
        // // + "|" + row.get("country") + "|" + row.get("city")); // +
        // // "|"+
        // // row.get("name"));
        // return true;
        // }
        final String id = GenericValidator.isBlankOrNull(idColumn)
                || "none".equalsIgnoreCase(idColumn) ? null : row.get(idColumn);
        Document oldDoc = null;
        try {
            oldDoc = documentRepository.getDocument(database, id);
        } catch (Exception e) {
            // ignore
        }
        DocumentBuilder docBuilder = new DocumentBuilder(database).setId(id);
        if (oldDoc != null) {
            docBuilder.putAll(oldDoc);
        }
        docBuilder.putAll(row);
        final Document doc = docBuilder.build();

        try {
            final Document saved = documentRepository.saveDocument(doc, false);
            if (rowNum > 0 && rowNum % 1000 == 0) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Adding " + rowNum + "th row " + row + " into "
                            + saved);
                }
            }
        } catch (PersistenceException e) {
            LOGGER.error("Failed to add " + doc + " with error-code "
                    + e.getErrorCode() + "-" + e);
        }
        return true;
    }

    /**
     * @return the documentRepository
     */
    public DocumentRepository getDocumentRepository() {
        return documentRepository;
    }

    /**
     * @param documentRepository
     *            the documentRepository to set
     */
    public void setDocumentRepository(DocumentRepository documentRepository) {
        this.documentRepository = documentRepository;
    }

    /**
     * @return the configRepository
     */
    public ConfigurationRepository getConfigRepository() {
        return configRepository;
    }

    /**
     * @param configRepository
     *            the configRepository to set
     */
    public void setConfigRepository(ConfigurationRepository configRepository) {
        this.configRepository = configRepository;
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
            XmlBeanFactory factory = new XmlBeanFactory(new FileSystemResource(
                    "src/main/webapp/WEB-INF/applicationContext.xml"));
            DocumentLoader xtractor = new DocumentLoader(new File(args[1]),
                    '~', args[0], args[2], args[3].split(","));
            xtractor.configRepository = (ConfigurationRepository) factory
                    .getBean("configRepository");
            xtractor.documentRepository = (DocumentRepository) factory
                    .getBean("documentRepository");

            xtractor.run();

        } catch (Exception e) {
            LOGGER.error("Failed to import " + args[0] + "/" + args[1], e);
        }
    }
}

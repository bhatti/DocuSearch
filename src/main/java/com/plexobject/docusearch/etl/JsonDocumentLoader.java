package com.plexobject.docusearch.etl;

import java.io.File;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;

import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.sun.jersey.spi.inject.Inject;

/**
 * @author Shahzad Bhatti
 * 
 */
public class JsonDocumentLoader extends JsonFileParser {
    static Logger LOGGER = Logger.getLogger(JsonDocumentLoader.class);

    @Inject
    @Autowired
    DocumentRepository documentRepository;

    @Inject
    @Autowired
    ConfigurationRepository configRepository;

    private final String database;
    private final String idColumn;

    public JsonDocumentLoader(final File inputFile, final String database,
            final String idColumn, final String... selectedColumns) {
        super(inputFile, selectedColumns);
        this.database = database;
        this.idColumn = idColumn;
    }

    /**
     * This method is defined by subclass to handle the json
     * 
     * @param rowNum
     *            - row num
     * @param json
     *            - json object
     * @return true - to continue processing the file, false to halt
     */
    @Override
    protected boolean handleJson(int rowNum, JSONObject json) {
        if (json == null) {
            throw new NullPointerException("null json " + rowNum);
        }

        try {
            final String id = GenericValidator.isBlankOrNull(idColumn)
                    || "none".equalsIgnoreCase(idColumn) ? null : json
                    .getString(idColumn);
            Document oldDoc = null;
            try {
                oldDoc = documentRepository.getDocument(database, id);
            } catch (Exception e) {
                // ignore
            }
            DocumentBuilder docBuilder = new DocumentBuilder(database);
            if (oldDoc != null) {
                docBuilder.putAll(oldDoc);
            }
            Document newDoc = Converters.getInstance().getConverter(
                    JSONObject.class, Document.class).convert(json);
            docBuilder.putAll(newDoc).setId(id);
            final Document doc = docBuilder.build();

            final Document saved = documentRepository.saveDocument(doc, false);
            if (rowNum > 0 && rowNum % 1000 == 0) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Adding " + rowNum + "th json " + json
                            + " into " + saved);
                }
            }
        } catch (PersistenceException e) {
            LOGGER.error("Failed to add " + json + " with error-code "
                    + e.getErrorCode() + "-" + e);
        } catch (JSONException e) {
            LOGGER.error("Failed to add " + json + " due to " + e);
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
            JsonDocumentLoader xtractor = new JsonDocumentLoader(new File(
                    args[1]), args[0], args[2], args[3].split(","));
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

package com.plexobject.docusearch.docs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.DocumentsIterator;
import com.sun.jersey.spi.inject.Inject;

@Component("documentPropertiesHelper")
public class DocumentPropertiesHelper {
    private static final Logger LOGGER = Logger
            .getLogger(DocumentPropertiesHelper.class);

    @Autowired
    @Inject
    DocumentRepository documentRepository;

    public DocumentPropertiesHelper() {
    }

    public Properties load(final String resourceName) throws IOException {
        return load(resourceName, true);
    }

    public Properties load(final String resourceName,
            final boolean saveIfNotInRepository) throws IOException {
        final Properties properties = new Properties();
        try {
            loadPropertiesFromRepository(normalizeDatabaseName(resourceName),
                    properties);
        } catch (Exception e) {
            LOGGER.error("failed to load " + resourceName, e);
        }
        if (properties.size() == 0) {
            loadPropertiesFromResource(resourceName, properties);
            try {
                if (saveIfNotInRepository) {
                    saveProperties(normalizeDatabaseName(resourceName),
                            properties);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("saved " + properties.size()
                                + " properties for " + resourceName);
                    }
                }
            } catch (Exception e) {
                LOGGER.error("failed to save " + resourceName, e);
            }
        }
        return properties;
    }

    private void loadPropertiesFromRepository(final String database,
            final Properties properties) {
        if (documentRepository != null) {
            final Iterator<List<Document>> docsIt = new DocumentsIterator(
                    documentRepository, database, Configuration.getInstance()
                            .getPageSize());
            while (docsIt.hasNext()) {
                for (Document doc : docsIt.next()) {
                    properties.put(doc.getId(), doc.get("value"));
                }
            }
        }
    }

    public static void loadPropertiesFromResource(final String resourceName,
            final Properties properties) throws IOException {
        InputStream in = DocumentPropertiesHelper.class.getClassLoader()
                .getResourceAsStream(resourceName);
        if (in == null) {
            throw new FileNotFoundException("Failed to find " + resourceName);
        }
        properties.load(in);
    }

    private void saveProperties(final String database,
            final Properties properties) {
        if (documentRepository != null) {
            documentRepository.createDatabase(database);
            for (String name : properties.stringPropertyNames()) {
                String value = properties.getProperty(name);
                Document doc = new DocumentBuilder(database).setId(name).put(
                        "value", value).build();
                documentRepository.saveDocument(doc, true);
            }
        }
    }

    /**
     * @return the repository
     */
    public DocumentRepository getRepository() {
        return documentRepository;
    }

    /**
     * @param repository
     *            the repository to set
     */
    public void setRepository(DocumentRepository repository) {
        this.documentRepository = repository;
    }

    private static String normalizeDatabaseName(final String resourceName) {
        int dot = resourceName.indexOf(".");
        return dot == -1 ? resourceName : resourceName.substring(0, dot);
    }
}

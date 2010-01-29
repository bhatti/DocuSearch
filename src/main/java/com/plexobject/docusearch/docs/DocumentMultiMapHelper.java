package com.plexobject.docusearch.docs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.MultiHashMap;
import org.apache.commons.collections.MultiMap;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.DocumentsIterator;
import com.sun.jersey.spi.inject.Inject;

@Component("documentMultiMapHelper")
public class DocumentMultiMapHelper {
    private static final Logger LOGGER = Logger
            .getLogger(DocumentPropertiesHelper.class);

    @Autowired
    @Inject
    DocumentRepository documentRepository;

    public DocumentMultiMapHelper() {
    }

    public MultiMap load(final String resourceName, final String keyName,
            final String valueName, final boolean saveIfNotInRepository)
            throws IOException {
        final MultiMap properties = new MultiHashMap();
        try {
            loadPropertiesFromRepository(normalizeDatabaseName(resourceName),
                    properties, keyName, valueName);
        } catch (Exception e) {
            LOGGER.error("failed to load " + resourceName, e);
        }
        if (properties.size() == 0) {
            loadPropertiesFromResource(resourceName, properties);
            try {
                if (saveIfNotInRepository) {
                    saveProperties(normalizeDatabaseName(resourceName),
                            properties, keyName, valueName);
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

    @SuppressWarnings("unchecked")
    private void loadPropertiesFromRepository(final String database,
            final MultiMap properties, final String keyName,
            final String valueName) {
        if (documentRepository != null) {
            final Iterator<List<Document>> docsIt = new DocumentsIterator(
                    documentRepository, database, Configuration.getInstance()
                            .getPageSize());
            while (docsIt.hasNext()) {
                for (Document doc : docsIt.next()) {
                    properties.put(doc.get(keyName), doc.get(valueName));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void loadPropertiesFromResource(final String resourceName,
            final MultiMap properties) throws IOException {
        InputStream in = DocumentPropertiesHelper.class.getClassLoader()
                .getResourceAsStream(resourceName);
        if (in == null) {
            throw new FileNotFoundException("Failed to find " + resourceName);
        }
        List<String> lines = IOUtils.readLines(in);
        for (String line : lines) {
            final String[] t = line.split("=");
            properties.put(t[0].trim(), t[1].trim());
        }
    }

    @SuppressWarnings("unchecked")
    private void saveProperties(final String database,
            final MultiMap properties, final String keyName,
            final String valueName) {
        if (documentRepository != null) {
            documentRepository.createDatabase(database);
            for (Object k : properties.keySet()) {
                final String name = (String) k;
                Collection<String> values = (Collection<String>) properties
                        .get(name);
                for (String value : values) {
                    Document doc = new DocumentBuilder(database).put(keyName,
                            name).put(valueName, value).build();
                    documentRepository.saveDocument(doc, true);
                }
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

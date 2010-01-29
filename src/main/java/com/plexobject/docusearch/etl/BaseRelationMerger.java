package com.plexobject.docusearch.etl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.DocumentsIterator;
import com.plexobject.docusearch.persistence.PersistenceException;

/**
 * @author Shahzad Bhatti
 * 
 *         Base class for adding relationships to the documents.
 * 
 */
public abstract class BaseRelationMerger implements Runnable {
    protected static enum RelationType {
        ATOM, HASH, ARRAY
    }

    static final int MAX_LIMIT = Configuration.getInstance().getPageSize();
    Logger logger = Logger.getLogger(getClass());
    final Map<String, Boolean> seenToDocIds = new TreeMap<String, Boolean>();

    final DocumentRepository repository;
    final String fromDatabase;
    final String toDatabase;
    final String fromId;
    final String toId;
    final String toRelationName;
    final String[] fromColumnsToMerge;
    final RelationType relationType;
    int docCount;

    public BaseRelationMerger(final DocumentRepository repository,
            final File configFile) throws IOException {
        this(repository, loadProperties(configFile));

    }

    public BaseRelationMerger(final DocumentRepository repository,
            final Properties props) {
        if (repository == null) {
            throw new NullPointerException("DocumentRepository not specified");
        }
        if (props == null) {
            throw new NullPointerException("Properties not specified");
        }
        this.repository = repository;
        fromDatabase = props.getProperty("from.database");
        if (GenericValidator.isBlankOrNull(fromDatabase)) {
            throw new IllegalArgumentException("fromDatabase not specified");
        }
        toDatabase = props.getProperty("to.database");
        if (GenericValidator.isBlankOrNull(toDatabase)) {
            throw new IllegalArgumentException("toDatabase not specified");
        }
        fromId = props.getProperty("from.id");
        if (GenericValidator.isBlankOrNull(fromId)) {
            throw new IllegalArgumentException("fromId not specified");
        }
        toId = props.getProperty("to.id");
        if (GenericValidator.isBlankOrNull(toId)) {
            throw new IllegalArgumentException("toId not specified");
        }
        toRelationName = props.getProperty("to.relation.name");
        if (GenericValidator.isBlankOrNull(toRelationName)) {
            throw new IllegalArgumentException("toRelationName not specified");
        }
        fromColumnsToMerge = props.getProperty("from.merge.columns", "").split(
                ",");
        relationType = RelationType.valueOf(props.getProperty("relation.type",
                String.valueOf(RelationType.ARRAY)).toUpperCase());

        if ((fromColumnsToMerge == null || fromColumnsToMerge.length == 0)) {
            throw new IllegalArgumentException(
                    "fromColumnsToMerge and joinColumnsToMerge not specified");
        }
    }

    public void merge(final List<Document> sourceDocuments) {
        final Timer timer = Metric.newTimer(getClass().getSimpleName()
                + ".merge");
        try {
            for (Document sourceDocument : sourceDocuments) {
                try {
                    merge(sourceDocument);
                } catch (Exception e) {
                    logger.error("Failed to merge " + sourceDocument, e);
                }
            }
        } finally {
            timer.stop();
        }
    }

    public void merge(final Document sourceDocument) {
        final String fromIdValue = sourceDocument.getProperty(fromId);

        if (GenericValidator.isBlankOrNull(fromIdValue)) {
            throw new IllegalArgumentException("fromId " + fromId
                    + " not found in source " + sourceDocument);
        }
        final String toIdValue = sourceDocument.getProperty(toId);
        if (GenericValidator.isBlankOrNull(toIdValue)) {
            throw new IllegalArgumentException("toId " + toId
                    + " not found in " + sourceDocument);
        }

        try {
            final Collection<Document> fromDocuments = getFromDocuments(
                    sourceDocument, fromIdValue);
            final Collection<Document> toDocuments = getToDocuments(
                    sourceDocument, toIdValue);

            for (Document fromDocument : fromDocuments) {
                for (Document toDocument : toDocuments) {
                    final DocumentBuilder docBuilder = new DocumentBuilder(
                            toDocument);

                    final Map<String, String> newRelation = new TreeMap<String, String>();
                    mergeAttributes(sourceDocument, fromDocument, newRelation);
                    //
                    if (relationType == RelationType.ARRAY) {
                        docBuilder.put(toRelationName, extractAsArray(
                                toDocument, newRelation));
                    } else if (relationType == RelationType.HASH) {
                        docBuilder.put(toRelationName, newRelation);
                    } else if (relationType == RelationType.ATOM) {
                        docBuilder.put(toRelationName, extractAsAtom(
                                toDocument, newRelation));
                    }
                    if (docCount > 0 && docCount % 1000 == 0
                            && logger.isInfoEnabled()) {
                        logger.info(docCount + ": Saving relation "
                                + newRelation + " from join " + fromDocument
                                + " into " + toDocument + " resulting in  "
                                + docBuilder.build());
                    }
                    repository.saveDocument(docBuilder.build(), true);

                }
            }
        } catch (PersistenceException e) {
            logger.error("Failed to merge " + sourceDocument + " due to " + e);
        }
        docCount++;
    }

    private String extractAsAtom(Document toDocument,
            final Map<String, String> newRelation) {
        for (Map.Entry<String, String> e : newRelation.entrySet()) {
            return e.getValue();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Collection<Map<String, String>> extractAsArray(Document toDocument,
            final Map<String, String> newRelation) {
        Collection<Map<String, String>> values = null;

        if (seenToDocIds.get(toDocument.getId()) != null
                && toDocument.get(toRelationName) instanceof Collection) {
            values = (Collection<Map<String, String>>) toDocument
                    .get(toRelationName);
        }
        seenToDocIds.put(toDocument.getId(), Boolean.TRUE);

        if (values == null) {
            values = new HashSet<Map<String, String>>();
        }

        values.add(newRelation);
        return values;
    }

    /**
     * This method will read all documents from the source repository and will
     * call merge method to combine attributes.
     */
    @Override
    public void run() {
        try {
            final Timer timer = Metric.newTimer(getClass().getSimpleName()
                    + ".run");
            int total = 0;
            final Iterator<List<Document>> docsIt = new DocumentsIterator(
                    repository, getSourceDatabase(), Configuration
                            .getInstance().getPageSize());
            while (docsIt.hasNext()) {
                List<Document> sourceDocuments = docsIt.next();
                total += sourceDocuments.size();
                merge(sourceDocuments);
            }
            timer
                    .stop("Merged " + total + " records of "
                            + getSourceDatabase());
        } catch (PersistenceException e) {
            logger.error("Failed to merge with error-code " + e.getErrorCode(),
                    e);
        }
    }

    /**
     * This method returns the name of source table that will be read
     * iteratively
     * 
     * @return
     */
    protected abstract String getSourceDatabase();

    protected abstract Collection<Document> getFromDocuments(
            final Document sourceDocument, final String fromIdValue);

    protected Collection<Document> getToDocuments(
            final Document sourceDocument, final String toIdValue) {
        try {
            // TODO fix
            if (false) {
                throw new PersistenceException("");
            }
            return Arrays.asList(repository.getDocument(toDatabase, toIdValue));
        } catch (PersistenceException e) {
            final Map<String, String> criteria = new TreeMap<String, String>();
            criteria.put(toId, toIdValue);
            final Map<String, Document> docs = repository.query(toDatabase,
                    criteria);
            if (docs.size() == 0) {
                throw new PersistenceException("faield to find " + fromId
                        + " with " + toIdValue);
            }
            return docs.values();
        }
    }

    protected void mergeAttributes(final Document sourceDocument,
            final Document fromDocument, final Map<String, String> newRelation) {
        for (String columnToMerge : fromColumnsToMerge) {
            if (Document.isValidAttributeKey(columnToMerge)) {
                String value = fromDocument.getProperty(columnToMerge);
                if (value == null) {
                    throw new IllegalArgumentException("Failed to find "
                            + columnToMerge + " in " + fromDocument);
                }
                newRelation.put(columnToMerge, value);
            }
        }
    }

    protected static Properties loadProperties(final File configFile)
            throws IOException {
        if (configFile == null) {
            throw new NullPointerException("configFile not specified");
        }

        final Properties props = new Properties();
        final InputStream in = new FileInputStream(configFile);
        try {
            props.load(in);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
            }
        }

        return props;
    }

}

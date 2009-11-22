package com.plexobject.docusearch.etl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.couchdb.DocumentRepositoryCouchdb;

/**
 * @author Shahzad Bhatti
 * 
 *         This allows copying one or more attributes from repository (table) to
 *         another repository using a join repository.
 * 
 */
public class TriRelationMerger extends BaseRelationMerger {
    final String joinDatabase;
    final String[] joinColumnsToMerge;

    public TriRelationMerger(final File configFile) throws IOException {
        this(new DocumentRepositoryCouchdb(), loadProperties(configFile));
    }

    public TriRelationMerger(final DocumentRepository repository,
            final File configFile) throws IOException {
        this(repository, loadProperties(configFile));

    }

    public TriRelationMerger(final Properties props) {
        this(new DocumentRepositoryCouchdb(), props);
    }

    public TriRelationMerger(final DocumentRepository repository,
            final Properties props) {
        super(repository, props);
        joinDatabase = props.getProperty("join.database");
        if (GenericValidator.isBlankOrNull(joinDatabase)) {
            throw new IllegalArgumentException("joinDatabase not specified");
        }
        final String mergeColumns = props.getProperty("join.merge.columns");
        joinColumnsToMerge = mergeColumns != null ? mergeColumns.split(",")
                : new String[0];
        if ((fromColumnsToMerge == null || fromColumnsToMerge.length == 0)
                && (joinColumnsToMerge == null || joinColumnsToMerge.length == 0)) {
            throw new IllegalArgumentException(
                    "fromColumnsToMerge and joinColumnsToMerge not specified");
        }
    }

    @Override
    protected String getSourceDatabase() {
        return joinDatabase;
    }

    @Override
    protected Collection<Document> getFromDocuments(
            final Document sourceDocument, final String fromIdValue) {
        return Arrays.asList(repository.getDocument(fromDatabase, fromIdValue));
    }

    @Override
    protected void mergeAttributes(final Document sourceDocument,
            final Document fromDocument, final Map<String, String> newRelation) {
        super.mergeAttributes(sourceDocument, fromDocument, newRelation);
        for (String columnToMerge : joinColumnsToMerge) {
            String value = sourceDocument.getProperty(columnToMerge);
            if (value == null) {
                throw new IllegalArgumentException("Failed to find '"
                        + columnToMerge + "' in " + sourceDocument);
            }
            newRelation.put(columnToMerge, value);
        }
    }

    private static void usage() {
        System.err.println("Usage: <config-file-name>");
        System.err
                .println("   e.g. mvn exec:java -Dexec.mainClass=\"com.plexobject.docusearch.etl.TriRelationMerger\" -Dexec.args=\"data/merge_test_datum_tags.properties\"");
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {
        Logger root = Logger.getRootLogger();
        root.setLevel(Level.INFO);

        root.addAppender(new ConsoleAppender(new PatternLayout(
                PatternLayout.TTCC_CONVERSION_PATTERN)));
        if (args.length != 1) {
            usage();
        }
        new TriRelationMerger(new File(args[0])).run();
    }
}

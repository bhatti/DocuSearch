package com.plexobject.docusearch.etl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.persistence.DocumentRepository;

/**
 * @author Shahzad Bhatti
 * 
 *         This allows copying one or more attributes from one repository
 *         (table) to another repository.
 * 
 */
public class BiRelationMerger extends BaseRelationMerger {

    public BiRelationMerger(final File configFile) throws IOException {
        super(configFile);

    }

    public BiRelationMerger(final DocumentRepository repository,
            final File configFile) throws IOException {
        super(repository, configFile);

    }

    public BiRelationMerger(final Properties props) {
        super(props);
    }

    public BiRelationMerger(final DocumentRepository repository,
            final Properties props) {
        super(repository, props);
    }

    @Override
    protected String getSourceDatabase() {
        return fromDatabase;
    }

    @Override
    protected Collection<Document> getFromDocuments(
            final Document sourceDocument, final String fromIdValue) {
        return Arrays.asList(sourceDocument);
    }

    private static void usage() {
        System.err.println("Usage: <config-file-name>");
    }

    public static void main(String[] args) throws IOException {
        Logger root = Logger.getRootLogger();
        root.setLevel(Level.INFO);

        root.addAppender(new ConsoleAppender(new PatternLayout(
                PatternLayout.TTCC_CONVERSION_PATTERN)));

        if (args.length != 1) {
            usage();
            return;
        }

        new BiRelationMerger(new File(args[0])).run();
        // new BiRelationMerger(new
        // File("data/merge_ticker_tags.properties")).run();
    }
}

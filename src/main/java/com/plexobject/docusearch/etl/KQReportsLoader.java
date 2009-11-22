package com.plexobject.docusearch.etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.util.SentenceIterator;

public class KQReportsLoader implements Runnable {
    private static final int MIN_PARAGRAPH_LENGTH = 512;
    private static final int MIN_SENTENCE_LENGTH = 128;

    enum Type {
        K10("10K"), Q10("10Q");
        private String name;

        private Type(String name) {
            this.name = name;
        }

        String getName() {
            return name;
        }

        static Type valueFromName(String name) {
            if (K10.name.equals(name)) {
                return K10;
            } else if (Q10.name.equals(name)) {
                return Q10;
            } else {
                throw new IllegalArgumentException("Invalid name " + name);
            }
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static final String FIRST_SENTENCE = "first_sentence";
    private static final String FIRST_PARAGRAPH = "first_paragraph";
    private static final String REST_OF_CONTENTS = "rest_contents";
    private static final String TYPE = "type";

    static Logger LOGGER = Logger.getLogger(KQReportsLoader.class);

    private final DocumentRepository repository;
    private final File dir;
    private final String database;

    public KQReportsLoader(final File dir, final String database) {
        this(new RepositoryFactory(), dir, database);
    }

    public KQReportsLoader(final RepositoryFactory repositoryFactory,
            final File dir, final String database) {
        if (repositoryFactory == null) {
            throw new NullPointerException("repositoryFactory not specified");
        }
        if (dir == null) {
            throw new NullPointerException("dir not specified");
        }
        if (GenericValidator.isBlankOrNull(database)) {
            throw new IllegalArgumentException("database not specified");
        }

        this.repository = repositoryFactory.getDocumentRepository();
        this.dir = dir;
        this.database = database;
    }

    public void run() {
        final Timer timer = Metric.newTimer("KQReportsLoader.run");
        int count = 0;
        for (File file : get10KQFiles()) {
            try {
                final Document doc = importFile(file);
                if (count % 1000 == 0) {
                    timer.lapse("Saved " + file + " into " + doc.getId());
                }
            } catch (Exception e) {
                LOGGER.error("Failed to import " + file, e);
            }
            count++;
        }
        timer.stop();
    }

    Document importFile(final File file) throws IOException {
        Reader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            return importData(reader, file.getName());
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                LOGGER.error("Failed to close " + file, e);
            }
        }

    }

    Document importData(final Reader reader, final String name)
            throws IOException {

        final int start = name.indexOf("_");
        if (start == -1) {
            throw new IllegalArgumentException("file " + name
                    + " does not include id");
        }
        final int end = name.indexOf(".");
        if (end == -1) {
            throw new IllegalArgumentException("file " + name
                    + " does not include id");
        }

        if (end < start) {
            throw new IllegalArgumentException("Invalid id in " + name);
        }

        final String id = name.substring(start + 1, end);
        final Type type = name.contains("K") ? Type.K10 : Type.Q10;
        final String allContents = IOUtils.toString(reader);
        final SentenceIterator paragraphIterator = new SentenceIterator(
                allContents);

        final StringBuilder firstParagraph = new StringBuilder();
        final StringBuilder firstSentence = new StringBuilder();

        while (firstParagraph.length() < MIN_PARAGRAPH_LENGTH) {
            if (firstSentence.length() < MIN_SENTENCE_LENGTH) {
                firstSentence.append(paragraphIterator.next() + "\n");
            }
            firstParagraph.append(paragraphIterator.next() + "\n");
        }

        final StringBuilder allButFirstParagraph = new StringBuilder();
        while (paragraphIterator.hasNext()) {
            allButFirstParagraph.append(paragraphIterator.next());
        }

        Document doc = new DocumentBuilder(database).setId(id).put(TYPE,
                type.getName()).put(FIRST_SENTENCE, firstSentence.toString())
                .put(FIRST_PARAGRAPH, firstParagraph.toString()).put(
                        REST_OF_CONTENTS, allButFirstParagraph.toString())
                .build();
        try {
            return repository.saveDocument(doc);
        } catch (PersistenceException e) {
            if (e.getErrorCode() == RestClient.CLIENT_ERROR_CONFLICT) {
                final Document oldDoc = repository.getDocument(database, id);
                doc = new DocumentBuilder(oldDoc).setRevision(
                        oldDoc.getRevision()).build();
                return repository.saveDocument(doc);
            } else {
                throw e;
            }
        }
    }

    File[] get10KQFiles() {
        return dir.listFiles(new FileFilter() {
            public boolean accept(final File file) {
                return file.getName().startsWith("10")
                        && file.getName().endsWith(".txt");
            }
        });
    }

    private static void usage() {
        System.err
                .println("Usage: <dir-containing-10KQ-files> <database-name>");
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {
        Logger root = Logger.getRootLogger();
        root.setLevel(Level.INFO);

        root.addAppender(new ConsoleAppender(new PatternLayout(
                PatternLayout.TTCC_CONVERSION_PATTERN)));

        if (args.length != 2) {
            usage();
        }

        new KQReportsLoader(new File(args[0]), args[1]).run();
    }
}

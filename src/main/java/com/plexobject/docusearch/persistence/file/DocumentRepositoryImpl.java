package com.plexobject.docusearch.persistence.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject; //import org.springframework.stereotype.Component;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.cache.CachedMap;
import com.plexobject.docusearch.converter.ConversionException;
import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.NotFoundException;
import com.plexobject.docusearch.persistence.PagedList;
import com.plexobject.docusearch.persistence.PersistenceException;

//@Component("xdocumentRepository")
public class DocumentRepositoryImpl implements DocumentRepository {
    private static final long INDEFINITE = 0;

    private static final Logger LOGGER = Logger
            .getLogger(DocumentRepositoryImpl.class);

    public static final String DB_DIR = Configuration.getInstance()
            .getProperty("docrepo.dir", "docdep");

    final Map<String, Document> cachedDocs = new CachedMap<String, Document>(
            INDEFINITE, 1024, null, null);

    private final File dir;

    /*
     * * document repository implementation
     */
    public DocumentRepositoryImpl() {
        this(new File(DB_DIR));
    }

    public DocumentRepositoryImpl(final File dir) {
        this.dir = dir;
        this.dir.mkdirs();
    }

    /**
     * @return - names of all databases
     */
    @Override
    public String[] getAllDatabases() throws PersistenceException {
        final Timer timer = Metric
                .newTimer("DocumentRepositoryImpl.getAllDatabases");
        try {
            String[] dbs = dir.list();
            return dbs == null ? new String[0] : dbs;
        } finally {
            timer.stop();
        }
    }

    /**
     * This method creates a new database on the document database server.
     * 
     * @param database
     *            - name of the database
     * @return - true if created the database successfully
     * @throws PersistenceException
     *             is thrown when error occurs while creating the database.
     */
    @Override
    public boolean createDatabase(final String database)
            throws PersistenceException {
        if (GenericValidator.isBlankOrNull(database)) {
            throw new IllegalArgumentException("database not specified");
        }

        final Timer timer = Metric
                .newTimer("DocumentRepositoryImpl.createDatabase");
        try {
            File base = new File(dir, database);
            if (base.exists()) {
                return false;
            } else {
                return base.mkdirs();
            }
        } finally {
            timer.stop();
        }
    }

    /**
     * This method deletes an existng database on the document database server.
     * 
     * @param database
     *            - name of the database
     * @return - true if deleted the database successfully
     * @throws PersistenceException
     *             is thrown when error occurs while deleting the database.
     */
    @Override
    public boolean deleteDatabase(final String database)
            throws PersistenceException {
        if (GenericValidator.isBlankOrNull(database)) {
            throw new IllegalArgumentException("database not specified");
        }

        final Timer timer = Metric
                .newTimer("DocumentRepositoryImpl.deleteDatabase");
        try {
            File base = new File(dir, database);
            if (base.exists()) {
                FileUtils.deleteDirectory(base);
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            throw new PersistenceException("Failed to delete " + database, e);
        } finally {
            timer.stop();
        }
    }

    /**
     * @param document
     *            - document containing database, id and attributes
     * @param overwrite
     *            - overwrites in case of revision mismatch
     * @return - saved document
     * @throws PersistenceException
     *             is thrown when error occurs while saving the database.
     */
    @Override
    public Document saveDocument(Document document, final boolean overwrite)
            throws PersistenceException {
        if (null == document) {
            throw new NullPointerException("document not specified");
        }
        final Timer timer = Metric
                .newTimer("DocumentRepositoryImpl.saveDocument");

        try {
            final File base = new File(dir, document.getDatabase());
            String id = document.getId();
            if (id == null) {
                id = UUID.randomUUID().toString();
                document = new DocumentBuilder(document).setId(id).build();
            }
            final File file = new File(base, id);
            write(file, document);
            return document;
        } catch (IOException e) {
            throw new PersistenceException("Failed to save " + document, e);
        } finally {
            timer.stop();
        }
    }

    /**
     * @param database
     *            - name of the database
     * @param startKey
     *            - starting sequence number
     * @param limit
     *            - maximum number of documents to return
     * @return - list of documents
     * @throws PersistenceException
     *             is thrown when error occurs while reading the database.
     */
    @Override
    public List<Document> getAllDocuments(final String database,
            final long startKey, int limit) throws PersistenceException {
        if (GenericValidator.isBlankOrNull(database)) {
            throw new IllegalArgumentException("database not specified");
        }

        return getAllDocuments(database);
    }

    /**
     * @param database
     *            - name of the database
     * @param startKey
     *            - starting sequence number
     * @param endKey
     *            - ending sequence number
     * @param limit
     *            - maximum number of documents to return
     * @return - list of documents
     * @throws PersistenceException
     *             is thrown when error occurs while reading the database.
     */
    @Override
    public PagedList<Document> getAllDocuments(final String database,
            final String startKey, final String endKey, final int max)
            throws PersistenceException {
        return getAllDocuments(database);
    }

    private PagedList<Document> getAllDocuments(final String database)
            throws PersistenceException {
        if (GenericValidator.isBlankOrNull(database)) {
            throw new IllegalArgumentException("database not specified");
        }
        List<Document> docs = new ArrayList<Document>();
        final Timer timer = Metric
                .newTimer("DocumentRepositoryImpl.getAllDocuments");

        try {
            File base = new File(dir, database);
            if (base.exists()) {
                File[] files = base.listFiles();
                for (File file : files) {
                    final Document doc = fetch(file);
                    docs.add(doc);
                }
            }
        } catch (IOException e) {
            throw new PersistenceException("Failed to get documents for  "
                    + database, e);
        } catch (JSONException e) {
            throw new ConversionException("failed to convert json", e);
        } finally {
            timer.stop();
        }

        return new PagedList<Document>(docs, null, null, docs.size(), false);
    }

    /**
     * @param database
     *            - name of the database
     * @return - map of document-id and document
     * @throws PersistenceException
     *             is thrown when error occurs creating the database.
     */
    @Override
    public Map<String, Document> getDocuments(final String database,
            final String... ids) throws PersistenceException {
        return getDocuments(database, Arrays.asList(ids));
    }

    /**
     * @param database
     *            - name of the database
     * @return - documents matching ids
     * @throws PersistenceException
     *             is thrown if error occurs reading the database.
     */
    public Map<String, Document> getDocuments(final String database,
            final Collection<String> ids) {

        if (GenericValidator.isBlankOrNull(database)) {
            throw new IllegalArgumentException("database not specified");
        }
        if (null == ids) {
            throw new NullPointerException("ids not specified");
        }
        final Timer timer = Metric
                .newTimer("DocumentRepositoryImpl.getDocuments");

        try {
            File base = new File(dir, database);
            Map<String, Document> docsById = new TreeMap<String, Document>();
            if (base.exists()) {
                File[] files = base.listFiles(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        return ids.contains(name);
                    }
                });
                for (File file : files) {
                    try {
                        Document doc = fetch(file);
                        docsById.put(doc.getId(), doc);
                    } catch (IOException e) {
                        LOGGER.error("Failed to get documents for " + file, e);
                    }
                }
            }
            return docsById;
        } catch (JSONException e) {
            throw new ConversionException("failed to convert json", e);
        } finally {
            timer.stop();
        }
    }

    /**
     * @param database
     *            - name of the database
     * @param id
     *            - unique identifier for the document
     * @return - document matching the database and id
     * @throws PersistenceException
     *             is thrown when error occurs creating the database.
     * @throws NotFoundException
     *             is thrown if document doesn't exist.
     */
    @Override
    public Document getDocument(final String database, final String id)
            throws PersistenceException {
        if (GenericValidator.isBlankOrNull(database)) {
            throw new IllegalArgumentException("database not specified");
        }
        if (GenericValidator.isBlankOrNull(id)) {
            throw new IllegalArgumentException("id not specified for database "
                    + database);
        }

        try {
            File base = new File(dir, database);
            if (base.exists()) {
                File file = new File(base, id);
                return fetch(file);
            }
            throw new FileNotFoundException(id);
        } catch (IOException e) {
            throw new PersistenceException("Failed to get " + id + " from "
                    + database, e);
        } catch (JSONException e) {
            throw new ConversionException("failed to convert json", e);
        }
    }

    @Override
    public boolean deleteDocument(final String database, final String id,
            final String version) throws PersistenceException {
        if (GenericValidator.isBlankOrNull(database)) {
            throw new IllegalArgumentException("database not specified");
        }
        if (GenericValidator.isBlankOrNull(id)) {
            throw new IllegalArgumentException("id not specified");
        }
        if (GenericValidator.isBlankOrNull(version)) {
            throw new IllegalArgumentException("version not specified");
        }

        final Timer timer = Metric
                .newTimer("DocumentRepositoryImpl.deleteDocument");
        try {
            File base = new File(dir, database);
            if (base.exists()) {
                File file = new File(base, id);
                return file.delete();
            }
            return false;
        } finally {
            timer.stop();
        }
    }

    /**
     * @param database
     *            - name of the database
     * @return - true if created the database successfully
     * @throws PersistenceException
     *             is thrown when error occurs creating the database.
     */
    @Override
    public Map<String, String> getInfo(final String database)
            throws PersistenceException {
        if (GenericValidator.isBlankOrNull(database)) {
            throw new IllegalArgumentException("database not specified");
        }

        Map<String, String> info = new TreeMap<String, String>();
        info.put("db_name", database);
        File base = new File(dir, database);
        int size = base.list() == null ? 0 : base.list().length;
        info.put("size", String.valueOf(size));

        return info;
    }

    /**
     * @param database
     *            - name of the database
     * @param criteria
     *            - name/value criteria joined by AND
     * @return - documents matching ids
     * @throws PersistenceException
     *             is thrown if error occurs reading the database.
     */
    public Map<String, Document> query(final String dbName,
            final Map<String, String> criteria) {
        if (GenericValidator.isBlankOrNull(dbName)) {
            throw new IllegalArgumentException("database not specified");
        }
        if (null == criteria || criteria.size() == 0) {
            throw new NullPointerException("criteria not specified");
        }
        final Timer timer = Metric.newTimer("DocumentRepositoryImpl.query");

        try {
            Collection<Document> docs = getAllDocuments(dbName);
            Map<String, Document> docsById = new TreeMap<String, Document>();
            for (Document doc : docs) {
                boolean matched = true;
                for (Map.Entry<String, String> e : criteria.entrySet()) {
                    if (doc.get(e.getKey()) != null) {
                        String value = doc.get(e.getKey()).toString();
                        if (!e.getValue().equals(value)) {
                            matched = false;
                            break;
                        }
                    } else {
                        matched = false;
                        break;
                    }
                }
                if (matched) {
                    docsById.put(doc.getId(), doc);
                }
            }
            return docsById;
        } finally {
            timer.stop();
        }

    }

    private String toKey(File file) {
        File parent = file.getParentFile();
        return parent.getName() + ":" + file.getName();
    }

    private synchronized void write(final File file, final Document doc)
            throws IOException {
        // cachedDocs.put(toKey(file), doc);
        final String body = Converters.getInstance().getConverter(
                Document.class, JSONObject.class).convert(doc).toString();
        FileUtils.writeStringToFile(file, body);
    }

    private synchronized Document fetch(final File file) throws IOException,
            JSONException {
        Document doc = null;
        final String key = toKey(file);
        synchronized (cachedDocs) {
            doc = cachedDocs.get(key);
            if (doc == null) {
                doc = read(file);
                cachedDocs.put(key, doc);
            }
        }
        return doc;
    }

    private Document read(final File file) throws IOException, JSONException {
        final String body = FileUtils.readFileToString(file);
        final JSONObject jsonDoc = new JSONObject(body);
        return Converters.getInstance().getConverter(JSONObject.class,
                Document.class).convert(jsonDoc);
    }
}

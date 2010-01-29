package com.plexobject.docusearch.persistence.bdb;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.NotFoundException;
import com.plexobject.docusearch.persistence.PagedList;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.evolve.IncompatibleClassException;

/**
 * @author Shahzad Bhatti
 */
public class DocumentRepositoryBdb implements DocumentRepository {
    private static final int MAX_MAX_LIMIT = 1024;

    private static final Logger LOGGER = Logger
            .getLogger(DocumentRepositoryBdb.class);
    static final int DEFAULT_LIMIT = 256;
    static final String DB_DIR = Configuration.getInstance().getProperty(
            "search_bdb", "search_bdb");

    private final File databaseDir;
    private Environment env;
    private StoreConfig storeConfig;

    private Map<String, EntityStore> stores = Collections
            .synchronizedMap(new TreeMap<String, EntityStore>());
    private Map<String, PrimaryIndex<String, JsonDocument>> indexes = Collections
            .synchronizedMap(new TreeMap<String, PrimaryIndex<String, JsonDocument>>());
    private Map<String, Database> databases = Collections
            .synchronizedMap(new HashMap<String, Database>());

    public DocumentRepositoryBdb() {
        this(DB_DIR);
    }

    public DocumentRepositoryBdb(final String databaseDir) {
        this(new File(databaseDir));
    }

    public DocumentRepositoryBdb(final File databaseDir) {
        if (databaseDir == null) {
            throw new NullPointerException("databaseDir is not specified");
        }
        this.databaseDir = databaseDir;
        open();
    }

    /**
     * @return - names of all databases
     */
    @Override
    public String[] getAllDatabases() throws PersistenceException {
        final Timer timer = Metric
                .newTimer("DocumentRepositoryBdb.getAllDatabases");
        try {
            List<String> dbList = null;
            synchronized (env) {
                dbList = env.getDatabaseNames();
            }
            String[] dbNames = new String[dbList.size()];
            for (int i = 0; i < dbList.size(); i++) {
                dbNames[i] = dbList.get(i);
            }
            return dbNames;
        } catch (EnvironmentLockedException e) {
            throw new PersistenceException(e);
        } catch (DatabaseException e) {
            throw new PersistenceException(e);
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
    public boolean createDatabase(String dbname) throws PersistenceException {
        if (GenericValidator.isBlankOrNull(dbname)) {
            throw new IllegalArgumentException("database name not specified");
        }
        final Timer timer = Metric
                .newTimer("DocumentRepositoryBdb.createDatabase");
        try {
            return getDatabase(dbname) != null;
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
    public boolean deleteDatabase(String dbname) throws PersistenceException {
        if (GenericValidator.isBlankOrNull(dbname)) {
            throw new IllegalArgumentException("database name not specified");
        }
        final Timer timer = Metric
                .newTimer("DocumentRepositoryBdb.deleteDatabase");
        if (!existsDatabase(dbname)) {
            return false;
        }
        final Database database = getDatabase(dbname);
        if (database != null) {
            try {
                database.close();

            } catch (DatabaseException e) {
                throw new PersistenceException(e);
            }
        }
        removeDatabase(dbname);
        timer.stop();
        return true;
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
    public Document saveDocument(Document document, boolean overwrite)
            throws PersistenceException {
        if (document == null) {
            throw new NullPointerException("document not specified");
        }
        if (document.getId() == null) {
            document = new DocumentBuilder(document).setId(
                    getNextKey(document.getDatabase())).build();
        }

        final Timer timer = Metric
                .newTimer("DocumentRepositoryBdb.saveDocument");
        try {
            PrimaryIndex<String, JsonDocument> primaryIndex = getIndex(document
                    .getDatabase());

            JsonDocument old = primaryIndex.put(new JsonDocument(document
                    .getId(), document));
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("saving old " + old + ", new " + document);
            }
            return old != null ? old.getDocument() : document;
        } catch (DatabaseException e) {
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
    public List<Document> getAllDocuments(String dbname, long startKey,
            int limit) throws PersistenceException {
        if (GenericValidator.isBlankOrNull(dbname)) {
            throw new IllegalArgumentException("database name not specified");
        }
        if (limit <= 0) {
            limit = DEFAULT_LIMIT;
        }
        limit = Math.max(limit, MAX_MAX_LIMIT);
        final Timer timer = Metric
                .newTimer("DocumentRepositoryBdb.getAllDocuments");

        EntityCursor<JsonDocument> cursor = null;
        List<Document> all = new ArrayList<Document>();
        try {
            PrimaryIndex<String, JsonDocument> primaryIndex = getIndex(dbname);

            cursor = primaryIndex.entities(String.valueOf(startKey), false,
                    null, true);
            Iterator<JsonDocument> it = cursor.iterator();
            for (int i = 0; it.hasNext() && i < limit; i++) {
                JsonDocument next = it.next();
                all.add(next.getDocument());
            }
            return all;
        } catch (DatabaseException e) {
            throw new PersistenceException("Failed to find all in " + dbname, e);
        } finally {
            timer.stop();
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (DatabaseException e) {
                    LOGGER.error("failed to close cursor", e);
                }
            }
        }
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
    public PagedList<Document> getAllDocuments(final String dbname,
            final String startKey, final String endKey, final int max)
            throws PersistenceException {
        if (GenericValidator.isBlankOrNull(dbname)) {
            throw new IllegalArgumentException("database name not specified");
        }
        final int limit = Math.min(max, MAX_MAX_LIMIT);

        final Timer timer = Metric
                .newTimer("DocumentRepositoryBdb.getAllDocuments");

        EntityCursor<JsonDocument> cursor = null;
        List<Document> all = new ArrayList<Document>();
        try {
            PrimaryIndex<String, JsonDocument> primaryIndex = getIndex(dbname);
            cursor = primaryIndex.entities(startKey, true, endKey, true);
            Iterator<JsonDocument> it = cursor.iterator();
            String lastKey = null;
            for (int i = 0; it.hasNext() && i < limit; i++) {
                JsonDocument next = it.next();
                all.add(next.getDocument());
                lastKey = next.getId();
            }
            return new PagedList<Document>(all, startKey, lastKey, limit, all
                    .size() == limit);
        } catch (DatabaseException e) {
            throw new PersistenceException("Failed to get documents from "
                    + dbname + " with range " + startKey + "-" + endKey, e);
        } finally {
            timer.stop();
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (DatabaseException e) {
                    LOGGER.error("failed to close cursor", e);
                }
            }
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
    public Document getDocument(final String dbname, final String id)
            throws PersistenceException {
        if (GenericValidator.isBlankOrNull(dbname)) {
            throw new IllegalArgumentException("database name not specified");
        }
        if (GenericValidator.isBlankOrNull(id)) {
            throw new IllegalArgumentException("id name not specified");
        }
        final Timer timer = Metric
                .newTimer("DocumentRepositoryBdb.getDocument");
        try {
            PrimaryIndex<String, JsonDocument> primaryIndex = getIndex(dbname);
            JsonDocument d = primaryIndex.get(id);
            if (d == null) {
                throw new NotFoundException("Failed to find " + id + " in "
                        + dbname);
            }
            return d.getDocument();
        } catch (DatabaseException e) {
            throw new PersistenceException("Failed to get document with id "
                    + id, e);
        } finally {
            timer.stop();
        }
    }

    /**
     * @param database
     *            - name of the database
     * @return - documents matching ids
     * @throws PersistenceException
     *             is thrown if error occurs reading the database.
     */
    @Override
    public Map<String, Document> getDocuments(String database, String... ids)
            throws PersistenceException {
        return getDocuments(database, Arrays.asList(ids));
    }

    /**
     * @param database
     *            - name of the database
     * @return - documents matching ids
     * @throws PersistenceException
     *             is thrown if error occurs reading the database.
     */
    @Override
    public Map<String, Document> getDocuments(String dbname,
            Collection<String> ids) {
        if (GenericValidator.isBlankOrNull(dbname)) {
            throw new IllegalArgumentException("database name not specified");
        }
        if (null == ids) {
            throw new IllegalArgumentException("ids not specified");
        }
        Map<String, Document> docsById = new TreeMap<String, Document>();
        final Timer timer = Metric
                .newTimer("DocumentRepositoryBdb.getDocuments");
        try {
            PrimaryIndex<String, JsonDocument> primaryIndex = getIndex(dbname);
            for (String id : ids) {
                JsonDocument d = primaryIndex.get(id);
                if (d != null) {
                    docsById.put(id, d.getDocument());
                }
            }
            return docsById;
        } catch (DatabaseException e) {
            throw new PersistenceException("Failed to get documents with ids "
                    + ids, e);
        } finally {
            timer.stop();
        }
    }

    @Override
    public boolean deleteDocument(String dbname, String id, String version)
            throws PersistenceException {
        if (GenericValidator.isBlankOrNull(dbname)) {
            throw new IllegalArgumentException("database name not specified");
        }
        if (GenericValidator.isBlankOrNull(id)) {
            throw new IllegalArgumentException("id name not specified");
        }

        final Timer timer = Metric
                .newTimer("DocumentRepositoryBdb.deleteDocument");
        try {
            PrimaryIndex<String, JsonDocument> primaryIndex = getIndex(dbname);

            return primaryIndex.delete(id);
        } catch (DatabaseException e) {
            throw new NotFoundException("Failed to remove " + id + " in "
                    + dbname, e);
        } finally {
            timer.stop();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.persistence.DocumentRepository#getInfo(java.lang
     * .String)
     */
    @Override
    public Map<String, String> getInfo(String database)
            throws PersistenceException {
        Map<String, String> info = new HashMap<String, String>();
        info.put("db_name", database);
        info.put("count", String.valueOf(count(database)));
        return info;
    }

    @Override
    public Map<String, Document> query(String dbname,
            Map<String, String> criteria) {
        if (GenericValidator.isBlankOrNull(dbname)) {
            throw new IllegalArgumentException("database name not specified");
        }

        final Timer timer = Metric.newTimer("DocumentRepositoryBdb.query");

        EntityCursor<JsonDocument> cursor = null;
        Map<String, Document> docsById = new TreeMap<String, Document>();
        try {
            PrimaryIndex<String, JsonDocument> primaryIndex = getIndex(dbname);

            cursor = primaryIndex.entities(null, false, null, false);
            Iterator<JsonDocument> it = cursor.iterator();
            while (it.hasNext()) {
                JsonDocument next = it.next();
                Document doc = next.getDocument();
                boolean matched = true;
                for (Map.Entry<String, String> e : criteria.entrySet()) {
                    try {
                        String value = (String) doc.get(e.getKey());
                        if (!e.getValue().equals(value)) {
                            matched = false;
                            break;
                        }
                    } catch (Exception ex) {
                        LOGGER.error("Failed to match " + e.getKey() + " in "
                                + doc, ex);
                    }
                }
                if (matched) {
                    docsById.put(doc.getId(), doc);
                }
            }
            return docsById;
        } catch (DatabaseException e) {
            throw new PersistenceException("Failed to find all in " + dbname, e);
        } finally {
            timer.stop();
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (DatabaseException e) {
                    LOGGER.error("failed to close cursor", e);
                }
            }
        }
    }

    public synchronized boolean open() {
        if (env != null) {
            return false;
        }
        Durability defaultDurability = new Durability(
                Durability.SyncPolicy.SYNC, null, // unused by non-HA
                // applications.
                null); // unused by non-HA applications.

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(false);
        envConfig.setSharedCache(true);
        envConfig.setDurability(defaultDurability);
        // envConfig.setReadOnly(true);
        // envConfig.setTxnTimeout(1000000);
        if (!databaseDir.exists()) {
            databaseDir.mkdirs();
        }

        final DatabaseConfig dbConfig = new DatabaseConfig();
        envConfig.setTransactional(false);
        dbConfig.setAllowCreate(true);
        dbConfig.setReadOnly(false);
        // dbConfig.setInitializeLocking(true);
        // dbConfig.setType(DatabaseType.BTREE);

        // envConfig.setInitializeCache(true);
        // envConfig.setInitializeLocking(true);
        // envConfig.setInitializeLogging(true);
        // envConfig.setThreaded(true);

        try {
            this.env = new Environment(databaseDir, envConfig);
        } catch (EnvironmentLockedException e) {
            throw new PersistenceException(e);
        } catch (DatabaseException e) {
            throw new PersistenceException(e);
        }
        storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setDeferredWrite(true);

        // writer.optimize();
        // javaCatalog = new StoredClassCatalog(database);
        return true;
    }

    public synchronized boolean close() {
        if (env == null) {
            LOGGER.warn("already closed");
            return false;
        }

        for (EntityStore store : stores.values()) {
            try {
                store.close();
            } catch (DatabaseException e) {
                LOGGER.error("Failed to close " + store, e);
            }
        }
        for (Database database : databases.values()) {
            try {
                database.close();
            } catch (DatabaseException e) {
                LOGGER.error("Failed to close " + database, e);
            }
        }
        try {
            env.sync();
            env.close();
            env = null;
            indexes.clear();
            stores.clear();
            databases.clear();
            return true;
        } catch (DatabaseException e) {
            LOGGER.error("Failed to close BDB environment", e);
            return false;
        }
    }

    private synchronized void removeDatabase(final String dbname) {
        synchronized (dbname.intern()) {
            // EntityStore store = stores.remove(dbname);
            // if (store != null) {
            // try {
            // store.close();
            // } catch (IllegalStateException e) {
            // // already closed
            // LOGGER.warn("Aready closed store " + dbname + ": " + e);
            // } catch (DatabaseException e) {
            // LOGGER.warn("Failed to close store " + dbname + ": " + e);
            // }
            // }
            Database database = databases.remove(dbname);
            if (database != null) {
                try {
                    database.close();
                } catch (IllegalStateException e) {
                    // already closed
                } catch (DatabaseException e) {
                    LOGGER.warn("Failed to close store " + dbname + ": " + e);

                }
            }
            try {
                env.removeDatabase(null, dbname);
                databases.remove(dbname);
            } catch (DatabaseException e) {
                throw new PersistenceException("Failed to remove " + dbname, e);
            }
        }
    }

    private boolean existsDatabase(String dbName) throws PersistenceException {
        for (String nextDB : getAllDatabases()) {
            if (nextDB.equals(dbName)) {
                return true;
            }
        }
        return false;
    }

    public long count(final String dbname) {
        if (GenericValidator.isBlankOrNull(dbname)) {
            throw new IllegalArgumentException("database name not specified");
        }
        try {
            return getDatabase(dbname).count();
        } catch (DatabaseException e) {
            throw new PersistenceException("Failed to count " + dbname, e);
        }
    }

    //
    private PrimaryIndex<String, JsonDocument> getIndex(final String dbname) {
        synchronized (dbname.intern()) {

            if (GenericValidator.isBlankOrNull(dbname)) {
                throw new IllegalArgumentException("dbname is not specified");
            }
            PrimaryIndex<String, JsonDocument> index = indexes.get(dbname);
            if (index == null) {
                try {
                    index = _getStore(dbname).getPrimaryIndex(String.class,
                            JsonDocument.class);
                } catch (IncompatibleClassException e) {
                    throw new PersistenceException(e);
                } catch (DatabaseException e) {
                    throw new PersistenceException(e);
                }
                indexes.put(dbname, index);
            }
            return index;
        }
    }

    //
    private EntityStore _getStore(final String dbname) {
        synchronized (dbname.intern()) {

            if (GenericValidator.isBlankOrNull(dbname)) {
                throw new IllegalArgumentException("dbname is not specified");
            }
            getDatabase(dbname);
            EntityStore store = stores.get(dbname);
            if (store == null) {
                try {
                    store = new EntityStore(env, dbname, storeConfig);
                } catch (IncompatibleClassException e) {
                    throw new PersistenceException(e);
                } catch (DatabaseException e) {
                    throw new PersistenceException(e);
                }
                stores.put(dbname, store);
            }
            return store;
        }
    }

    Database getDatabase(final String dbname) {
        final Timer timer = Metric
                .newTimer("DocumentRepositoryBdb.deleteDocument");
        synchronized (dbname.intern()) {
            Database database = databases.get(dbname);
            if (database != null) {
                return database;
            }

            try {
                DatabaseConfig dbconfig = new DatabaseConfig();
                dbconfig.setAllowCreate(true);
                dbconfig.setSortedDuplicates(false);
                dbconfig.setExclusiveCreate(false);
                dbconfig.setReadOnly(false);
                dbconfig.setTransactional(false);
                database = env.openDatabase(null, dbname, dbconfig);
                databases.put(dbname, database);
                return database;
            } catch (DatabaseException e) {
                throw new PersistenceException("Failed to open " + dbname, e);
            } finally {
                timer.stop();
            }
        }
    }

    /**
     * Return the key of the last record entered in the database
     * 
     * @return String Numeric value of next key
     */

    public String getNextKey(final String dbname) {
        synchronized (dbname.intern()) {
            try {
                String lastKey = "0";
                Cursor cursor = null;
                cursor = getDatabase(dbname).openCursor(null, null);
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();
                if (cursor.getLast(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS)
                    lastKey = new String(key.getData());
                cursor.close();

                int i = new Integer(lastKey).intValue() + 1;
                return String.format("%10d", i);
            } catch (DatabaseException e) {
                throw new PersistenceException("Failed to get last key for "
                        + dbname, e);
            }
        }
    }

    protected SerialBinding<Document> getDocumentBinding(Database database)
            throws IllegalArgumentException, DatabaseException {
        return new SerialBinding<Document>(new StoredClassCatalog(database),
                Document.class);
    }

    protected SerialBinding<String> getKeyBinding(Database database)
            throws IllegalArgumentException, DatabaseException {
        return new SerialBinding<String>(new StoredClassCatalog(database),
                String.class);
    }

}

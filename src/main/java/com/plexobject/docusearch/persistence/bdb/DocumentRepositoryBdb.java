package com.plexobject.docusearch.persistence.bdb;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PagedList;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * @author Shahzad Bhatti
 * 
 *         TODO: implement this
 * 
 */
public class DocumentRepositoryBdb implements DocumentRepository {
    private static final int MAX_MAX_LIMIT = 1024;

    private static final Logger LOGGER = Logger
            .getLogger(DocumentRepositoryBdb.class);
    private final File envDir;
    private final boolean readOnly;
    private Environment env;
    private Map<String, Database> databases = Collections
            .synchronizedMap(new HashMap<String, Database>());

    public DocumentRepositoryBdb(final File envDir, final boolean readOnly) {
        this.envDir = envDir;
        this.readOnly = readOnly;
        open();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.persistence.DocumentRepository#createDatabase
     * (java.lang.String)
     */
    @Override
    public boolean createDatabase(String dbname) throws PersistenceException {
        if (GenericValidator.isBlankOrNull(dbname)) {
            throw new IllegalArgumentException("database name not specified");
        }
        final Timer timer = Metric
                .newTimer("DocumentRepositoryBdb.deleteDatabase");
        try {
            return getDatabase(dbname, false) != null;
        } finally {
            timer.stop();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.persistence.DocumentRepository#deleteDatabase
     * (java.lang.String)
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
        final Database database = getDatabase(dbname, true);
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

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.persistence.DocumentRepository#getAllDatabases()
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

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.persistence.DocumentRepository#getAllDocuments
     * (java.lang.String, long, int)
     */
    @Override
    public List<Document> getAllDocuments(String dbname, long startKey,
            int limit) throws PersistenceException {
        if (GenericValidator.isBlankOrNull(dbname)) {
            throw new IllegalArgumentException("database name not specified");
        }
        final Timer timer = Metric
                .newTimer("DocumentRepositoryBdb.getAllDocuments");
        /*
         * partMap = new StoredMap(db.getPartDatabase(), partKeyBinding,
         * partValueBinding, true);
         * 
         * txn = env.beginTransaction(null, null); DbDirectory directory = new
         * DbDirectory(txn, index, blocks);
         * 
         * directory.close(); txn.commit();
         */
        // TODO Auto-generated method stub
        timer.stop();
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.persistence.DocumentRepository#getAllDocuments
     * (java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public PagedList<Document> getAllDocuments(final String dbname,
            final String startKey, final String endKey, final int max)
            throws PersistenceException {
        if (GenericValidator.isBlankOrNull(dbname)) {
            throw new IllegalArgumentException("database name not specified");
        }
        if (GenericValidator.isBlankOrNull(startKey)) {
            throw new IllegalArgumentException("startKey name not specified");
        }
        if (GenericValidator.isBlankOrNull(endKey)) {
            throw new IllegalArgumentException("endKey name not specified");
        }
        final int limit = Math.min(max, MAX_MAX_LIMIT);
        final Timer timer = Metric
                .newTimer("DocumentRepositoryBdb.getAllDocuments");
        final boolean rangeFlag = true;
        try {
            final DatabaseEntry dbKey = new DatabaseEntry(startKey
                    .getBytes("UTF-8"));
            DatabaseEntry dbData = new DatabaseEntry();
            final Database database = getDatabase(dbname, false);
            final SerialBinding<Document> docBinding = getDocumentBinding(database);

            final Cursor cursor = database.openCursor(null, null);
            final List<Document> documents = new ArrayList<Document>();

            int numRecords = 0;
            OperationStatus ostat = null;

            while ((ostat == OperationStatus.SUCCESS)
                    && (numRecords < limit)) {
                final Document doc = docBinding.entryToObject(dbData);
                documents.add(doc);

                if (endKey.equals(doc.getId())) {
                    break;
                }
                dbData = new DatabaseEntry();
                ostat = (rangeFlag) ? cursor.getNext(dbKey, dbData,
                        LockMode.DEFAULT) : cursor.getNextDup(dbKey, dbData,
                        LockMode.DEFAULT);
                numRecords++;
            }
            return new PagedList<Document>(documents, startKey, endKey,
                    documents.size(), documents.size() == MAX_MAX_LIMIT);
        } catch (DatabaseException e) {
            throw new PersistenceException("Failed to get documents from "
                    + dbname + " with range " + startKey + "-" + endKey, e);
        } catch (UnsupportedEncodingException e) {
            throw new PersistenceException("Failed to get documents from "
                    + dbname + " with range " + startKey + "-" + endKey, e);
        } finally {
            timer.stop();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.persistence.DocumentRepository#getDocument(java
     * .lang.String, java.lang.String)
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
        try {
            final DatabaseEntry dbKey = new DatabaseEntry(id.getBytes("UTF-8"));
            final DatabaseEntry dbData = new DatabaseEntry();
            final Database database = getDatabase(dbname, false);
            final SerialBinding<Document> docBinding = getDocumentBinding(database);
            final OperationStatus ost = database.get(null, dbKey, dbData,
                    LockMode.DEFAULT);
            if (ost == OperationStatus.SUCCESS) {
                return docBinding.entryToObject(dbData);
            }
            throw new PersistenceException("Failed to get document with id "
                    + id + " with status " + ost);
        } catch (DatabaseException e) {
            throw new PersistenceException("Failed to get document with id "
                    + id, e);
        } catch (UnsupportedEncodingException e) {
            throw new PersistenceException("Failed to encode document with id "
                    + id, e);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.persistence.DocumentRepository#getDocuments(java
     * .lang.String, java.lang.String[])
     */
    @Override
    public Map<String, Document> getDocuments(String database, String... ids)
            throws PersistenceException {
        return getDocuments(database, Arrays.asList(ids));
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.persistence.DocumentRepository#getDocuments(java
     * .lang.String, java.util.Collection)
     */
    @Override
    public Map<String, Document> getDocuments(String dbName,
            Collection<String> ids) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean deleteDocument(String database, String id, String version)
            throws PersistenceException {
        // TODO Auto-generated method stub
        return false;
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
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.persistence.DocumentRepository#saveDocument(com
     * .plexobject.docusearch.domain.Document)
     */
    @Override
    public Document saveDocument(Document document, boolean overwrite) throws PersistenceException {
        if (document == null) {
            throw new NullPointerException("document not specified");
        }
        final String id = document.getId() != null ? document.getId()
                : getNextKey(document.getDatabase());

        try {
            final DatabaseEntry dbKey = new DatabaseEntry(id.getBytes("UTF-8"));
            final DatabaseEntry dbData = new DatabaseEntry();
            final Database database = getDatabase(document.getDatabase(), false);
            final SerialBinding<Document> binding = getDocumentBinding(database);
            binding.objectToEntry(document, dbData);
            final OperationStatus ost = getDatabase(document.getDatabase(),
                    false).put(null, dbKey, dbData);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Added " + document + " exists? "
                        + (ost == OperationStatus.KEYEXIST));
            }

        } catch (DatabaseException e) {
            throw new PersistenceException("Failed to save " + document, e);
        } catch (UnsupportedEncodingException e) {
            throw new PersistenceException("Failed to encode " + document, e);
        }
        return document;
    }

    @Override
    public Map<String, Document> query(String dbName,
            Map<String, String> criteria) {
        // TODO Auto-generated method stub
        return null;
    }

    public synchronized boolean open() {
        if (env != null) {
            return false;
        }
        final EnvironmentConfig envConfig = new EnvironmentConfig();
        final DatabaseConfig dbConfig = new DatabaseConfig();

        envConfig.setTransactional(false);
        dbConfig.setAllowCreate(!readOnly);
        dbConfig.setReadOnly(readOnly);
        // dbConfig.setInitializeLocking(true);
        // dbConfig.setType(DatabaseType.BTREE);

        // envConfig.setInitializeCache(true);
        // envConfig.setInitializeLocking(true);
        // envConfig.setInitializeLogging(true);
        // envConfig.setThreaded(true);

        try {
            this.env = new Environment(envDir, envConfig);
        } catch (EnvironmentLockedException e) {
            throw new PersistenceException(e);
        } catch (DatabaseException e) {
            throw new PersistenceException(e);
        }

        // writer.optimize();
        // javaCatalog = new StoredClassCatalog(database);
        return true;
    }

    public synchronized boolean close() {
        if (env == null) {
            LOGGER.warn("already closed");
            return false;
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
            databases.clear();
            return true;
        } catch (DatabaseException e) {
            LOGGER.error("Failed to close BDB environment", e);
            return false;
        }
    }

    private synchronized void removeDatabase(final String dbname) {
        try {
            env.removeDatabase(null, dbname);
            databases.remove(dbname);

        } catch (DatabaseException e) {
            throw new PersistenceException("Failed to remove " + dbname);
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
            return getDatabase(dbname, false).count();
        } catch (DatabaseException e) {
            throw new PersistenceException("Failed to count " + dbname, e);
        }
    }

    private Database getDatabase(final String dbname, final boolean cacheOnly) {
        synchronized (dbname.intern()) {
            Database database = databases.get(dbname);
            if (database != null) {
                return database;
            }
            if (cacheOnly) {
                return null;
            }
            try {
                DatabaseConfig dbconfig = new DatabaseConfig();
                dbconfig.setAllowCreate(true);
                dbconfig.setSortedDuplicates(false);
                dbconfig.setExclusiveCreate(false);
                dbconfig.setReadOnly(false);
                dbconfig.setTransactional(true);
                database = env.openDatabase(null, dbname, dbconfig);
                databases.put(dbname, database);
                return database;
            } catch (DatabaseException e) {
                throw new PersistenceException("Failed to open " + dbname, e);
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
                cursor = getDatabase(dbname, false).openCursor(null, null);
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

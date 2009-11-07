package com.plexobject.docusearch.persistence;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.plexobject.docusearch.domain.Document;

/**
 * This interface defines APIs to read or save documents and metadata related to
 * documents.
 * 
 * @author Shahzad Bhatti
 * 
 */
public interface DocumentRepository {
    /**
     * @return - names of all databases
     */
    public String[] getAllDatabases() throws PersistenceException;

    /**
     * This method creates a new database on the document database server.
     * 
     * @param database
     *            - name of the database
     * @return - true if created the database successfully
     * @throws PersistenceException
     *             is thrown when error occurs while creating the database.
     */
    public boolean createDatabase(final String database)
            throws PersistenceException;

    /**
     * This method deletes an existng database on the document database server.
     * 
     * @param database
     *            - name of the database
     * @return - true if deleted the database successfully
     * @throws PersistenceException
     *             is thrown when error occurs while deleting the database.
     */
    public boolean deleteDatabase(final String database)
            throws PersistenceException;

    /**
     * @param document
     *            - document containing database, id and attributes
     * @return - saved document
     * @throws PersistenceException
     *             is thrown when error occurs while saving the database.
     */
    public Document saveDocument(final Document document)
            throws PersistenceException;

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
    public List<Document> getAllDocuments(final String database,
            final long startKey, final int limit) throws PersistenceException;

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
    public PagedList<Document> getAllDocuments(final String database,
            final String startKey, final String endKey, final int limit)
            throws PersistenceException;

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
    public Document getDocument(final String database, final String id)
            throws PersistenceException;

    /**
     * This method deletes an existing document
     * 
     * @param database
     *            - name of the database
     * @param id
     *            - id of document
     * @param version
     *            - version of the document
     * @return - true if deleted the document successfully
     * @throws PersistenceException
     *             is thrown when error occurs while deleting the document.
     */
    public boolean deleteDocument(final String database, final String id,
            final String version) throws PersistenceException;

    /**
     * @param database
     *            - name of the database
     * @return - map of document-id and document
     * @throws PersistenceException
     *             is thrown when error occurs creating the database.
     */
    public Map<String, Document> getDocuments(final String database,
            final String... ids) throws PersistenceException;

    /**
     * @param database
     *            - name of the database
     * @return - true if created the database successfully
     * @throws PersistenceException
     *             is thrown when error occurs creating the database.
     */
    public Map<String, String> getInfo(final String database)
            throws PersistenceException;

    /**
     * @param database
     *            - name of the database
     * @param ids
     *            - ids of documents
     * @return - documents matching ids
     * @throws PersistenceException
     *             is thrown if error occurs reading the database.
     */
    public Map<String, Document> getDocuments(String dbName,
            Collection<String> ids);

    /**
     * @param database
     *            - name of the database
     * @param criteria
     *            - name/value criteria joined by AND
     * @return - documents matching ids
     * @throws PersistenceException
     *             is thrown if error occurs reading the database.
     */
    public Map<String, Document> query(String dbName,
            Map<String, String> criteria);
}

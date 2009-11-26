package com.plexobject.docusearch.persistence.couchdb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.converter.ConversionException;
import com.plexobject.docusearch.converter.Converter;
import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.domain.Tuple;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.http.RestException;
import com.plexobject.docusearch.http.impl.RestClientImpl;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.NotFoundException;
import com.plexobject.docusearch.persistence.PagedList;
import com.plexobject.docusearch.persistence.PersistenceException;

/**
 * This class implements DocumentRepository using CouchDB.
 * 
 * @author Shahzad Bhatti
 * 
 */
@Component("documentRepository")
public class DocumentRepositoryCouchdb implements DocumentRepository {
    private static final int MAX_MAX_LIMIT = 1024;

    public static final String DEFAULT_FIELD = "default";

    public static final String VIEW = "_view";

    public static final String SEQ = "_seq";

    public static final String TITLE = "_title";

    public static final String AUTHOR = "_author";
    private static final String ID = "id";
    private static final String REVISION = "rev";
    private static final Logger LOGGER = Logger
            .getLogger(DocumentRepositoryCouchdb.class);

    public static final String DB_URL = Configuration.getInstance()
            .getProperty("couchdb.url", "http://localhost:5984");

    public static final String DB_USER = Configuration.getInstance()
            .getProperty("couchdb.user");

    public static final String DB_PASSWORD = Configuration.getInstance()
            .getProperty("couchdb.password");

    private final RestClient httpClient;
    @SuppressWarnings("unchecked")
    private final Converter<JSONObject, Map> jsonToObject = Converters
            .getInstance().getConverter(JSONObject.class, Map.class);

    /**
     * document repository implementation
     * 
     */
    public DocumentRepositoryCouchdb() {
        this(new RestClientImpl(DB_URL, DB_USER, DB_PASSWORD));
    }

    DocumentRepositoryCouchdb(final RestClient httpClient) {
        if (httpClient == null) {
            throw new NullPointerException("httpClient");
        }
        this.httpClient = httpClient;
    }

    /**
     * @return - names of all databases
     */
    @Override
    public String[] getAllDatabases() throws PersistenceException {
        final Timer timer = Metric
                .newTimer("DocumentRepositoryCouchdb.getAllDatabases");
        try {
            final Tuple jsonResponse = httpClient.get("_all_dbs");
            JSONArray arr = new JSONArray((String) jsonResponse.second());
            String[] databases = new String[arr.length()];
            for (int i = 0; i < databases.length; i++) {
                databases[i] = arr.getString(i);
            }
            return databases;
        } catch (RestException e) {
            throw new PersistenceException("Failed to get databases", e, e
                    .getErrorCode());
        } catch (IOException e) {
            throw new PersistenceException("Failed to get databases", e);
        } catch (JSONException e) {
            throw new ConversionException("failed to convert json", e);
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
                .newTimer("DocumentRepositoryCouchdb.createDatabase");
        try {
            Tuple response = httpClient.put(encode(database), null);
            Integer rc = response.first();
            return rc == RestClient.OK_CREATED;
        } catch (RestException e) {
            throw new PersistenceException("Failed to create database"
                    + database, e, e.getErrorCode());
        } catch (IOException e) {
            throw new PersistenceException("Failed to create database"
                    + database, e);
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
                .newTimer("DocumentRepositoryCouchdb.deleteDatabase");
        try {
            return httpClient.delete(encode(database)) == RestClient.OK;
        } catch (RestException e) {
            throw new PersistenceException("Failed to delete database"
                    + database, e, e.getErrorCode());
        } catch (IOException e) {
            throw new PersistenceException("Failed to delete database"
                    + database, e);
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
    @SuppressWarnings("unchecked")
    @Override
    public Document saveDocument(final Document document,
            final boolean overwrite) throws PersistenceException {
        if (null == document) {
            throw new NullPointerException("document not specified");
        }
        final Timer timer = Metric
                .newTimer("DocumentRepositoryCouchdb.saveDocument");

        try {
            final String body = Converters.getInstance().getConverter(
                    Document.class, JSONObject.class).convert(document)
                    .toString();
            Tuple response = null;
            try {
                if (document.getId() != null) {
                    response = httpClient.put(String.format("%s/%s",
                            encode(document.getDatabase()), document.getId()),
                            body);
                } else {
                    response = httpClient.post(encode(document.getDatabase()),
                            body);
                }
            } catch (RestException e) {
                if (e.getErrorCode() == RestClient.CLIENT_ERROR_PRECONDITION
                        || e.getErrorCode() == RestClient.CLIENT_ERROR_NOT_FOUND) {
                    createDatabase(document.getDatabase());
                    return saveDocument(document, overwrite);
                } else if (e.getErrorCode() == RestClient.CLIENT_ERROR_CONFLICT
                        && overwrite) {
                    final Document oldDoc = getDocument(document.getDatabase(),
                            document.getId());
                    return saveDocument(new DocumentBuilder(oldDoc)
                            .setRevision(oldDoc.getRevision()).build(), false);
                } else {
                    throw e;
                }
            }
            final Integer rc = response.first();
            if (rc != RestClient.OK_CREATED) {
                throw new PersistenceException(
                        "failed to save document with error code " + rc);
            }
            Map<String, Object> idAndRev = jsonToObject.convert(new JSONObject(
                    (String) response.second()));
            final String id = (String) idAndRev.get(ID);
            final String rev = (String) idAndRev.get(REVISION);
            return new DocumentBuilder(document.getDatabase()).putAll(document)
                    .setId(id).setRevision(rev).build();
        } catch (RestException e) {
            throw new PersistenceException("Failed to save " + document, e, e
                    .getErrorCode());
        } catch (IOException e) {
            throw new PersistenceException("Failed to save " + document, e);
        } catch (JSONException e) {
            throw new ConversionException("failed to convert json", e);
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
        if (limit > MAX_MAX_LIMIT) {
            limit = MAX_MAX_LIMIT;
        }

        return getAllDocuments(String.format(
                "%s/_all_docs_by_seq?startkey=%d&limit=%d&include_docs=true",
                encode(database), startKey, limit));
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
        if (GenericValidator.isBlankOrNull(database)) {
            throw new IllegalArgumentException("database not specified");
        }
        final int limit = Math.min(max, MAX_MAX_LIMIT);
        StringBuilder req = new StringBuilder();
        req.append(encode(database));
        req.append("/_all_docs?limit=").append(limit);
        if (!GenericValidator.isBlankOrNull(startKey)) {
            req.append(String.format("&startkey=%%22%s%%22", encode(startKey)));
        }
        if (!GenericValidator.isBlankOrNull(endKey)) {
            req.append(String.format("&endkey=%%22%s%%22", encode(endKey)));
        }
        req.append("&include_docs=true");
        List<Document> docs = getAllDocuments(req.toString());
        return new PagedList<Document>(docs, startKey, endKey, limit, docs
                .size() == limit);
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
                .newTimer("DocumentRepositoryCouchdb.getDocuments");

        final JSONArray keys = new JSONArray();
        for (final String id : ids) {
            keys.put(id);
        }
        final JSONObject req = new JSONObject();
        try {
            req.put("keys", keys);

            final Tuple response = httpClient.post(String.format(
                    "%s/_all_docs?include_docs=true", encode(database)), req
                    .toString());
            final JSONObject jsonDocs = new JSONObject((String) response
                    .second());
            List<Document> documents = toDocuments(jsonDocs);
            Map<String, Document> docsById = new HashMap<String, Document>();
            for (Document doc : documents) {
                docsById.put(doc.getId(), doc);
            }
            return docsById;

        } catch (RestException e) {
            throw new PersistenceException("Failed to get documents for "
                    + database + " with ids " + ids, e, e.getErrorCode());
        } catch (IOException e) {
            throw new PersistenceException("Failed to get documents for "
                    + database + " with ids " + ids, e);
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
            final Tuple response = httpClient.get(String.format("%s/%s",
                    encode(database), id));
            final JSONObject jsonDoc = new JSONObject((String) response
                    .second());
            jsonDoc.put(Document.DATABASE, database);
            return Converters.getInstance().getConverter(JSONObject.class,
                    Document.class).convert(jsonDoc);
        } catch (RestException e) {
            if (RestClient.CLIENT_ERROR_NOT_FOUND == e.getErrorCode()) {
                throw new NotFoundException("Failed to find " + id + " in "
                        + database + " on " + httpClient);
            } else {
                throw new PersistenceException("Failed to get " + id + " from "
                        + database, e, e.getErrorCode());
            }
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
                .newTimer("DocumentRepositoryCouchdb.deleteDocument");
        try {
            final String url = String.format("%s/%s?rev=%s", encode(database),
                    encode(id), encode(version));
            return httpClient.delete(encode(url)) == RestClient.OK;
        } catch (RestException e) {
            throw new PersistenceException("Failed to delete document"
                    + database + "/" + id + "/" + version, e, e.getErrorCode());
        } catch (IOException e) {
            throw new PersistenceException("Failed to delete document"
                    + database + "/" + id + "/" + version, e);
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
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, String> getInfo(final String database)
            throws PersistenceException {
        if (GenericValidator.isBlankOrNull(database)) {
            throw new IllegalArgumentException("database not specified");
        }

        try {
            final Tuple response = httpClient.get(encode(database));
            final JSONObject json = new JSONObject((String) response.second());
            return (Map<String, String>) Converters.getInstance().getConverter(
                    JSONObject.class, Object.class).convert(json);
        } catch (RestException e) {
            throw new PersistenceException(
                    "Failed to get info for " + database, e, e.getErrorCode());
        } catch (IOException e) {
            throw new PersistenceException(
                    "Failed to get info for " + database, e);
        } catch (JSONException e) {
            throw new ConversionException("failed to convert json", e);
        }
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
        final Timer timer = Metric.newTimer("DocumentRepositoryCouchdb.query");
        final StringBuilder func = new StringBuilder("function(doc) {");
        boolean firstCriteria = true;
        for (Map.Entry<String, String> e : criteria.entrySet()) {
            if (!firstCriteria) {
                func.append(" && ");
            }
            firstCriteria = false;
            func.append("if (doc." + e.getKey() + " == '" + e.getValue()
                    + "' && doc.deleted != true) {");
        }
        func.append("emit(null, doc);");
        func.append("}");
        func.append("}");

        try {
            final JSONObject req = new JSONObject();
            req.put("map", func.toString());

            final Tuple response = httpClient.post(String.format(
                    "%s/_temp_view", encode(dbName)), req.toString());
            JSONObject jsonDocs = new JSONObject((String) response.second());
            List<Document> documents = toDocuments(jsonDocs);
            Map<String, Document> docsById = new HashMap<String, Document>();
            for (Document doc : documents) {
                docsById.put(doc.getId(), doc);
            }
            return docsById;

        } catch (RestException e) {
            throw new PersistenceException("Failed to get documents for "
                    + dbName + " with criteria " + criteria, e, e
                    .getErrorCode());
        } catch (IOException e) {
            throw new PersistenceException("Failed to get documents for "
                    + dbName + " with criteria " + criteria, e);
        } catch (JSONException e) {
            throw new ConversionException("failed to convert json", e);
        } finally {
            timer.stop();
        }

    }

    //
    private List<Document> getAllDocuments(final String url)
            throws PersistenceException {
        final Timer timer = Metric
                .newTimer("DocumentRepositoryCouchdb.getAllDocuments");
        try {
            final Tuple response = httpClient.get(url);
            final JSONObject jsonDocs = new JSONObject((String) response
                    .second());
            List<Document> docs = toDocuments(jsonDocs);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("getAllDocuments(" + url + ") got " + docs.size());
            }
            return docs;
        } catch (RestException e) {
            throw new PersistenceException(
                    "Failed to get documents for " + url, e, e.getErrorCode());
        } catch (IOException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Failed to get documents for " + url, e);
            }
            throw new PersistenceException(
                    "Failed to get documents for " + url, e);
        } catch (JSONException e) {
            throw new ConversionException("failed to convert json", e);
        } finally {
            timer.stop();
        }
    }

    private List<Document> toDocuments(final JSONObject jsonDocs) {
        JSONArray jsonArray;
        try {
            jsonArray = jsonDocs.getJSONArray("rows");
        } catch (JSONException e) {
            throw new ConversionException("failed to convert json " + jsonDocs,
                    e);
        }

        List<Document> documents = new ArrayList<Document>();

        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonDoc = null;
            try {
                final String tag = jsonArray.getJSONObject(i).has("doc") ? "doc"
                        : "value";

                jsonDoc = jsonArray.getJSONObject(i).getJSONObject(tag);
                final Document document = Converters.getInstance()
                        .getConverter(JSONObject.class, Document.class)
                        .convert(jsonDoc);
                documents.add(document);
            } catch (IllegalArgumentException e) {
                LOGGER.error("Failed to parse document " + i + " from "
                        + jsonDoc + " due to " + e);
            } catch (Exception e) {
                LOGGER.error("Failed to parse document " + i + " from "
                        + jsonDoc, e);
            }
        }
        return documents;

    }

    public String encode(final String path) {
        try {
            return URLEncoder.encode(path, "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 support missing!");
        }
    }
}

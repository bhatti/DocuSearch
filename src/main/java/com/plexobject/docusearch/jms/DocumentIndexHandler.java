package com.plexobject.docusearch.jms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.support.converter.MessageConversionException;
import org.springframework.jms.support.converter.MessageConverter;

import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.converter.HtmlToTextConverter;
import com.plexobject.docusearch.docs.DocumentsDatabaseIndexer;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.Pair;
import com.plexobject.docusearch.domain.Tuple;
import com.plexobject.docusearch.jmx.JMXRegistrar;
import com.plexobject.docusearch.jmx.impl.ServiceJMXBeanImpl;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.sun.jersey.spi.inject.Inject;

public abstract class DocumentIndexHandler implements MessageListener,
        MessageConverter, ExceptionListener {
    enum Action {
        ADD, DELETE
    }

    protected final Logger LOGGER = Logger.getLogger(getClass());
    private final String sourceName;
    final String indexName;
    private final String policyName;
    protected final HtmlToTextConverter htmlToTextConverter = new HtmlToTextConverter();

    @Autowired
    @Inject
    DocumentsDatabaseIndexer documentsDatabaseIndexer;

    @Autowired
    @Inject
    DocumentRepository documentRepository;

    private final ServiceJMXBeanImpl mbean;

    public DocumentIndexHandler(final String sourceName,
            final String indexName, final String policyName) {
        if (GenericValidator.isBlankOrNull(sourceName)) {
            throw new IllegalArgumentException("sourceName not specified");
        }
        if (GenericValidator.isBlankOrNull(indexName)) {
            throw new IllegalArgumentException("indexName not specified");
        }

        if (GenericValidator.isBlankOrNull(policyName)) {
            throw new IllegalArgumentException("sourceName not specified");
        }

        this.sourceName = sourceName;
        this.indexName = indexName;
        this.policyName = policyName;
        mbean = JMXRegistrar.getInstance().register(getClass());
    }

    public void onMessage(final Message message) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("*** Received " + message);
        }
        if (documentsDatabaseIndexer == null) {
            throw new NullPointerException(
                    "documentsDatabaseIndexer not specified");
        }
        try {
            final JSONArray arr = toJSONArray(message);
            final List<Document> docs = getNewDocuments(arr);
            if (docs != null && docs.size() > 0) {
                save(docs);
                int succeeded = indexDocuments(docs);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Indexed " + succeeded + "/" + docs.size()
                            + ", json array " + arr.length());
                }
            }
            //
            final Tuple deleteRequest = getDeleteRequest(arr);
            if (deleteRequest != null && deleteRequest.size() > 0) {
                final String database = deleteRequest.first();
                final String secondaryIdName = deleteRequest.second();
                final Collection<Pair<String, String>> primaryAndSecondaryId = deleteRequest
                        .third();
                final Integer olderThanDays = deleteRequest.last();
                int removed = removeIndexedDocuments(database, secondaryIdName,
                        primaryAndSecondaryId, olderThanDays);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Removed " + removed
                            + " documents from json request size "
                            + arr.length());
                }
            }
            mbean.incrementRequests();
        } catch (RuntimeException e) {
            mbean.incrementError();

            LOGGER.error("Failed to convert JSON format " + message, e);
            throw e;
        } catch (JMSException e) {
            mbean.incrementError();

            LOGGER.error("Failed to process feed messages " + message, e);
            throw new RuntimeException(e);
        } catch (JSONException e) {
            mbean.incrementError();

            LOGGER.error("Failed to convert JSON format " + message, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object fromMessage(final Message message) throws JMSException,
            MessageConversionException {
        return null; // NOT IMPLEMENTED
    }

    @SuppressWarnings("unchecked")
    @Override
    public Message toMessage(Object object, Session session)
            throws JMSException, MessageConversionException {
        final List<Document> docs = (List<Document>) object;

        JSONArray jsonDocs = new JSONArray();
        for (Document doc : docs) {
            JSONObject jsonDoc = Converters.getInstance().getConverter(
                    Document.class, JSONObject.class).convert(doc);
            jsonDocs.put(jsonDoc);
        }
        return session.createTextMessage(jsonDocs.toString());
    }

    @Override
    public void onException(final JMSException e) {
        LOGGER.error("onException", e);
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getPolicyName() {
        return policyName;
    }

    /**
     * @return the documentRepository
     */
    public DocumentRepository getDocumentRepository() {
        return documentRepository;
    }

    /**
     * @param documentRepository
     *            the documentRepository to set
     */
    public void setDocumentRepository(DocumentRepository documentRepository) {
        this.documentRepository = documentRepository;
    }

    protected List<Document> createNewDocuments(final JSONObject jsonDoc)
            throws JSONException {
        jsonDoc.put(Document.DATABASE, sourceName);
        final Document doc = Converters.getInstance().getConverter(
                JSONObject.class, Document.class).convert(jsonDoc);
        return Arrays.asList(doc);
    }

    protected Tuple createDeleteRequest(final JSONArray jsonDoc)
            throws JSONException {
        return null;
    }

    protected abstract void save(final List<Document> docs);

    // //////
    private JSONArray toJSONArray(final Message message) throws JMSException,
            JSONException {
        if (message instanceof TextMessage) {
            final String docMessage = ((TextMessage) message).getText();
            return new JSONArray(docMessage);
        } else {
            LOGGER.fatal("Unknown message format received " + message);
            throw new IllegalArgumentException(
                    "Message must be of type ObjectMessage");
        }
    }

    // //////
    private List<Document> getNewDocuments(final JSONArray jsonDocs)
            throws JSONException {
        final int len = jsonDocs.length();
        List<Document> docs = new ArrayList<Document>();
        for (int i = 0; i < len; i++) {
            final JSONObject jsonDoc = jsonDocs.getJSONObject(i);
            if (getAction(jsonDoc) == Action.ADD) {
                docs.addAll(createNewDocuments(jsonDoc));
            }
        }
        return docs;
    }

    // //////
    private Tuple getDeleteRequest(final JSONArray jsonDocs)
            throws JSONException {
        JSONArray deletedArr = new JSONArray();
        final int len = jsonDocs.length();
        for (int i = 0; i < len; i++) {
            final JSONObject jsonDoc = jsonDocs.getJSONObject(i);
            if (getAction(jsonDoc) == Action.DELETE) {
                deletedArr.put(jsonDoc);
            }
        }
        return deletedArr.length() > 0 ? createDeleteRequest(deletedArr) : null;
    }

    int indexDocuments(final List<Document> docs) {
        int succeeded = 0;
        if (docs.size() > 0) {
            succeeded = documentsDatabaseIndexer
                    .updateIndexUsingPrimaryDatabase(indexName, policyName,
                            docs);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Added " + succeeded + "/" + docs.size() + "/"
                        + docs.size() + " to " + indexName);
            }

        }
        return succeeded;
    }

    int removeIndexedDocuments(final String database, String secondaryIdName,
            final Collection<Pair<String, String>> primaryAndSecondaryIds,
            int olderThanDays) {
        return documentsDatabaseIndexer.removeIndexedDocuments(indexName,
                database, secondaryIdName, primaryAndSecondaryIds,
                olderThanDays);
    }

    private Action getAction(JSONObject jsonDoc) {
        String action = jsonDoc.optString("action", null);

        if (action != null
                && (action.toLowerCase().startsWith("delete") || action
                        .toLowerCase().startsWith("remove"))) {
            return Action.DELETE;
        }
        return Action.ADD;
    }
}

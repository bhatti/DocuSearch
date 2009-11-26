package com.plexobject.docusearch.jms;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.plexobject.docusearch.docs.DocumentsDatabaseIndexer;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.jmx.JMXRegistrar;
import com.plexobject.docusearch.jmx.impl.ServiceJMXBeanImpl;
import com.sun.jersey.spi.inject.Inject;

public class DocumentIndexHandler implements MessageListener, MessageConverter,
        ExceptionListener {
    protected static final Logger LOGGER = Logger
            .getLogger(DocumentIndexHandler.class);
    private final String sourceName;
    private final String indexName;
    private final String policyName;

    @Autowired
    @Inject
    DocumentsDatabaseIndexer documentsDatabaseIndexer;
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

    @SuppressWarnings("unchecked")
    public void onMessage(final Message message) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Received " + message);
        }
        if (documentsDatabaseIndexer == null) {
            throw new NullPointerException(
                    "documentsDatabaseIndexer not specified");
        }
        try {
            final List<Document> docs = (List<Document>) fromMessage(message);
            int succeeded = indexDocuments(docs);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Indexed " + succeeded + "/" + docs.size());
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
        }
    }

    @Override
    public Object fromMessage(final Message message) throws JMSException,
            MessageConversionException {
        if (message instanceof TextMessage) {
            final String docMessage = ((TextMessage) message).getText();
            final List<Document> docs = new ArrayList<Document>();

            JSONArray jsonDocs;
            try {
                jsonDocs = new JSONArray(docMessage);
                final int len = jsonDocs.length();
                for (int i = 0; i < len; i++) {
                    final JSONObject jsonDoc = jsonDocs.getJSONObject(i);
                    docs.addAll(jsonToDocuments(jsonDoc));
                }
                return docs;
            } catch (JSONException e) {
                LOGGER.error("Failed to process feed messages " + message, e);
                throw new RuntimeException(e);
            }
        } else {
            LOGGER.fatal("Unknown message format received " + message);
            throw new IllegalArgumentException(
                    "Message must be of type ObjectMessage");
        }
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

    protected List<Document> jsonToDocuments(final JSONObject jsonDoc)
            throws JSONException {
        jsonDoc.put(Document.DATABASE, sourceName);
        final Document doc = Converters.getInstance().getConverter(
                JSONObject.class, Document.class).convert(jsonDoc);
        return Arrays.asList(doc);
    }

    int indexDocuments(final List<Document> docs) {
        int succeeded = 0;
        if (docs.size() > 0) {
            succeeded = documentsDatabaseIndexer
                    .updateIndexUsingPrimaryDatabase(indexName, policyName,
                            docs);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Added " + succeeded + "/" + docs.size() + " to "
                        + indexName);
            }
        }
        return succeeded;
    }
}

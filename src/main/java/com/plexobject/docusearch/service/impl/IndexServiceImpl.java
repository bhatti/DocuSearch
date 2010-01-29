package com.plexobject.docusearch.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.docs.DocumentsDatabaseIndexer;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.Pair;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.jmx.JMXRegistrar;
import com.plexobject.docusearch.jmx.impl.ServiceJMXBeanImpl;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.service.IndexService;
import com.sun.jersey.spi.inject.Inject;

@Path("/index")
@Component("indexService")
@Scope("singleton")
public class IndexServiceImpl implements IndexService, InitializingBean {
    private static final Logger LOGGER = Logger
            .getLogger(IndexServiceImpl.class);
    private static final boolean ACTIVATE_INDEX = Configuration.getInstance()
            .getBoolean("activate.index", true);
    @Autowired
    @Inject
    DocumentsDatabaseIndexer documentsDatabaseIndexer;

    private final ServiceJMXBeanImpl mbean;

    public IndexServiceImpl() {
        mbean = JMXRegistrar.getInstance().register(getClass());
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.WILDCARD })
    @Path("/primary/{index}")
    @Override
    public Response createIndexUsingPrimaryDatabase(
            @PathParam("index") final String index,
            @QueryParam("sourceDatabase") final String sourceDatabase,
            @QueryParam("policyName") final String policyName) {
        if (!ACTIVATE_INDEX) {
            return Response.status(RestClient.SERVICE_UNAVAILABLE).type(
                    "text/plain").entity("Index Service is not available\n")
                    .build();
        }
        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified\n").build();
        }
        if (index.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index name is valid " + index + "\n")
                    .build();
        }
        final Timer timer = Metric
                .newTimer("IndexServiceImpl.createIndexUsingPrimaryDatabase");
        timer.lapse("Creating index " + index + " from " + sourceDatabase
                + " using " + policyName);

        try {
            documentsDatabaseIndexer.indexUsingPrimaryDatabase(index,
                    sourceDatabase, policyName);
            mbean.incrementRequests();

            return Response.status(RestClient.OK_CREATED).entity(
                    "rebuilt index for " + index + " using primary database\n")
                    .build();
        } catch (Exception e) {
            LOGGER.error("failed to create index " + index, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to create index " + index + "\n").build();
        } finally {
            timer.stop();
        }

    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.WILDCARD })
    @Path("/secondary/{index}")
    @Override
    public Response createIndexUsingSecondaryDatabase(
            @PathParam("index") final String index,
            @QueryParam("policyName") final String policyName,
            @QueryParam("sourceDatabase") final String sourceDatabase,
            @QueryParam("joinDatabase") final String joinDatabase,
            @QueryParam("indexIdInJoinDatabase") final String indexIdInJoinDatabase,
            @QueryParam("sourceIdInJoinDatabase") final String sourceIdInJoinDatabase) {
        if (!ACTIVATE_INDEX) {
            return Response.status(RestClient.SERVICE_UNAVAILABLE).type(
                    "text/plain").entity("Index Service is not available\n")
                    .build();
        }

        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified\n").build();
        }
        if (GenericValidator.isBlankOrNull(sourceDatabase)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("sourceDatabase not specified\n")
                    .build();
        }
        if (GenericValidator.isBlankOrNull(joinDatabase)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("joinDatabase not specified\n")
                    .build();
        }
        if (GenericValidator.isBlankOrNull(indexIdInJoinDatabase)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity(
                    "indexIdInJoinDatabase not specified\n").build();
        }
        if (GenericValidator.isBlankOrNull(sourceIdInJoinDatabase)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity(
                    "sourceIdInJoinDatabase not specified\n").build();
        }

        if (index.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index name is valid " + index + "\n")
                    .build();
        }

        final Timer timer = Metric
                .newTimer("IndexServiceImpl.createIndexUsingSecondaryDatabase");
        timer.lapse("Creating secondary index " + index + " from "
                + sourceDatabase + " using " + policyName);
        try {

            documentsDatabaseIndexer.indexUsingSecondaryDatabase(index,
                    policyName, sourceDatabase, joinDatabase,
                    indexIdInJoinDatabase, sourceIdInJoinDatabase);
            mbean.incrementRequests();

            return Response.status(RestClient.OK_CREATED).entity(
                    "rebuilt index for " + index + " using secondary database "
                            + sourceDatabase + "\n").build();
        } catch (Exception e) {
            LOGGER.error("failed to create index " + index, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to create index " + index + "\n").build();
        } finally {
            timer.stop();
        }
    }

    @POST
    @Produces("application/json")
    @Consumes( { MediaType.WILDCARD })
    @Path("/{index}")
    @Override
    public Response updateIndexUsingPrimaryDatabase(
            @PathParam("index") final String index,
            @PathParam("sourceDatabase") final String sourceDatabase,
            @QueryParam("policyName") final String policyName,
            @FormParam("docs") final String rawDocs) {
        if (!ACTIVATE_INDEX) {
            return Response.status(RestClient.SERVICE_UNAVAILABLE).type(
                    "text/plain").entity("Index Service is not available\n")
                    .build();
        }

        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified\n").build();
        }
        if (index.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index name is valid " + index + "\n")
                    .build();
        }
        if (GenericValidator.isBlankOrNull(rawDocs)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("docs not specified\n").build();
        }

        final Timer timer = Metric
                .newTimer("IndexServiceImpl.updateIndexUsingPrimaryDatabase");
        try {
            JSONArray jsonDocs = new JSONArray(rawDocs);
            final List<Document> docs = new ArrayList<Document>();
            for (int i = 0; i < jsonDocs.length(); i++) {
                JSONObject jsonDoc = jsonDocs.getJSONObject(i);
                final String db = jsonDoc.optString(Document.DATABASE, null);
                if (db == null && sourceDatabase != null) {
                    jsonDoc.put(Document.DATABASE, sourceDatabase);
                }
                final Document doc = Converters.getInstance().getConverter(
                        JSONObject.class, Document.class).convert(jsonDoc);
                docs.add(doc);
            }
            //
            int succeeded = documentsDatabaseIndexer
                    .updateIndexUsingPrimaryDatabase(index, policyName, docs);
            mbean.incrementRequests();

            return Response.ok().entity(
                    "updated " + succeeded + "/" + docs.size()
                            + " documents in index for " + index + "\n")
                    .build();
        } catch (Exception e) {
            LOGGER.error("failed to update index " + index, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to update index " + index + "\n").build();
        } finally {
            timer.stop();
        }

    }

    @DELETE
    @Produces("application/json")
    @Consumes( { MediaType.WILDCARD })
    @Path("/{index}")
    @Override
    public Response removeIndexedDocuments(
            @PathParam("index") final String index,
            @PathParam("sourceDatabase") final String sourceDatabase,
            @QueryParam("secondaryIdName") final String secondaryIdName,
            @QueryParam("primaryAndSecondaryIds") final String rawPrimaryAndSecondaryIds,
            @DefaultValue("0") @QueryParam("olderThanDays") int olderThanDays) {
        if (!ACTIVATE_INDEX) {
            return Response.status(RestClient.SERVICE_UNAVAILABLE).type(
                    "text/plain").entity("Index Service is not available\n")
                    .build();
        }

        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified\n").build();
        }
        if (index.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index name is valid " + index + "\n")
                    .build();
        }

        final Timer timer = Metric
                .newTimer("IndexServiceImpl.updateIndexUsingPrimaryDatabase");
        try {
            JSONArray jsonPrimaryAndSecondaryIds = new JSONArray(
                    rawPrimaryAndSecondaryIds);
            Collection<Pair<String, String>> primaryAndSecondaryIds = new ArrayList<Pair<String, String>>();
            for (int i = 0; i < jsonPrimaryAndSecondaryIds.length(); i++) {
                String next = jsonPrimaryAndSecondaryIds.getString(i);
                if (next.startsWith("[")) {
                    JSONArray jsonPair = new JSONArray(next);
                    primaryAndSecondaryIds.add(new Pair<String, String>(
                            jsonPair.getString(0), jsonPair.getString(1)));
                } else {
                    primaryAndSecondaryIds.add(new Pair<String, String>(next,
                            null));
                }
            }
            //
            documentsDatabaseIndexer.removeIndexedDocuments(index,
                    sourceDatabase, secondaryIdName, primaryAndSecondaryIds,
                    olderThanDays);
            mbean.incrementRequests();

            return Response.ok().entity("removed documents from index\n")
                    .build();
        } catch (Exception e) {
            LOGGER.error("failed to remove documents from index " + index, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to update index " + index + "\n").build();
        } finally {
            timer.stop();
        }

    }

    @PUT
    @Produces("application/json")
    @Consumes( { MediaType.WILDCARD })
    @Path("/primary/{index}")
    @Override
    public Response updateIndexUsingPrimaryDatabaseIDs(
            @PathParam("index") final String index,
            @QueryParam("policyName") final String policyName,
            @QueryParam("docIds") final String docIds) {
        if (!ACTIVATE_INDEX) {
            return Response.status(RestClient.SERVICE_UNAVAILABLE).type(
                    "text/plain").entity("Index Service is not available\n")
                    .build();
        }

        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified\n").build();
        }
        if (index.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index name is valid " + index + "\n")
                    .build();
        }
        if (GenericValidator.isBlankOrNull(docIds)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("docIds not specified\n").build();
        }

        final String[] ids = docIds.split(",");
        for (String id : ids) {
            if (GenericValidator.isBlankOrNull(id)) {
                return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST)
                        .type("text/plain").entity(
                                "empty docId specified in "
                                        + Arrays.asList(docIds) + "\n").build();
            }
        }

        final Timer timer = Metric
                .newTimer("IndexServiceImpl.updateIndexUsingPrimaryDatabase");
        try {
            int succeeded = documentsDatabaseIndexer
                    .updateIndexUsingPrimaryDatabase(index, policyName, ids);
            mbean.incrementRequests();

            return Response.ok().entity(
                    "updated " + succeeded + " documents in index for " + index
                            + " with ids " + docIds + "\n").build();
        } catch (Exception e) {
            LOGGER.error("failed to update index " + index, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to update index " + index + "\n").build();
        } finally {
            timer.stop();
        }

    }

    @PUT
    @Produces("application/json")
    @Consumes( { MediaType.WILDCARD })
    @Path("/secondary/{index}")
    @Override
    public Response updateIndexUsingSecondaryDatabaseIDs(
            @PathParam("index") final String index,
            @QueryParam("policyName") final String policyName,
            @QueryParam("sourceDatabase") final String sourceDatabase,
            @QueryParam("joinDatabase") final String joinDatabase,
            @QueryParam("indexIdInJoinDatabase") final String indexIdInJoinDatabase,
            @QueryParam("sourceIdInJoinDatabase") final String sourceIdInJoinDatabase,
            @QueryParam("docIds") final String docIds) {
        if (!ACTIVATE_INDEX) {
            return Response.status(RestClient.SERVICE_UNAVAILABLE).type(
                    "text/plain").entity("Index Service is not available\n")
                    .build();
        }

        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified\n").build();
        }
        if (index.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index name is valid " + index + "\n")
                    .build();
        }
        if (GenericValidator.isBlankOrNull(sourceDatabase)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("sourceDatabase not specified\n")
                    .build();
        }
        if (GenericValidator.isBlankOrNull(joinDatabase)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("joinDatabase not specified\n")
                    .build();
        }
        if (GenericValidator.isBlankOrNull(indexIdInJoinDatabase)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity(
                    "indexIdInJoinDatabase not specified\n").build();
        }
        if (GenericValidator.isBlankOrNull(sourceIdInJoinDatabase)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity(
                    "sourceIdInJoinDatabase not specified\n").build();
        }

        if (GenericValidator.isBlankOrNull(docIds)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("docIds not specified\n").build();
        }

        final Timer timer = Metric
                .newTimer("IndexServiceImpl.updateIndexUsingSecondaryDatabase");
        final String[] ids = docIds.split(",");
        for (String id : ids) {
            if (GenericValidator.isBlankOrNull(id)) {
                return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST)
                        .type("text/plain").entity(
                                "empty docId specified in "
                                        + Arrays.asList(docIds) + "\n").build();
            }
        }

        try {
            int succeeded = documentsDatabaseIndexer
                    .updateIndexUsingSecondaryDatabase(index, policyName,
                            sourceDatabase, joinDatabase,
                            indexIdInJoinDatabase, sourceIdInJoinDatabase, ids);
            mbean.incrementRequests();

            return Response.ok().entity(
                    "updated " + succeeded + " documents in index for " + index
                            + " with ids " + docIds + "\n").build();
        } catch (Exception e) {
            LOGGER.error("failed to update index " + index, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to update index " + index + "\n").build();
        } finally {
            timer.stop();
        }

    }

    /**
     * @return the documentsDatabaseIndexer
     */
    public DocumentsDatabaseIndexer getDocumentsDatabaseIndexer() {
        return documentsDatabaseIndexer;
    }

    /**
     * @param documentsDatabaseIndexer
     *            the documentsDatabaseIndexer to set
     */
    public void setDocumentsDatabaseIndexer(
            DocumentsDatabaseIndexer documentsDatabaseIndexer) {
        this.documentsDatabaseIndexer = documentsDatabaseIndexer;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (documentsDatabaseIndexer == null) {
            throw new IllegalStateException("documentsDatabaseIndexer not set");
        }
    }

}

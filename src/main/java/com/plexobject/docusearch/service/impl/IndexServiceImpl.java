package com.plexobject.docusearch.service.impl;

import java.util.Arrays;

import javax.ws.rs.Consumes;
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

import com.plexobject.docusearch.docs.DocumentsDatabaseIndexer;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.jmx.JMXRegistrar;
import com.plexobject.docusearch.jmx.impl.ServiceJMXBeanImpl;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.service.IndexService;

@Path("/index")
public class IndexServiceImpl implements IndexService {
    private static final Logger LOGGER = Logger
            .getLogger(IndexServiceImpl.class);

    private final DocumentsDatabaseIndexer documentsDatabaseIndexer;
    private final ServiceJMXBeanImpl mbean;

    public IndexServiceImpl() {
        this(new RepositoryFactory());
    }

    public IndexServiceImpl(final RepositoryFactory repositoryFactory) {
        this(new DocumentsDatabaseIndexer(repositoryFactory));
    }

    public IndexServiceImpl(
            final DocumentsDatabaseIndexer documentsDatabaseIndexer) {

        if (documentsDatabaseIndexer == null) {
            throw new NullPointerException(
                    "documentsDatabaseIndexer not specified");
        }
        this.documentsDatabaseIndexer = documentsDatabaseIndexer;
        mbean = JMXRegistrar.getInstance().register(getClass());
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.WILDCARD })
    @Path("/primary/{index}")
    @Override
    public Response createIndexUsingPrimaryDatabase(
            @PathParam("index") final String index) {
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
        try {
            documentsDatabaseIndexer.indexUsingPrimaryDatabase(index);
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
            @QueryParam("sourceDatabase") final String sourceDatabase,
            @QueryParam("joinDatabase") final String joinDatabase,
            @QueryParam("indexIdInJoinDatabase") final String indexIdInJoinDatabase,
            @QueryParam("sourceIdInJoinDatabase") final String sourceIdInJoinDatabase) {
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
        try {

            documentsDatabaseIndexer.indexUsingSecondaryDatabase(index,
                    sourceDatabase, joinDatabase, indexIdInJoinDatabase,
                    sourceIdInJoinDatabase);
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

    @PUT
    @Produces("application/json")
    @Consumes( { MediaType.WILDCARD })
    @Path("/primary/{index}")
    @Override
    public Response updateIndexUsingPrimaryDatabase(
            @PathParam("index") final String index,
            @QueryParam("docIds") final String docIds) {
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
                    .updateIndexUsingPrimaryDatabase(index, ids);
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
    public Response updateIndexUsingSecondaryDatabase(
            @PathParam("index") final String index,
            @QueryParam("sourceDatabase") final String sourceDatabase,
            @QueryParam("joinDatabase") final String joinDatabase,
            @QueryParam("indexIdInJoinDatabase") final String indexIdInJoinDatabase,
            @QueryParam("sourceIdInJoinDatabase") final String sourceIdInJoinDatabase,
            @QueryParam("docIds") final String docIds) {
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
                    .updateIndexUsingSecondaryDatabase(index, sourceDatabase,
                            joinDatabase, indexIdInJoinDatabase,
                            sourceIdInJoinDatabase, ids);
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
}

package com.plexobject.docusearch.service.impl;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
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
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.jmx.JMXRegistrar;
import com.plexobject.docusearch.jmx.impl.ServiceJMXBeanImpl;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.plexobject.docusearch.service.RepositoryService;
import com.sun.jersey.spi.inject.Inject;

@Path("/storage")
@Component("storageService")
@Scope("singleton")
public class RepositoryServiceImpl implements RepositoryService,
        InitializingBean {
    private static final Logger LOGGER = Logger
            .getLogger(RepositoryServiceImpl.class);
    @Autowired
    @Inject
    DocumentRepository documentRepository;
    private final ServiceJMXBeanImpl mbean;

    public RepositoryServiceImpl() {
        mbean = JMXRegistrar.getInstance().register(getClass());

    }

    @DELETE
    @Produces("application/json")
    @Consumes( { "*/*" })
    @Path("/{database}/{id}")
    @Override
    public Response delete(@PathParam("database") String database,
            @PathParam("database") String id,
            @QueryParam("version") String version) {
        if (GenericValidator.isBlankOrNull(database)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("database not specified").build();
        }
        if (database.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity(
                    "database name is valid " + database + "\n").build();
        }

        if (GenericValidator.isBlankOrNull(id)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("id not specified").build();
        }
        if (GenericValidator.isBlankOrNull(version)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("version not specified").build();
        }
        try {

            documentRepository.deleteDocument(database, id, version);
            mbean.incrementRequests();

            return Response.ok().build();
        } catch (PersistenceException e) {
            LOGGER.error("failed to delete document " + database + "/" + id, e);
            mbean.incrementError();
            final int errorCode = e.getErrorCode() == 0 ? 500 : e
                    .getErrorCode();
            return Response.status(errorCode).type("text/plain").entity(
                    "failed to delete document " + database + "/" + id + "\n")
                    .build();
        } catch (Exception e) {
            LOGGER.error("failed to delete document " + database + "/" + id, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to delete document " + database + "/" + id + "\n")
                    .build();
        }

    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.TEXT_HTML, MediaType.APPLICATION_JSON })
    @Path("{database}/{id}")
    @Override
    public Response get(@PathParam("database") final String database,
            @PathParam("id") final String id) {
        if (GenericValidator.isBlankOrNull(database)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("database not specified").build();
        }
        if (database.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity(
                    "index name is valid " + database + "\n").build();
        }

        if (GenericValidator.isBlankOrNull(id)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("id not specified").build();
        }

        try {

            Document doc = documentRepository.getDocument(database, id);
            JSONObject jsonDoc = Converters.getInstance().getConverter(
                    Object.class, JSONObject.class).convert(doc);
            mbean.incrementRequests();

            return Response.ok(jsonDoc.toString()).build();

        } catch (PersistenceException e) {
            LOGGER.error("failed to find details for " + database + " with "
                    + id, e);
            final int errorCode = e.getErrorCode() == 0 ? 500 : e
                    .getErrorCode();

            return Response.status(errorCode).type("text/plain").entity(
                    "failed to get details for " + database + " with " + id
                            + "\n").build();
        } catch (Exception e) {
            LOGGER.error("failed to get details for " + database + " with "
                    + id, e);
            mbean.incrementError();

            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to get details for " + database + " with " + id
                            + "\n").build();
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.APPLICATION_JSON })
    @Path("/{database}")
    @Override
    public Response post(@PathParam("database") String database, String body) {
        if (GenericValidator.isBlankOrNull(database)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("database not specified").build();
        }
        if (database.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity(
                    "index name is valid " + database + "\n").build();
        }

        if (GenericValidator.isBlankOrNull(body)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("body not specified").build();
        }

        try {
            final JSONObject jsonReq = new JSONObject(body);
            final Document reqDoc = new DocumentBuilder(Converters
                    .getInstance().getConverter(JSONObject.class,
                            Document.class).convert(jsonReq)).setDatabase(
                    database).build();
            final Document savedDoc = documentRepository.saveDocument(reqDoc,
                    false);
            final JSONObject jsonRes = Converters.getInstance().getConverter(
                    Object.class, JSONObject.class).convert(savedDoc);
            mbean.incrementRequests();

            return Response.status(RestClient.OK_CREATED).entity(
                    jsonRes.toString()).build();
        } catch (PersistenceException e) {
            LOGGER.error("failed to save " + body, e);
            mbean.incrementError();
            final int errorCode = e.getErrorCode() == 0 ? 500 : e
                    .getErrorCode();

            return Response.status(errorCode).type("text/plain").entity(
                    "failed to save " + body + "\n").build();
        } catch (Exception e) {
            LOGGER.error("failed to save " + body, e);
            mbean.incrementError();

            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity("failed to save " + body + "\n")
                    .build();
        }
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.APPLICATION_JSON })
    @Path("/{database}/{id}")
    @Override
    public Response put(@PathParam("database") String database,
            @PathParam("id") String id, @QueryParam("version") String version,
            String body) {
        if (GenericValidator.isBlankOrNull(database)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("database not specified").build();
        }
        if (database.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity(
                    "index name is valid " + database + "\n").build();
        }

        if (GenericValidator.isBlankOrNull(id)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("id not specified").build();
        }

        if (GenericValidator.isBlankOrNull(body)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("body not specified").build();
        }

        try {
            final JSONObject jsonReq = new JSONObject(body);
            final Document reqDoc = new DocumentBuilder(Converters
                    .getInstance().getConverter(JSONObject.class,
                            Document.class).convert(jsonReq)).setDatabase(
                    database).setId(id).setRevision(version).build();
            final Document savedDoc = documentRepository.saveDocument(reqDoc,
                    false);
            final JSONObject jsonRes = Converters.getInstance().getConverter(
                    Object.class, JSONObject.class).convert(savedDoc);
            mbean.incrementRequests();

            if (version == null) {
                return Response.status(RestClient.OK_CREATED).entity(
                        jsonRes.toString()).build();
            } else {
                return Response.ok(jsonRes.toString()).build();
            }
        } catch (PersistenceException e) {
            LOGGER.error("failed to save " + body, e);
            mbean.incrementError();
            final int errorCode = e.getErrorCode() == 0 ? 500 : e
                    .getErrorCode();

            return Response.status(errorCode).type("text/plain").entity(
                    "failed to save " + body + "\n").build();
        } catch (Exception e) {
            LOGGER.error("failed to save " + body, e);
            mbean.incrementError();

            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity("failed to save " + body + "\n")
                    .build();
        }
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

    @Override
    public void afterPropertiesSet() throws Exception {
        if (documentRepository == null) {
            throw new IllegalStateException("documentRepository not set");
        }
    }
}

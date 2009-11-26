/**
 * 
 */
package com.plexobject.docusearch.service.impl;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
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
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.jmx.JMXRegistrar;
import com.plexobject.docusearch.jmx.impl.ServiceJMXBeanImpl;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.plexobject.docusearch.query.LookupPolicy;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.service.ConfigurationService;
import com.sun.jersey.spi.inject.Inject;

/**
 * @author Shahzad Bhatti
 * 
 */
@Path("/config")
@Component("configService")
@Scope("singleton")
public class ConfigurationServiceImpl implements ConfigurationService,
        InitializingBean {
    private static final Logger LOGGER = Logger
            .getLogger(ConfigurationServiceImpl.class);
    @Autowired
    @Inject
    ConfigurationRepository configRepository;

    ServiceJMXBeanImpl mbean;

    public ConfigurationServiceImpl() {
        mbean = JMXRegistrar.getInstance().register(getClass());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.plexobject.docusearch.service.ConfigurationService#getIndexPolicy(java.
     * lang.String)
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.WILDCARD })
    @Path("/index/{index}")
    @Override
    public Response getIndexPolicy(@PathParam("index") final String index) {
        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified").build();
        }
        try {
            final IndexPolicy policy = configRepository.getIndexPolicy(index);
            final JSONObject jsonPolicy = Converters.getInstance()
                    .getConverter(IndexPolicy.class, JSONObject.class).convert(
                            policy);
            mbean.incrementRequests();
            return Response.ok(jsonPolicy.toString()).build();
        } catch (PersistenceException e) {
            LOGGER.error("failed to get index policy for " + index, e);
            mbean.incrementError();
            final int errorCode = e.getErrorCode() == 0 ? 500 : e
                    .getErrorCode();
            return Response.status(errorCode).type("text/plain").entity(
                    "failed to get index policy for " + index + "\n").build();
        } catch (Exception e) {
            LOGGER.error("failed to get index policy for " + index, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to get index policy for " + index + "\n").build();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.plexobject.docusearch.service.ConfigurationService#getQueryPolicy(java.
     * lang.String)
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.WILDCARD })
    @Path("/query/{index}")
    @Override
    public Response getQueryPolicy(@PathParam("index") final String index) {
        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified").build();
        }
        try {
            final QueryPolicy policy = configRepository.getQueryPolicy(index);
            final JSONObject jsonPolicy = Converters.getInstance()
                    .getConverter(QueryPolicy.class, JSONObject.class).convert(
                            policy);
            mbean.incrementRequests();

            return Response.ok(jsonPolicy.toString()).build();
        } catch (PersistenceException e) {
            LOGGER.error("failed to get query policy for " + index, e);
            mbean.incrementError();
            final int errorCode = e.getErrorCode() == 0 ? 500 : e
                    .getErrorCode();
            return Response.status(errorCode).type("text/plain").entity(
                    "failed to get index query for " + index + "\n").build();
        } catch (Exception e) {
            LOGGER.error("failed to get query policy for " + index, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to get index query for " + index + "\n").build();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.plexobject.docusearch.service.ConfigurationService#saveIndexPolicy(java
     * .lang.String, java.lang.String)
     */
    @PUT
    @Produces("application/json")
    @Consumes( { MediaType.WILDCARD })
    @Path("/index/{index}")
    @Override
    public Response saveIndexPolicy(@PathParam("index") String index,
            final String jsonPolicy) {
        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified").build();
        }
        if (GenericValidator.isBlankOrNull(jsonPolicy)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("jsonPolicy not specified").build();
        }
        try {
            final IndexPolicy policy = Converters.getInstance().getConverter(
                    JSONObject.class, IndexPolicy.class).convert(
                    new JSONObject(jsonPolicy));
            final IndexPolicy savedPolicy = configRepository.saveIndexPolicy(
                    index, policy);
            final JSONObject jsonRes = Converters.getInstance().getConverter(
                    IndexPolicy.class, JSONObject.class).convert(savedPolicy);
            mbean.incrementRequests();

            return Response.status(RestClient.OK_CREATED).entity(
                    jsonRes.toString()).build();
        } catch (PersistenceException e) {
            LOGGER.error("failed to save index policy for " + jsonPolicy, e);
            mbean.incrementError();
            final int errorCode = e.getErrorCode() == 0 ? 500 : e
                    .getErrorCode();
            return Response.status(errorCode).type("text/plain").entity(
                    "failed to save index policy for " + jsonPolicy + "\n")
                    .build();
        } catch (Exception e) {
            LOGGER.error("failed to get index policy for " + jsonPolicy, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to get index policy for " + jsonPolicy + "\n")
                    .build();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.plexobject.docusearch.service.ConfigurationService#saveQueryPolicy(java
     * .lang.String, java.lang.String)
     */
    @PUT
    @Produces("application/json")
    @Consumes( { MediaType.WILDCARD })
    @Path("/query/{index}")
    @Override
    public Response saveQueryPolicy(@PathParam("index") final String index,
            String jsonPolicy) {
        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified").build();
        }
        if (GenericValidator.isBlankOrNull(jsonPolicy)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("jsonPolicy not specified").build();
        }
        try {
            final QueryPolicy policy = Converters.getInstance().getConverter(
                    JSONObject.class, QueryPolicy.class).convert(
                    new JSONObject(jsonPolicy));
            final QueryPolicy savedPolicy = configRepository.saveQueryPolicy(
                    index, policy);
            final JSONObject jsonRes = Converters.getInstance().getConverter(
                    QueryPolicy.class, JSONObject.class).convert(savedPolicy);
            mbean.incrementRequests();

            return Response.status(RestClient.OK_CREATED).entity(
                    jsonRes.toString()).build();
        } catch (PersistenceException e) {
            LOGGER.error("failed to save query policy for " + jsonPolicy, e);
            mbean.incrementError();
            final int errorCode = e.getErrorCode() == 0 ? 500 : e
                    .getErrorCode();
            return Response.status(errorCode).type("text/plain").entity(
                    "failed to save query policy for " + jsonPolicy + "\n")
                    .build();
        } catch (Exception e) {
            LOGGER.error("failed to get query policy for " + jsonPolicy, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to get query policy for " + jsonPolicy + "\n")
                    .build();
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.WILDCARD })
    @Path("/lookup/{index}")
    @Override
    public Response getLookupPolicy(@PathParam("index") final String index) {
        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified").build();
        }
        try {
            final LookupPolicy policy = configRepository.getLookupPolicy(index);
            final JSONObject jsonPolicy = Converters.getInstance()
                    .getConverter(LookupPolicy.class, JSONObject.class)
                    .convert(policy);
            mbean.incrementRequests();

            return Response.ok(jsonPolicy.toString()).build();
        } catch (PersistenceException e) {
            LOGGER.error("failed to get lookup policy for " + index, e);
            mbean.incrementError();
            final int errorCode = e.getErrorCode() == 0 ? 500 : e
                    .getErrorCode();
            return Response.status(errorCode).type("text/plain").entity(
                    "failed to get lookup policy for " + index + "\n").build();
        } catch (Exception e) {
            LOGGER.error("failed to get lookup policy for " + index, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to get lookup policy for " + index + "\n").build();
        }
    }

    @PUT
    @Produces("application/json")
    @Consumes( { MediaType.WILDCARD })
    @Path("/lookup/{index}")
    @Override
    public Response saveLookupPolicy(@PathParam("index") final String index,
            String jsonPolicy) {
        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified").build();
        }
        if (GenericValidator.isBlankOrNull(jsonPolicy)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("jsonPolicy not specified").build();
        }
        try {
            final LookupPolicy policy = Converters.getInstance().getConverter(
                    JSONObject.class, LookupPolicy.class).convert(
                    new JSONObject(jsonPolicy));
            final LookupPolicy savedPolicy = configRepository.saveLookupPolicy(
                    index, policy);
            final JSONObject jsonRes = Converters.getInstance().getConverter(
                    LookupPolicy.class, JSONObject.class).convert(savedPolicy);
            mbean.incrementRequests();

            return Response.status(RestClient.OK_CREATED).entity(
                    jsonRes.toString()).build();
        } catch (PersistenceException e) {
            LOGGER.error("failed to save lookup policy for " + jsonPolicy, e);
            mbean.incrementError();
            final int errorCode = e.getErrorCode() == 0 ? 500 : e
                    .getErrorCode();
            return Response.status(errorCode).type("text/plain").entity(
                    "failed to save lookup policy for " + jsonPolicy + "\n")
                    .build();
        } catch (Exception e) {
            LOGGER.error("failed to get lookup policy for " + jsonPolicy, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to get query lookup for " + jsonPolicy + "\n")
                    .build();
        }
    }

    /**
     * @return the configRepository
     */
    public ConfigurationRepository getConfigRepository() {
        return configRepository;
    }

    /**
     * @param configRepository
     *            the configRepository to set
     */
    public void setConfigRepository(ConfigurationRepository configRepository) {
        this.configRepository = configRepository;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (configRepository == null) {
            throw new IllegalStateException("configRepository not set");
        }

    }
}

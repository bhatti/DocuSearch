package com.plexobject.docusearch.service.impl;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
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

import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.jmx.JMXRegistrar;
import com.plexobject.docusearch.jmx.impl.ServiceJMXBeanImpl;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.query.CriteriaBuilder;
import com.plexobject.docusearch.query.LookupPolicy;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.RankedTerm;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.SearchDocList;
import com.plexobject.docusearch.query.lucene.QueryImpl;
import com.plexobject.docusearch.service.SearchService;
import com.plexobject.docusearch.util.SptialLookup;
import com.sun.jersey.spi.inject.Inject;

@Path("/search")
@Component("searchService")
@Scope("singleton")
public class SearchServiceImpl implements SearchService, InitializingBean {
    private static final Logger LOGGER = Logger
            .getLogger(SearchServiceImpl.class);
    private final Map<File, Query> cachedQueries = new HashMap<File, Query>();

    @Autowired
    @Inject
    ConfigurationRepository configRepository;

    @Autowired
    @Inject
    DocumentRepository documentRepository;

    @Autowired
    @Inject
    SptialLookup sptialLookup;

    private final ServiceJMXBeanImpl mbean;

    public SearchServiceImpl() {
        mbean = JMXRegistrar.getInstance().register(getClass());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.TEXT_HTML, MediaType.APPLICATION_JSON })
    @Path("{index}")
    @Override
    public Response query(@PathParam("index") final String index,
            @QueryParam("owner") final String owner,
            @QueryParam("keywords") final String keywords,
            @QueryParam("zipCode") final String zipCode,
            @QueryParam("radius") final String radius,
            @QueryParam("suggestions") final boolean includeSuggestions,
            @QueryParam("start") final int start,
            @QueryParam("limit") final int limit,
            @QueryParam("detailedResults") final boolean detailedResults) {
        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified").build();
        }
        if (index.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index name is valid " + index + "\n")
                    .build();
        }

        if (GenericValidator.isBlankOrNull(keywords)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("keywrods not specified").build();
        }
        final Timer timer = Metric.newTimer("SearchServiceImpl.query");

        try {
            IndexPolicy indexPolicy = configRepository.getIndexPolicy(index);

            QueryPolicy queryPolicy = configRepository.getQueryPolicy(index);
            final CriteriaBuilder criteriaBuilder = new CriteriaBuilder()
                    .setKeywords(keywords).setOwner(owner);
            if (zipCode != null && zipCode.length() > 0) {
                criteriaBuilder.setZipcode(zipCode);
                double[] latLongs = sptialLookup
                        .getLatitudeAndLongitude(zipCode);
                criteriaBuilder.setLatitude(latLongs[0]);
                criteriaBuilder.setLongitude(latLongs[1]);
            }
            if (radius != null && radius.length() > 0) {
                criteriaBuilder.setRadius(Double.valueOf(radius));
            }
            final QueryCriteria criteria = criteriaBuilder.build();

            final File dir = new File(LuceneUtils.INDEX_DIR, index);

            Query query = newQueryImpl(dir);

            SearchDocList results = query.search(criteria, indexPolicy,
                    queryPolicy, includeSuggestions, start, limit);

            JSONArray docs = docsToJson(index, detailedResults, results);
            JSONArray similar = new JSONArray();
            if (results.getSimilarWords() != null) {
                for (String word : results.getSimilarWords()) {
                    similar.put(word);
                }
            }
            final JSONObject response = new JSONObject();
            response.put("suggestions", similar);
            response.put("keywords", keywords);
            response.put("start", start);
            response.put("limit", limit);
            response.put("totalHits", results.getTotalHits());
            response.put("docs", docs);

            timer.stop("Found " + results.getTotalHits() + " hits for "
                    + keywords + " on index " + index + ", detailed "
                    + detailedResults + ", start " + start + ", limit " + limit
                    + ", suggestions " + includeSuggestions);
            mbean.incrementRequests();

            return Response.ok(response.toString()).build();

        } catch (Exception e) {
            LOGGER.error("failed to query " + index + " with " + keywords
                    + " from " + start + "/" + limit, e);
            mbean.incrementError();

            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to query " + index + " with " + keywords + " from "
                            + start + "/" + limit + "\n").build();
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.TEXT_HTML, MediaType.APPLICATION_JSON })
    @Path("/autocomplete/{index}")
    @Override
    public Response autocomplete(@PathParam("index") final String index,
            @QueryParam("q") final String keywords,
            @QueryParam("limit") final int limit) {
        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified").build();
        }
        if (index.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index name is valid " + index + "\n")
                    .build();
        }

        if (GenericValidator.isBlankOrNull(keywords)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("keywrods not specified").build();
        }
        final Timer timer = Metric.newTimer("SearchServiceImpl.query");

        try {
            IndexPolicy indexPolicy = configRepository.getIndexPolicy(index);

            LookupPolicy lookupPolicy = configRepository.getLookupPolicy(index);
            final QueryCriteria criteria = new CriteriaBuilder().setKeywords(
                    keywords.trim() + "*").build();

            final File dir = new File(LuceneUtils.INDEX_DIR, index);

            Query query = newQueryImpl(dir);

            List<String> results = query.partialLookup(criteria, indexPolicy,
                    lookupPolicy, limit);
            StringBuilder response = new StringBuilder();
            for (String word : results) {
                response.append(word + "\r\n");
            }
            timer.stop("Found " + results + " hits for " + keywords
                    + " on index " + index + ", limit " + limit);
            mbean.incrementRequests();

            return Response.ok(response.toString()).build();

        } catch (Exception e) {
            LOGGER.error("failed to autocomplete " + index + " with "
                    + keywords + " limit " + limit, e);
            mbean.incrementError();

            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to autocomplete " + index + " with '" + keywords
                            + "' with limit " + limit + "\n").build();
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.TEXT_HTML, MediaType.APPLICATION_JSON })
    @Path("/similar/{index}")
    @Override
    public Response moreLikeThis(@PathParam("index") final String index,
            @QueryParam("externalId") final String externalId,
            @QueryParam("luceneId") final int luceneId,
            @QueryParam("start") final int start,
            @QueryParam("limit") final int limit,
            @QueryParam("detailedResults") final boolean detailedResults) {
        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified").build();
        }
        if (index.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index name is valid " + index + "\n")
                    .build();
        }

        final Timer timer = Metric.newTimer("SearchServiceImpl.query");

        try {
            IndexPolicy indexPolicy = configRepository.getIndexPolicy(index);

            QueryPolicy queryPolicy = configRepository.getQueryPolicy(index);

            final File dir = new File(LuceneUtils.INDEX_DIR, index);

            Query query = newQueryImpl(dir);

            SearchDocList results = query.moreLikeThis(externalId, luceneId,
                    indexPolicy, queryPolicy, start, limit);
            JSONArray docs = docsToJson(index, detailedResults, results);
            final JSONObject response = new JSONObject();
            response.put("externalId", externalId);

            response.put("luceneId", luceneId);
            response.put("start", start);
            response.put("limit", limit);
            response.put("totalHits", results.getTotalHits());
            response.put("docs", docs);

            timer
                    .stop("Found " + results.getTotalHits() + " hits for "
                            + luceneId + " on index " + index + ", detailed "
                            + detailedResults + ", start " + start + ", limit "
                            + limit);
            mbean.incrementRequests();

            return Response.ok(response.toString()).build();

        } catch (Exception e) {
            LOGGER.error("failed to find similar documents for " + index
                    + " with " + luceneId + " from " + start + "/" + limit, e);
            mbean.incrementError();

            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to find similar documents for " + index + " with "
                            + luceneId + " from " + start + "/" + limit + "\n")
                    .build();
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.TEXT_HTML, MediaType.APPLICATION_JSON })
    @Path("/explain/{index}")
    @Override
    public Response explain(@PathParam("index") final String index,
            @QueryParam("owner") final String owner,
            @QueryParam("keywords") final String keywords,
            @QueryParam("zipCode") final String zipCode,
            @QueryParam("radius") final String radius,
            @QueryParam("start") final int start,
            @QueryParam("limit") final int limit) {
        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified").build();
        }
        if (index.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index name is valid " + index + "\n")
                    .build();
        }

        if (GenericValidator.isBlankOrNull(keywords)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("keywrods not specified").build();
        }
        final Timer timer = Metric.newTimer("SearchServiceImpl.explain");

        try {
            IndexPolicy indexPolicy = configRepository.getIndexPolicy(index);

            QueryPolicy queryPolicy = configRepository.getQueryPolicy(index);

            final CriteriaBuilder criteriaBuilder = new CriteriaBuilder()
                    .setKeywords(keywords).setOwner(owner);
            if (zipCode != null && zipCode.length() > 0) {
                criteriaBuilder.setZipcode(zipCode);
                double[] latLongs = sptialLookup
                        .getLatitudeAndLongitude(zipCode);
                criteriaBuilder.setLatitude(latLongs[0]);
                criteriaBuilder.setLongitude(latLongs[1]);
            }
            if (radius != null && radius.length() > 0) {
                criteriaBuilder.setRadius(Double.valueOf(radius));
            }
            final QueryCriteria criteria = criteriaBuilder.build();

            final File dir = new File(LuceneUtils.INDEX_DIR, index);

            final Query query = newQueryImpl(dir);

            final Collection<String> results = query.explain(criteria,
                    indexPolicy, queryPolicy, start, limit);
            final JSONArray response = new JSONArray();
            for (String result : results) {
                response.put(result);
            }

            timer.stop("Explanationfor " + keywords + " on index " + index
                    + ", start " + start + ", limit " + limit);
            mbean.incrementRequests();

            return Response.ok(response.toString()).build();

        } catch (Exception e) {
            LOGGER.error("failed to query " + index + " with " + keywords
                    + " from " + start + "/" + limit, e);
            mbean.incrementError();

            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to query " + index + " with " + keywords + " from "
                            + start + "/" + limit + "\n").build();
        }
    }

    /**
     * 
     * @param index
     * @param numTerms
     * @return JSONArray with top ranking terms
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.TEXT_HTML, MediaType.APPLICATION_JSON })
    @Path("/rank/{index}")
    @Override
    public Response getTopRankingTerms(@PathParam("index") final String index,
            @QueryParam("limit") final int numTerms) {
        if (GenericValidator.isBlankOrNull(index)) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index not specified").build();
        }
        if (index.contains("\"")) {
            return Response.status(RestClient.CLIENT_ERROR_BAD_REQUEST).type(
                    "text/plain").entity("index name is valid " + index + "\n")
                    .build();
        }

        final Timer timer = Metric.newTimer("SearchServiceImpl.ranking");

        try {

            QueryPolicy policy = configRepository.getQueryPolicy(index);

            final File dir = new File(LuceneUtils.INDEX_DIR, index);

            Query query = newQueryImpl(dir);

            Collection<RankedTerm> results = query.getTopRankingTerms(policy,
                    numTerms);
            JSONArray response = new JSONArray();
            for (RankedTerm result : results) {
                JSONObject jsonDoc = Converters.getInstance().getConverter(
                        RankedTerm.class, JSONObject.class).convert(result);
                response.put(jsonDoc);
            }

            timer.stop();
            mbean.incrementRequests();

            return Response.ok(response.toString()).build();

        } catch (Exception e) {
            LOGGER.error("failed to get top ranks for " + index, e);
            mbean.incrementError();
            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to top rank for " + index + "\n").build();
        }
    }

    protected synchronized Query newQueryImpl(final File dir) {
        Query query = cachedQueries.get(dir);
        if (query == null) {
            query = new QueryImpl(dir);
            cachedQueries.put(dir, query);
        }
        return query;
    }

    private JSONArray docsToJson(final String index,
            final boolean detailedResults, SearchDocList results) {
        JSONArray docs = new JSONArray();
        for (SearchDoc result : results) {
            if (detailedResults) {
                Document doc = documentRepository.getDocument(index, result
                        .getId());
                JSONObject jsonDoc = Converters.getInstance().getConverter(
                        Object.class, JSONObject.class).convert(doc);
                docs.put(jsonDoc);
            } else {
                JSONObject jsonDoc = Converters.getInstance().getConverter(
                        Object.class, JSONObject.class).convert(result);
                docs.put(jsonDoc);
            }
        }
        return docs;
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

    /**
     * @return the sptialLookup
     */
    public SptialLookup getSptialLookup() {
        return sptialLookup;
    }

    /**
     * @param sptialLookup
     *            the sptialLookup to set
     */
    public void setSptialLookup(SptialLookup sptialLookup) {
        this.sptialLookup = sptialLookup;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (configRepository == null) {
            throw new IllegalStateException("configRepository not set");
        }
        if (documentRepository == null) {
            throw new IllegalStateException("documentRepository not set");
        }
        if (sptialLookup == null) {
            throw new IllegalStateException("sptialLookup not set");
        }

    }
}

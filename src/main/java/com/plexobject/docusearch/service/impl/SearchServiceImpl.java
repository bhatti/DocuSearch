package com.plexobject.docusearch.service.impl;

import java.io.File;
import java.util.Collection;
import java.util.List;

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

import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.jmx.JMXRegistrar;
import com.plexobject.docusearch.jmx.impl.ServiceJMXBeanImpl;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.query.LookupPolicy;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.RankedTerm;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.SearchDocList;
import com.plexobject.docusearch.query.lucene.QueryImpl;
import com.plexobject.docusearch.service.SearchService;

@Path("/search")
public class SearchServiceImpl implements SearchService {
    private static final Logger LOGGER = Logger
            .getLogger(SearchServiceImpl.class);
    private final ConfigurationRepository configRepository;
    private final DocumentRepository docRepository;
    private final ServiceJMXBeanImpl mbean;

    public SearchServiceImpl() {
        this(new RepositoryFactory());
    }

    public SearchServiceImpl(final RepositoryFactory repositoryFactory) {
        this.docRepository = repositoryFactory.getDocumentRepository();
        this.configRepository = repositoryFactory.getConfigurationRepository();
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
            QueryPolicy policy = configRepository.getQueryPolicy(index);
            final QueryCriteria criteria = new QueryCriteria()
                    .setKeywords(keywords).setOwner(owner);

            final File dir = new File(LuceneUtils.INDEX_DIR, index);

            Query query = newQueryImpl(dir);

            SearchDocList results = query.search(criteria, policy,
                    includeSuggestions, start, limit);

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
                    + ", suggestions " + includeSuggestions + "-->" + response);
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

            LookupPolicy policy = configRepository.getLookupPolicy(index);
            final QueryCriteria criteria = new QueryCriteria()
                    .setKeywords(keywords);

            final File dir = new File(LuceneUtils.INDEX_DIR, index);

            Query query = newQueryImpl(dir);

            List<String> results = query.partialLookup(criteria, policy, limit);
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

            QueryPolicy policy = configRepository.getQueryPolicy(index);

            final File dir = new File(LuceneUtils.INDEX_DIR, index);

            Query query = newQueryImpl(dir);

            SearchDocList results = query.moreLikeThis(externalId, luceneId,
                    policy, start, limit);
            JSONArray docs = docsToJson(index, detailedResults, results);
            final JSONObject response = new JSONObject();
            response.put("externalId", externalId);

            response.put("luceneId", luceneId);
            response.put("start", start);
            response.put("limit", limit);
            response.put("totalHits", results.getTotalHits());
            response.put("docs", docs);

            timer.stop("Found " + results.getTotalHits() + " hits for "
                    + luceneId + " on index " + index + ", detailed "
                    + detailedResults + ", start " + start + ", limit " + limit
                    + ", json " + response);
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
            @QueryParam("keywords") final String keywords,
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

            QueryPolicy policy = configRepository.getQueryPolicy(index);
            final QueryCriteria criteria = new QueryCriteria()
                    .setKeywords(keywords);

            final File dir = new File(LuceneUtils.INDEX_DIR, index);

            final Query query = newQueryImpl(dir);

            final Collection<String> results = query.explain(criteria, policy,
                    start, limit);
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

    protected Query newQueryImpl(final File dir) {
        return new QueryImpl(dir);
    }

    private JSONArray docsToJson(final String index,
            final boolean detailedResults, SearchDocList results) {
        JSONArray docs = new JSONArray();
        for (SearchDoc result : results) {
            if (detailedResults) {
                Document doc = docRepository.getDocument(index, result.getId());
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

}

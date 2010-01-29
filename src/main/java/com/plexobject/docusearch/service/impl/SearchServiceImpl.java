package com.plexobject.docusearch.service.impl;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.validator.GenericValidator;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.query.CriteriaBuilder;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.SearchDocList;
import com.plexobject.docusearch.service.SearchService;

@Path("/search")
@Component("searchService")
@Scope("singleton")
public class SearchServiceImpl extends BaseSearchServiceImpl implements
        SearchService {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.TEXT_HTML, MediaType.APPLICATION_JSON })
    @Path("{index}")
    @Override
    public Response query(
            @PathParam("index") final String index,
            @QueryParam("owner") final String owner,
            @QueryParam("q") final String keywords,
            @QueryParam("zipCode") final String zipCode,
            @QueryParam("city") final String city,
            @QueryParam("state") final String state,
            @QueryParam("country") final String country,
            @QueryParam("region") final String region,
            @DefaultValue("50") @QueryParam("radius") final float radius,
            @QueryParam("sort") final String sortBy,
            @DefaultValue("true") @QueryParam("asc") final boolean sortAscending,
            @DefaultValue("false") @QueryParam("suggestions") final boolean includeSuggestions,
            @DefaultValue("0") @QueryParam("start") final int start,
            @DefaultValue("20") @QueryParam("limit") final int limit,
            @DefaultValue("false") @QueryParam("detailedResults") final boolean detailedResults) {
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
            if (!GenericValidator.isBlankOrNull(zipCode)) {
                criteriaBuilder.setZipcode(zipCode);
                double[] latLongs = spatialLookup
                        .getLatitudeAndLongitude(zipCode);
                criteriaBuilder.setLatitude(latLongs[0]);
                criteriaBuilder.setLongitude(latLongs[1]);
            }
            criteriaBuilder.setCity(city);
            criteriaBuilder.setState(state);
            criteriaBuilder.setCountry(country);
            criteriaBuilder.setRegion(region);
            criteriaBuilder.setRadius(radius);
            criteriaBuilder.setSortBy(sortBy, sortAscending);

            final QueryCriteria criteria = criteriaBuilder.build();

            final File dir = new File(LuceneUtils.INDEX_DIR, index);

            Query query = getQueryImpl(dir);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Searching " + criteria + " using " + queryPolicy);
            }
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
            response.put("q", keywords);
            response.put("start", start);
            response.put("limit", limit);
            response.put("links", createQueryLinks(index, keywords, zipCode,
                    includeSuggestions, limit, detailedResults, results));
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
                            + start + "/" + limit + " due to " + e + "\n")
                    .build();
        }
    }

    private JSONArray createQueryLinks(final String index,
            final String keywords, final String zipCode,
            final boolean includeSuggestions, final int limit,
            final boolean detailedResults, SearchDocList results)
            throws UnsupportedEncodingException {
        final String baseUrl = uriInfo != null ? uriInfo.getAbsolutePath()
                .toString() : "/search/" + index;
        JSONArray links = new JSONArray();
        final int max = results.getTotalHits() % limit == 0 ? results
                .getTotalHits()
                / limit : (results.getTotalHits() / limit) + 1;
        for (int i = 0; i < max; i += limit) {
            links.put(baseUrl + "?q=" + URLEncoder.encode(keywords, "UTF8")
                    + "&start=" + i + "&limit" + limit + "&zipCode=" + zipCode
                    + "&suggestions=" + includeSuggestions
                    + "&detailedResults=" + detailedResults);
        }
        return links;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes( { MediaType.TEXT_HTML, MediaType.APPLICATION_JSON })
    @Path("/similar/{index}")
    @Override
    public Response moreLikeThis(
            @PathParam("index") final String index,
            @QueryParam("externalId") final String externalId,
            @QueryParam("luceneId") final int luceneId,
            @DefaultValue("0") @QueryParam("start") final int start,
            @DefaultValue("20") @QueryParam("limit") final int limit,
            @DefaultValue("false") @QueryParam("detailedResults") final boolean detailedResults) {
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

            Query query = getQueryImpl(dir);

            SearchDocList results = query.moreLikeThis(externalId, luceneId,
                    indexPolicy, queryPolicy, start, limit);
            JSONArray docs = docsToJson(index, detailedResults, results);
            final JSONObject response = new JSONObject();
            response.put("externalId", externalId);

            response.put("luceneId", luceneId);
            response.put("start", start);
            response.put("limit", limit);
            response.put("totalHits", results.getTotalHits());
            response.put("links", createSimilarLinks(index, externalId,
                    luceneId, limit, detailedResults, results));
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

    private JSONArray createSimilarLinks(final String index,
            final String externalId,
            @QueryParam("luceneId") final int luceneId, final int limit,
            final boolean detailedResults, SearchDocList results) {
        JSONArray links = new JSONArray();
        final String baseUrl = uriInfo != null ? uriInfo.getAbsolutePath()
                .toString() : "/similar/" + index;
        final int max = results.getTotalHits() % limit == 0 ? results
                .getTotalHits()
                / limit : (results.getTotalHits() / limit) + 1;
        for (int i = 0; i < max; i += limit) {
            links.put(baseUrl + "?start=" + i + "&limit" + limit + "&luceneId="
                    + luceneId + "&externalId=" + externalId
                    + "&detailedResults=" + detailedResults);
        }
        return links;
    }
}

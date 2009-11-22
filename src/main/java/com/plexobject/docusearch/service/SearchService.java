package com.plexobject.docusearch.service;

import javax.ws.rs.core.Response;

/**
 * 
 * @author Shahzad Bhatti
 * 
 */
public interface SearchService {
    /**
     * This method queries the index with keywords and returns JSONObject for
     * SearchDocList.
     * 
     * @param index
     * @param keywords
     * @param includeSuggestions
     *            - include suggestions for similar keywords
     * @param start
     * @param limit
     * @param details
     *            - send detailed results
     * @return JSONObject for SearchDocList
     */
    Response query(String index, String owner, String keywords,
            boolean includeSuggestions, int start, int limit,
            boolean detailedResults);

    /**
     * This method queries the index with keywords and returns JSONObject for
     * SearchDocList.
     * 
     * @param index
     * @param keywords
     * @param limit
     * @return JSONObject for SearchDocList
     */
    Response autocomplete(String index, String keywords, int limit);

    /**
     * This method finds similar results for given document id
     * 
     * @param index
     * @param externalId
     * @param luceneId
     * @param start
     * @param limit
     * @param details
     *            - send detailed results
     * @return JSONObject for SearchDocList
     */
    Response moreLikeThis(String index, String externalId, int luceneId,
            int start, int limit, boolean detailedResults);

    /**
     * This method collection of explanations for query results.
     * 
     * @param index
     * @param keywords
     * @param start
     * @param limit
     * @return JSONArray for explanations
     */
    Response explain(String index, String keywords, int start, int limit);

    /**
     * 
     * @param index
     * @param numTerms
     * @return JSONArray with top ranking terms
     */
    Response getTopRankingTerms(String index, int numTerms);
}

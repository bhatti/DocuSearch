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
     * @param owner
     * @param keywords
     * @param zipCode
     * @param city
     * @param state
     * @param country
     * @param region
     * @param radius
     *            - for spatial search
     * @param sortBy
     * @param ascending
     * @param includeSuggestions
     *            - include suggestions for similar keywords
     * @param start
     * @param limit
     * @param details
     *            - send detailed results
     * @return JSONObject for SearchDocList
     */
    Response query(String index, String owner, String keywords, String zipCode,
            String city, String state, String country, String region,
            float radius, String sortBy, boolean sortAscending,
            boolean includeSuggestions, int start, int limit,
            boolean detailedResults);

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
}

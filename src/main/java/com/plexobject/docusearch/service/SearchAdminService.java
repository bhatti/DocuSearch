package com.plexobject.docusearch.service;

import javax.ws.rs.core.Response;

public interface SearchAdminService {
    /**
     * This method collection of explanations for query results.
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
     * @param start
     * @param limit
     * @return JSONArray for explanations
     */
    Response explainSearch(String index, String owner, String keywords,
            String zipCode, String city, String state, String country,
            String region, float radius, String sortBy, boolean sortAscending,
            int start, int limit);

    /**
     * 
     * @param index
     * @param numTerms
     * @return JSONArray with top ranking terms
     */
    Response getTopRankingTerms(String index, int numTerms);
}

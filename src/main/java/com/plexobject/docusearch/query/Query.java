package com.plexobject.docusearch.query;

import java.util.Collection;
import java.util.List;

/**
 * This interface searches index for the given keywords
 * 
 * @author Shahzad Bhatti
 * 
 */
public interface Query {
    /**
     * 
     * @param criteria
     *            - search criteria to search
     * @param policy
     *            - query policy
     * @param includeSuggestions
     *            - whether to include suggestions
     * @param start
     *            - start index for pagination
     * @param limit
     *            - max # of results
     * @return collection of maps, where each map stores information about the
     *         document fields.
     */
    SearchDocList search(QueryCriteria criteria, QueryPolicy policy,
            boolean includeSuggestions, int start, int limit);

    /**
     * 
     * @param criteria
     *            - criteria with partial keywords for lookup
     * @param policy
     *            - query policy
     * @param limit
     *            - max # of results
     * @return List of matches document fields.
     */
    List<String> partialLookup(QueryCriteria criteria, LookupPolicy policy,
            int limit);

    /**
     * 
     * @param externalId
     *            - external document id to compare and dedup
     * @param luceneId
     *            - internal document id (Lucene) to compare
     * @param policy
     *            - query policy
     * @param start
     *            - start index for pagination
     * @param limit
     *            - max # of results
     * @return collection of maps, where each map stores information about the
     *         document fields.
     */
    SearchDocList moreLikeThis(String externalId, int luceneId,
            QueryPolicy policy, int start, int limit);

    /**
     * 
     * @param criteria
     *            - search criteria to search
     * @param policy
     *            - query policy
     * @param start
     *            - start index for pagination
     * @param limit
     *            - max # of results
     * @return collection of explanations for query results.
     */
    Collection<String> explain(QueryCriteria criteria, QueryPolicy policy,
            int start, int limit);

    /**
     * @param policy
     *            - query policy
     * @param max
     *            - number of terms
     * @return top ranking terms
     */
    Collection<RankedTerm> getTopRankingTerms(QueryPolicy policy, int max);
}

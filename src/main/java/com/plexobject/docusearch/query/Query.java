package com.plexobject.docusearch.query;

import java.util.Collection;
import java.util.List;

import com.plexobject.docusearch.index.IndexPolicy;

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
     * @param indexPolicy
     *            - index policy
     * @param queryPolicy
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
    SearchDocList search(QueryCriteria criteria, IndexPolicy indexPolicy,
            QueryPolicy queryPolicy, boolean includeSuggestions, int start,
            int limit);

    /**
     * 
     * @param criteria
     *            - criteria with partial keywords for lookup
     * @param indexPolicy
     *            - index policy
     * @param queryPolicy
     *            - query policy
     * @param limit
     *            - max # of results
     * @return List of matches document fields.
     */
    List<String> partialLookup(QueryCriteria criteria, IndexPolicy indexPolicy,
            LookupPolicy policy, int limit);

    /**
     * 
     * @param externalId
     *            - external document id to compare and dedup
     * @param luceneId
     *            - internal document id (Lucene) to compare
     * @param indexPolicy
     *            - index policy
     * @param queryPolicy
     *            - query policy
     * @param start
     *            - start index for pagination
     * @param limit
     *            - max # of results
     * @return collection of maps, where each map stores information about the
     *         document fields.
     */
    SearchDocList moreLikeThis(String externalId, int luceneId,
            IndexPolicy indexPolicy, QueryPolicy queryPolicy, int start,
            int limit);

    /**
     * 
     * @param criteria
     *            - search criteria to search
     * @param indexPolicy
     *            - index policy
     * @param queryPolicy
     *            - query policy
     * @param start
     *            - start index for pagination
     * @param limit
     *            - max # of results
     * @return collection of explanations for query results.
     */
    Collection<String> explainSearch(QueryCriteria criteria,
            IndexPolicy indexPolicy, QueryPolicy queryPolicy, int start,
            int limit);

    /**
     * @param policy
     *            - query policy
     * @param max
     *            - number of terms
     * @return top ranking terms
     */
    Collection<RankedTerm> getTopRankingTerms(QueryPolicy policy, int max);

    void close();
}

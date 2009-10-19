package com.plexobject.docusearch.query;

/**
 * This interface searches index for the given keywords
 * 
 * @author bhatti@plexobject.com
 * 
 */
public interface Query {
	/**
	 * 
	 * @param criteria
	 *            - search criteria to search
	 * @param start
	 *            - start index for pagination
	 * @param limit
	 *            - max # of results
	 * @return collection of maps, where each map stores information about the
	 *         document fields.
	 */
	SearchDocList search(QueryCriteria criteria, int start, int limit);
}

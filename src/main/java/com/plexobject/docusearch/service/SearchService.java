package com.plexobject.docusearch.service;

import javax.ws.rs.core.Response;

/**
 * 
 * @author bhatti@plexobject.com
 * 
 */
public interface SearchService {
	/**
	 * This method queries the index with keywords and returns JSONObject for
	 * SearchDocList.
	 * 
	 * @param index
	 * @param keywords
	 * @param start
	 * @param limit
	 * @param details
	 *            - send detailed results
	 * @return JSONObject for SearchDocList
	 */
	Response query(String index, String keywords, int start, int limit,
			boolean detailedResults);

	/**
	 * This method fetches detailed document information
	 * 
	 * @param index
	 * @param id
	 * @return
	 */
	Response get(String index, String id);
}

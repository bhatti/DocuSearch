package com.plexobject.docusearch.service;

import javax.ws.rs.core.Response;

/**
 * 
 * @author bhatti@plexobject.com
 * 
 */
public interface IndexService {
	/**
	 * This method rebuilds the index
	 * 
	 * @param index
	 */
	public Response create(String index);

	/**
	 * This method adds new documents to the index where documents are first
	 * fetched from the database
	 * 
	 * @param index
	 * @param docsAndPolicies
	 *            - this object contains URL-ids of documents along with the
	 *            policy
	 */
	public Response update(String index, String docIds);
}

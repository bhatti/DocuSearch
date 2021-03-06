package com.plexobject.docusearch.service;

import javax.ws.rs.core.Response;

/**
 * 
 * @author Shahzad Bhatti
 * 
 */
public interface IndexService {
    /**
     * This method builds the index using the database with same name
     * 
     * @param index
     */
    public Response createIndexUsingPrimaryDatabase(String index,
            String sourceDatabase, String policyName);

    /**
     * This method builds the index using secondary database and a join table
     * 
     * @param index
     * @param sourceDatabase
     *            - secondary database
     * @param joinDatabase
     *            - join table
     * @param indexIdInJoinDatabase
     *            - id column to match in the index
     * @param sourceIdInJoinDatabase
     *            - id column to match in secondary database
     */
    public Response createIndexUsingSecondaryDatabase(String index,
            String policyName, String sourceDatabase, String joinDatabase,
            String indexIdInJoinDatabase, String sourceIdInJoinDatabase);

    /**
     * This method adds new documents to the index where documents are first
     * fetched from the database
     * 
     * @param index
     * @param docsAndPolicies
     *            - this object contains URL-ids of documents along with the
     *            policy
     */
    public Response updateIndexUsingPrimaryDatabase(String index,
            String sourceDatabase, String policyName, String docs);

    /**
     * This method removes documents with matching ids or indexDate
     */
    public Response removeIndexedDocuments(String index, String sourceDatabase,
            String secondaryIdName, String primaryAndSecondaryIds,
            int olderThanDays);

    /**
     * This method adds new documents to the index where documents are first
     * fetched from the database
     * 
     * @param index
     * @param sourceDatabase
     * @param policyName
     * @param docIds
     */
    public Response updateIndexUsingPrimaryDatabaseIDs(String index,
            String policyName, String docIds);

    /**
     * This method builds the index using secondary database and a join table
     * 
     * @param index
     * @param sourceDatabase
     *            - secondary database
     * @param joinDatabase
     *            - join table
     * @param indexIdInJoinDatabase
     *            - id column to match in the index
     * @param sourceIdInJoinDatabase
     *            - id column to match in secondary database
     * @param docIds
     *            - doc ids in the secondary database
     */
    public Response updateIndexUsingSecondaryDatabaseIDs(String index,
            String policyName, String sourceDatabase, String joinDatabase,
            String indexIdInJoinDatabase, String sourceIdInJoinDatabase,
            String docIds);
}

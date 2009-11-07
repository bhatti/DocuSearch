package com.plexobject.docusearch.service;

import javax.ws.rs.core.Response;

public interface RepositoryService {

    /**
     * This method fetches detailed document information
     * 
     * @param index
     * @param id
     * @return
     */
    Response get(String index, String id);

    /**
     * This method adds or updates a document using id from client
     * 
     * @param database
     *            - name of database
     * @param id
     *            - document id
     * @param version
     *            - version of document
     * @param body
     *            - contents
     */
    public Response put(String database, String id, String version, String body);

    /**
     * This method adds a new document and creates id for the document
     * 
     * @param database
     *            - name of database
     * @param body
     *            - contents
     */
    public Response post(String database, String body);

    /**
     * This method adds a new document and creates id for the document
     * 
     * @param database
     *            - name of database
     * @param id
     *            - document id
     * @param version
     *            - version of document
     */
    public Response delete(String database, String id, String version);

}

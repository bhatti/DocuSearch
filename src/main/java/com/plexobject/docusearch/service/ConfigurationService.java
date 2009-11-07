package com.plexobject.docusearch.service;

import javax.ws.rs.core.Response;

public interface ConfigurationService {

    /**
     * This method fetches index policy for given index
     * 
     * @param index
     * @param id
     * @return index policy
     */
    Response getIndexPolicy(String index);

    /**
     * This method fetches query policy for given index
     * 
     * @param index
     * @param id
     * @return query policy
     */
    Response getQueryPolicy(String index);

    /**
     * This method saves index policy for given index
     * 
     * @param index
     * @param policy
     * @return response code
     */
    public Response saveIndexPolicy(String index, String policy);

    /**
     * This method saves query policy for given index
     * 
     * @param index
     * @param policy
     * @return response code
     */
    public Response saveQueryPolicy(String index, String policy);

}

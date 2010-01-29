package com.plexobject.docusearch.service;

import javax.ws.rs.core.Response;

public interface SuggestionService {
    /**
     * This method queries the index with keywords and returns JSONObject for
     * SearchDocList.
     * 
     * @param index
     * @param keywords
     * @param format
     * @param limit
     * @return JSONObject for SearchDocList
     */
    Response autocomplete(String index, String keywords, String format,
            int limit);

}

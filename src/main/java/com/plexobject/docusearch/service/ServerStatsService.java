package com.plexobject.docusearch.service;

import javax.ws.rs.core.Response;

public interface ServerStatsService {
    Response flushCaches();
    Response cacheSizes();
}

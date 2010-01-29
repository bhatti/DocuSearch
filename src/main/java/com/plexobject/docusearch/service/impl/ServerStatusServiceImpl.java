/**
 * 
 */
package com.plexobject.docusearch.service.impl;

import java.util.Date;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.cache.CacheFlusher;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.jmx.JMXRegistrar;
import com.plexobject.docusearch.jmx.impl.ServiceJMXBeanImpl;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.service.ServerStatsService;
import com.plexobject.docusearch.util.TimeUtils;

/**
 * @author Shahzad Bhatti
 * 
 */
@Path("/status")
@Component("serverStatusService")
@Scope("singleton")
public class ServerStatusServiceImpl implements ServerStatsService {
    private static final Logger LOGGER = Logger
            .getLogger(ServerStatusServiceImpl.class);

    @Context
    UriInfo uriInfo;

    final ServiceJMXBeanImpl mbean;

    private static final long STARTED = TimeUtils.getCurrentTimeMillis();

    public ServerStatusServiceImpl() {
        mbean = JMXRegistrar.getInstance().register(getClass());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.service.CacheFlushService#flushCaches()
     */
    @Override
    @DELETE
    @Consumes( { MediaType.WILDCARD })
    public Response flushCaches() {
        try {
            CacheFlusher.getInstance().flushCaches();
            mbean.incrementRequests();
            return Response.ok().build();
        } catch (Exception e) {
            LOGGER.error("failed to flush caches", e);
            mbean.incrementError();

            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to flush caches due to " + e + "\n").build();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.plexobject.docusearch.service.CacheFlushService#flushCaches()
     */
    @Override
    @GET
    @Consumes( { MediaType.WILDCARD })
    public Response cacheSizes() {
        final JSONObject response = new JSONObject();
        try {
            response.put("started", new Date(STARTED));
            response.put("uptime", uptime());
            response.put("cacheSizes", getCacheSizes());
            response.put("metrics", getMetrics());
            response.put("systemStats", Timer.getSystemStats());
            response.put("serviceJMXBeans", getServiceJMXBeans());

            if (uriInfo != null) {
                response.put("baseUrl", uriInfo.getAbsolutePath().toString());
            }
            mbean.incrementRequests();

        } catch (JSONException e) {
            LOGGER.error("failed to get stats", e);
            mbean.incrementError();

            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to get stats due to " + e + "\n").build();
        }
        return Response.ok(response.toString()).build();
    }

    private JSONArray getMetrics() {
        final JSONArray metrics = new JSONArray();
        for (Metric m : Metric.getMetrics()) {
            metrics.put(m.toString());
        }
        return metrics;
    }

    private JSONArray getServiceJMXBeans() {
        final JSONArray jmx = new JSONArray();
        for (ServiceJMXBeanImpl m : JMXRegistrar.getInstance()
                .getServiceJMXBeans()) {
            jmx.put(m.toString());
        }
        return jmx;
    }

    private JSONArray getCacheSizes() {
        final JSONArray sizes = new JSONArray();
        int[] size = CacheFlusher.getInstance().cacheSizes();
        for (int i = 0; i < size.length; i++) {
            sizes.put(String.valueOf(size[i]));
        }
        return sizes;
    }

    private static String uptime() {
        final long elapsed = TimeUtils.getCurrentTimeMillis() - STARTED;
        return elapsed > 1000 ? String.format("%.2f secs", elapsed / 1000)
                : String.format("%.2f millis", elapsed);
    }
}

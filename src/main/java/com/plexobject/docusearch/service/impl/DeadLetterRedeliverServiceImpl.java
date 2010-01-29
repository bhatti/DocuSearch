package com.plexobject.docusearch.service.impl;

import java.util.Collection;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.jms.DeadLetterReprocessor;
import com.plexobject.docusearch.jmx.JMXRegistrar;
import com.plexobject.docusearch.jmx.impl.ServiceJMXBeanImpl;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.service.DeadLetterRedeliverService;
import com.sun.jersey.spi.inject.Inject;

@Path("/dlq")
@Component("deadLetterRedeliverService")
@Scope("singleton")
public class DeadLetterRedeliverServiceImpl implements
        DeadLetterRedeliverService {
    static final Logger LOGGER = Logger
            .getLogger(DeadLetterRedeliverServiceImpl.class);
    private static final String DEFAULT_DEST = "com.plexobject.docusearch.secondary_test_dataQ";
    private static final String DEFAULT_BROKER = "localhost";
    private static final boolean ACTIVATE_INDEX = Configuration.getInstance()
            .getBoolean("activate.index", true);

    @Autowired
    @Inject
    private DeadLetterReprocessor deadLetterReprocessor;
    final ServiceJMXBeanImpl mbean;

    public DeadLetterRedeliverServiceImpl() {
        mbean = JMXRegistrar.getInstance().register(getClass());
    }

    @Override
    @POST
    @Produces("application/json")
    @Consumes( { MediaType.WILDCARD })
    public Response redeliver(@QueryParam("broker") final String broker,
            @QueryParam("to") final String to) {
        if (!ACTIVATE_INDEX) {
            return Response.status(RestClient.SERVICE_UNAVAILABLE).type(
                    "text/plain").entity(
                    "DeadLetterRedeliverServiceImpl is not available\n")
                    .build();
        }
        final Timer timer = Metric
                .newTimer("DeadLetterRedeliverServiceImpl.redeliver");

        try {
            Collection<String> messageIds = deadLetterReprocessor.redeliverDLQ(
                    GenericValidator.isBlankOrNull(broker) ? DEFAULT_BROKER
                            : broker,
                    GenericValidator.isBlankOrNull(to) ? DEFAULT_DEST : to);
            final JSONArray response = new JSONArray();
            for (String messageId : messageIds) {
                response.put(messageId);
            }
            timer.stop();
            mbean.incrementRequests();

            return Response.ok(response.toString()).build();
        } catch (Exception e) {
            LOGGER.error("failed to redeliver", e);
            mbean.incrementError();

            return Response.status(RestClient.SERVER_INTERNAL_ERROR).type(
                    "text/plain").entity(
                    "failed to redeliver due to " + e + "\n").build();
        }
    }

    /**
     * @return the deadLetterReprocessor
     */
    public DeadLetterReprocessor getDeadLetterReprocessor() {
        return deadLetterReprocessor;
    }

    /**
     * @param deadLetterReprocessor
     *            the deadLetterReprocessor to set
     */
    public void setDeadLetterReprocessor(
            DeadLetterReprocessor deadLetterReprocessor) {
        this.deadLetterReprocessor = deadLetterReprocessor;
    }

}

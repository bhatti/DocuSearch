package com.plexobject.docusearch.web;

import javax.servlet.ServletContextEvent;

import org.apache.log4j.Logger;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.plexobject.docusearch.Configuration;

public class WebSpringInitializer extends ContextLoaderListener {
    private static final Logger LOGGER = Logger
            .getLogger(WebSpringInitializer.class);
    private static final boolean ACTIVATE_INDEX = Configuration.getInstance()
            .getBoolean("activate.index", true);
    private WebApplicationContext wac;

    @Override
    public void contextInitialized(final ServletContextEvent servletContextEvent) {
        super.contextInitialized(servletContextEvent);
        wac = WebApplicationContextUtils
                .getRequiredWebApplicationContext(servletContextEvent
                        .getServletContext());
        if (ACTIVATE_INDEX) {
            wac.getBean("secondary_test_dataIndexHandler");
            wac.getBean("secondary_test_dataMessageListenerContainer");
            wac.getBean("test_dataMessageListenerContainer");
            wac.getBean("deadLetterReprocessor");
            LOGGER
                    .info("**************************** JMS listener will be enabled for indexing *********************************");
        } else {
            LOGGER
                    .info("**************************** JMS listener will NOT be enabled for indexing *********************************");
        }
    }
}

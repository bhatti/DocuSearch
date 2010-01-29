package com.plexobject.docusearch.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.docs.DocumentPropertiesHelper;
import com.sun.jersey.spi.inject.Inject;

@Component("provincesLookup")
public class ProvincesLookup implements InitializingBean {
    private static final String US_RESOURCE_NAME = "us_states.properties";
    private static final Logger LOGGER = Logger
            .getLogger(CountriesHelper.class);

    private volatile Map<String, Map<String, String>> cachedStates = new TreeMap<String, Map<String, String>>();

    private volatile Map<String, Collection<String>> cachedMajorCities = new TreeMap<String, Collection<String>>();

    @Autowired
    @Inject
    DocumentPropertiesHelper documentPropertiesHelper = new DocumentPropertiesHelper();

    public String getStateNameByCode(String countryCode, String stateCode) {
        Map<String, String> states = cachedStates
                .get(countryCode.toUpperCase());
        return states != null ? states.get(stateCode.toUpperCase()) : null;
    }

    public Collection<String> getStateCodes(String countryCode) {
        Map<String, String> states = cachedStates
                .get(countryCode.toUpperCase());
        return states != null ? Collections.unmodifiableCollection(states
                .keySet()) : Collections.<String> emptyList();
    }

    public Collection<String> getMajorCities(String countryCode) {
        Collection<String> cities = cachedMajorCities.get(countryCode
                .toUpperCase());
        return cities != null ? Collections.unmodifiableCollection(cities)
                : Collections.<String> emptyList();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (documentPropertiesHelper == null) {
            throw new IllegalStateException("DocumentPropertiesHelper not set");
        }

        try {
            final Properties properties = documentPropertiesHelper
                    .load(US_RESOURCE_NAME);
            if (properties.size() == 0) {
                throw new IllegalStateException(
                        "test_data not found");
            }
            Map<String, String> states = new TreeMap<String, String>();
            //
            for (Object id : properties.keySet()) {
                String code = (String) id;
                String name = properties.getProperty(code);
                states.put(code.toUpperCase(), name);
            }
            cachedStates.put("US", states);
            cachedMajorCities.put("US", Arrays.asList("New York", "Chicago",
                    "San Francisco", "Los Angeles", "Dallas", "Philadelphia",
                    "Washington", "Boston", "Atlanta", "Minneapolis"));

        } catch (RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + US_RESOURCE_NAME, e);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("loaded countries symbols " + cachedStates.size());
        }
    }

    /**
     * @return the documentPropertiesHelper
     */
    public DocumentPropertiesHelper getDocumentPropertiesHelper() {
        return documentPropertiesHelper;
    }

    /**
     * @param documentPropertiesHelper
     *            the documentPropertiesHelper to set
     */
    public void setDocumentPropertiesHelper(
            DocumentPropertiesHelper documentPropertiesHelper) {
        this.documentPropertiesHelper = documentPropertiesHelper;
    }
}

package com.plexobject.docusearch.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.collections.MultiHashMap;
import org.apache.commons.collections.MultiMap;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.docs.DocumentMultiMapHelper;
import com.plexobject.docusearch.docs.DocumentPropertiesHelper;
import com.sun.jersey.spi.inject.Inject;

@Component("regionLookup")
public class RegionLookup implements InitializingBean {
    private static final String CONTINENTS_NAME = "continents.properties";
    private static final String COUNTRIES_CONTINENTS_NAME = "countries_continents.properties";

    private static final Logger LOGGER = Logger
            .getLogger(CountriesHelper.class);

    private volatile Map<String, String> cachedContinents = new TreeMap<String, String>();
    private MultiMap continentsToCountries = new MultiHashMap();

    @Autowired
    @Inject
    DocumentPropertiesHelper documentPropertiesHelper = new DocumentPropertiesHelper();

    @Autowired
    @Inject
    DocumentMultiMapHelper documentMultiMapHelper = new DocumentMultiMapHelper();

    public String getContinentNameByCode(String code) {
        return cachedContinents.get(code.toUpperCase());
    }

    public Collection<String> getContinentNames() {
        return Collections.unmodifiableCollection(cachedContinents.values());
    }

    @SuppressWarnings("unchecked")
    public Collection<String> getCountryCodesByContinentCode(String code) {
        return (Collection<String>) continentsToCountries.get(code
                .toUpperCase());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void afterPropertiesSet() throws Exception {
        if (documentPropertiesHelper == null) {
            throw new IllegalStateException("DocumentPropertiesHelper not set");
        }
        if (documentMultiMapHelper == null) {
            throw new IllegalStateException("documentMultiMapHelper not set");
        }
        try {
            final Properties properties = documentPropertiesHelper
                    .load(CONTINENTS_NAME);
            if (properties.size() == 0) {
                throw new IllegalStateException(
                        "test_data not found");
            }

            for (Object id : properties.keySet()) {
                String code = (String) id;
                String name = properties.getProperty(code);
                cachedContinents.put(code.toUpperCase(), name);
            }

            final MultiMap multiMap = documentMultiMapHelper.load(
                    COUNTRIES_CONTINENTS_NAME, "continent_code",
                    "country_code", true);
            if (multiMap.size() == 0) {
                throw new IllegalStateException(
                        "countries / continent not found");
            }

            for (Object id : multiMap.keySet()) {
                String countryCode = (String) id;

                Collection<String> continentCodes = (Collection<String>) multiMap
                        .get(countryCode);
                for (String continentCode : continentCodes) {
                    continentsToCountries.put(continentCode.toUpperCase(),
                            countryCode.toUpperCase());
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + CONTINENTS_NAME, e);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("loaded countries symbols " + cachedContinents.size());
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

    /**
     * @return the documentMultiMapHelper
     */
    public DocumentMultiMapHelper getDocumentMultiMapHelper() {
        return documentMultiMapHelper;
    }

    /**
     * @param documentMultiMapHelper
     *            the documentMultiMapHelper to set
     */
    public void setDocumentMultiMapHelper(
            DocumentMultiMapHelper documentMultiMapHelper) {
        this.documentMultiMapHelper = documentMultiMapHelper;
    }

}

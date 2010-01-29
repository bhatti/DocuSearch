package com.plexobject.docusearch.util;

import java.io.IOException;
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

@Component("countriesHelper")
public class CountriesHelper implements InitializingBean {
    private static final String RESOURCE_NAME = "countries.properties";
    private static final Logger LOGGER = Logger
            .getLogger(CountriesHelper.class);

    private volatile Map<String, String> cachedCountries = new TreeMap<String, String>();

    @Autowired
    @Inject
    DocumentPropertiesHelper documentPropertiesHelper = new DocumentPropertiesHelper();

    public String getCountryNameByCode(String code) {
        return cachedCountries.get(code.toUpperCase());
    }

    public Collection<String> getCountryCodes() {
        return Collections.unmodifiableCollection(cachedCountries.keySet());
    }

    public Collection<String> getCountryNames() {
        return Collections.unmodifiableCollection(cachedCountries.values());
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (documentPropertiesHelper == null) {
            throw new IllegalStateException("DocumentPropertiesHelper not set");
        }

        try {
            final Properties properties = new Properties();

            DocumentPropertiesHelper.loadPropertiesFromResource(RESOURCE_NAME,
                    properties);
            if (properties.size() == 0) {
                throw new IllegalStateException(
                        "test_data not found");
            }
            //
            for (Object id : properties.keySet()) {
                String code = (String) id;
                String name = properties.getProperty(code);
                cachedCountries.put(code.toUpperCase(), name);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + RESOURCE_NAME, e);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("loaded countries symbols " + cachedCountries.size());
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

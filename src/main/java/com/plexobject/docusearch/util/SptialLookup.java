package com.plexobject.docusearch.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.sun.jersey.spi.inject.Inject;

@Component("sptialLookup")
public class SptialLookup {
    private static final String ZIP_DATABASE = "the_zipcodes";
    private Map<String, double[]> cachedLatLongs = new ConcurrentHashMap<String, double[]>();

    @Autowired
    @Inject
    DocumentRepository documentRepository;

    public double[] getLatitudeAndLongitude(final String zip) {
        double[] latLongs = cachedLatLongs.get(zip);
        if (latLongs == null) {
            final Document doc = documentRepository.getDocument(ZIP_DATABASE,
                    zip);
            final String latitude = (String) doc.get("latitude");
            final String longitude = (String) doc.get("longitude");
            latLongs = new double[] { Double.valueOf(latitude).doubleValue(),
                    Double.valueOf(longitude).doubleValue() };
            cachedLatLongs.put(zip, latLongs);
        }
        return latLongs;
    }
}

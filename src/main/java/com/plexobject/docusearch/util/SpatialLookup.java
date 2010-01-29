package com.plexobject.docusearch.util;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.plexobject.docusearch.cache.CacheLoader;
import com.plexobject.docusearch.cache.CachedMap;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.sun.jersey.spi.inject.Inject;

@Component("spatialLookup")
public class SpatialLookup {
    private static final String ZIP_DATABASE = "the_zipcodes";
    private static final long INDEFINITE = 0;

    private Map<String, double[]> cachedLatLongs = new CachedMap<String, double[]>(
            INDEFINITE, 8192, new CacheLoader<String, double[]>() {
                @Override
                public double[] get(String zip) {
                    return fetchLatitudeAndLongitude(zip);
                }
            }, null);

    @Autowired
    @Inject
    DocumentRepository documentRepository;

    public double[] getLatitudeAndLongitude(final String zip) {
        double[] latLongs = cachedLatLongs.get(zip);
        if (latLongs == null) {
            latLongs = fetchLatitudeAndLongitude(zip);
            cachedLatLongs.put(zip, latLongs);
        }
        return latLongs;
    }

    private double[] fetchLatitudeAndLongitude(final String zip) {
        double[] latLongs;
        final Document doc = documentRepository.getDocument(ZIP_DATABASE, zip);
        final String latitude = (String) doc.get("latitude");
        final String longitude = (String) doc.get("longitude");
        latLongs = new double[] { Double.valueOf(latitude).doubleValue(),
                Double.valueOf(longitude).doubleValue() };
        return latLongs;
    }
}

package com.plexobject.docusearch.converter;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;

import com.plexobject.docusearch.index.IndexPolicy;

/**
 * 
 * @author Shahzad Bhatti
 * 
 */
public class IndexPolicyToMap implements
        Converter<IndexPolicy, Map<String, Object>> {
    /**
     * 
     * @param IndexPolicy
     * 
     * @return Map
     */
    @Override
    public Map<String, Object> convert(final IndexPolicy policy) {
        final Map<String, Object> value = new TreeMap<String, Object>();

        if (policy != null) {
            if (policy.hasCustomSortingField()) {
                value.put(Constants.CUSTOM_SORTING_FIELD, policy
                        .getCustomSortingField());
            }
            if (policy.hasCustomIdField()) {
                value.put(Constants.CUSTOM_ID_FIELD, policy.getCustomIdField());
            }

            value.put(Constants.SCORE, policy.getScore());
            value.put(Constants.BOOST, policy.getBoost());
            value.put(Constants.ANALYZER, policy.getAnalyzer());
            value.put(Constants.ADD_TO_DICTIONARY, policy.isAddToDictionary());
            value.put(Constants.OWNER, policy.getOwner());
            final List<Object> mapFields = new ArrayList<Object>();
            for (IndexPolicy.Field field : policy.getFields()) {
                final Map<String, Object> mapField = new TreeMap<String, Object>();

                mapField.put(Constants.NAME, field.name);
                mapField.put(Constants.STORE_AS, field.storeAs);
                mapField.put(Constants.STORE_IN_INDEX, field.storeInIndex);
                mapField.put(Constants.HTML_TO_TEXT, field.htmlToText);
                mapField.put(Constants.SPATIAL_LATITUDE, field.spatialLatitude);
                mapField.put(Constants.SPATIAL_LONGITUDE,
                        field.spatialLongitude);
                mapField.put(Constants.TOKENIZE, field.tokenize);
                mapField.put(Constants.ANALYZE, field.analyze);
                mapField.put(Constants.BOOST, field.boost);
                mapFields.add(mapField);
            }
            value.put(Constants.FIELDS, mapFields);
        }
        return value;
    }
}

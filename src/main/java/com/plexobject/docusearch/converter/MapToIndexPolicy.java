package com.plexobject.docusearch.converter;

import java.util.List;
import java.util.Map;

import com.plexobject.docusearch.index.IndexPolicy;

/**
 * 
 * @author Shahzad Bhatti
 * 
 */
public class MapToIndexPolicy implements
        Converter<Map<String, Object>, IndexPolicy> {

    /**
     * @param value
     *            - Map object
     * @return IndexPolicy
     */
    @SuppressWarnings("unchecked")
    @Override
    public IndexPolicy convert(final Map<String, Object> value) {
        final IndexPolicy policy = new IndexPolicy();
        if (value != null) {
            policy.setCustomSortingField((String) value
                    .get(Constants.CUSTOM_SORTING_FIELD));
            policy.setCustomIdField((String) value
                    .get(Constants.CUSTOM_ID_FIELD));
            if (value.containsKey(Constants.SCORE)) {
                policy.setScore(Integer.parseInt(String.valueOf(value
                        .get(Constants.SCORE))));
            }
            if (value.containsKey(Constants.BOOST)) {
                policy.setBoost(Float.valueOf(String.valueOf(value
                        .get(Constants.BOOST))));
            }
            if (value.get(Constants.ANALYZER) != null) {
                policy.setAnalyzer(value.get(Constants.ANALYZER).toString());
            }
            if (value.get(Constants.ADD_TO_DICTIONARY) != null) {
                policy.setAddToDictionary(Boolean.valueOf(value.get(
                        Constants.ADD_TO_DICTIONARY).toString()));
            }
            if (value.get(Constants.OWNER) != null) {
                policy.setOwner(value.get(Constants.OWNER).toString());
            }
            final List<Object> fields = (List<Object>) value
                    .get(Constants.FIELDS);
            if (fields != null) {
                for (Object f : fields) {
                    final Map<String, Object> field = (Map<String, Object>) f;
                    final String name = (String) field.get(Constants.NAME);
                    boolean htmlToText = false;
                    if (field.containsKey(Constants.HTML_TO_TEXT)) {
                        htmlToText = Boolean.valueOf(field.get(
                                Constants.HTML_TO_TEXT).toString());
                    }
                    boolean spatialLatitude = false;
                    if (field.containsKey(Constants.SPATIAL_LATITUDE)) {
                        spatialLatitude = Boolean.valueOf(field.get(
                                Constants.SPATIAL_LATITUDE).toString());
                    }
                    boolean spatialLongitude = false;
                    if (field.containsKey(Constants.SPATIAL_LONGITUDE)) {
                        spatialLongitude = Boolean.valueOf(field.get(
                                Constants.SPATIAL_LONGITUDE).toString());
                    }
                    boolean storeInIndex = false;
                    if (field.containsKey(Constants.STORE_IN_INDEX)) {
                        storeInIndex = Boolean.valueOf(field.get(
                                Constants.STORE_IN_INDEX).toString());
                    }
                    String storeAs = null;
                    if (field.get(Constants.STORE_AS) != null) {
                        storeAs = field.get(Constants.STORE_AS).toString();
                    }

                    boolean analyze = true;
                    if (field.containsKey(Constants.ANALYZE)) {
                        analyze = Boolean.valueOf(field.get(Constants.ANALYZE)
                                .toString());
                    }
                    boolean tokenize = false;
                    if (field.containsKey(Constants.TOKENIZE)) {
                        tokenize = Boolean.valueOf(field
                                .get(Constants.TOKENIZE).toString());
                    }
                    float boost = 0.0F;
                    if (field.containsKey(Constants.BOOST)) {
                        boost = Float.valueOf(
                                field.get(Constants.BOOST).toString())
                                .floatValue();
                    }

                    policy.add(name, storeInIndex, storeAs, analyze, tokenize,
                            boost, htmlToText, spatialLatitude,
                            spatialLongitude);
                }

            }
        }
        return policy;
    }
}

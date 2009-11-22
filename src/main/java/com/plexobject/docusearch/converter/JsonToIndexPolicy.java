package com.plexobject.docusearch.converter;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.plexobject.docusearch.index.IndexPolicy;

/**
 * 
 * @author Shahzad Bhatti
 * 
 */
public class JsonToIndexPolicy implements Converter<JSONObject, IndexPolicy> {
    /**
     * @param value
     *            - JSON object
     * @return IndexPolicy
     */
    @Override
    public IndexPolicy convert(final JSONObject value) {
        final IndexPolicy policy = new IndexPolicy();
        if (value != null) {
            try {
                if (value.has(Constants.SCORE)) {
                    policy.setScore(Integer.parseInt(value
                            .getString(Constants.SCORE)));
                }
                if (value.has(Constants.BOOST)) {
                    policy.setBoost(Float.valueOf(
                            value.getString(Constants.BOOST)).floatValue());
                }
                if (value.has(Constants.ANALYZER)) {
                    policy.setAnalyzer(value.getString(Constants.ANALYZER));
                }
                if (value.has(Constants.ADD_TO_DICTIONARY)) {
                    policy.setAddToDictionary(Boolean.valueOf(value.getString(Constants.ADD_TO_DICTIONARY)));
                }
                if (value.has(Constants.OWNER)) {
                    policy.setOwner(value.getString(Constants.OWNER));
                }
                final JSONArray fields = value.getJSONArray(Constants.FIELDS);
                if (fields != null) {
                    final int len = fields.length();
                    for (int i = 0; i < len; i++) {
                        final JSONObject field = fields.getJSONObject(i);
                        final String name = field.getString(Constants.NAME);
                        final boolean storeInIndex = field
                                .optBoolean(Constants.STORE_IN_INDEX);
                        boolean analyze = true;
                        if (field.has(Constants.ANALYZE)) {
                            analyze = field.getBoolean(Constants.ANALYZE);
                        }
                        boolean tokenize = false;
                        if (field.has(Constants.TOKENIZE)) {
                            tokenize = field.getBoolean(Constants.TOKENIZE);
                        }
                        float boost = 0.0F;
                        if (field.has(Constants.BOOST)) {
                            boost = Float.valueOf(
                                    field.getString(Constants.BOOST))
                                    .floatValue();
                        }
                        policy
                                .add(name, storeInIndex, analyze, tokenize,
                                        boost);
                    }
                }
            } catch (NumberFormatException e) {
                throw new ConversionException("failed to convert ", e);
            } catch (JSONException e) {
                throw new ConversionException("failed to convert json", e);
            }
        }
        return policy;
    }
}

package com.plexobject.docusearch.converter;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.plexobject.docusearch.query.LookupPolicy;

public class JsonToLookupPolicy implements Converter<JSONObject, LookupPolicy> {
    private final JsonToQueryPolicy jsonToQueryPolicy = new JsonToQueryPolicy();

    /**
     * @param value
     *            - JSON object
     * @return QueryPolicy
     */
    @Override
    public LookupPolicy convert(final JSONObject value) {
        final LookupPolicy policy = new LookupPolicy();

        try {

            if (value != null) {
                policy.setDictionaryIndex(value
                        .optString(Constants.DICTIONARY_DATABASE, null));
                policy.setDictionaryField(value
                        .optString(Constants.DICTIONARY_FIELD, null));

                policy.setFieldToReturn(value
                        .getString(Constants.FIELD_TO_RETURN));
                jsonToQueryPolicy.doConvert(value, policy);
            }
        } catch (JSONException e) {
            throw new ConversionException("failed to convert json", e);
        }
        return policy;
    }
}

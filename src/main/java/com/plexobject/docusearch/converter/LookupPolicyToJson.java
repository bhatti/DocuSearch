package com.plexobject.docusearch.converter;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.plexobject.docusearch.query.LookupPolicy;

public class LookupPolicyToJson implements Converter<LookupPolicy, JSONObject> {
    private final QueryPolicyToJson queryPolicyToJson = new QueryPolicyToJson();

    /**
     * 
     * @param QueryPolicy
     * 
     * @return JSON object
     */
    @Override
    public JSONObject convert(final LookupPolicy policy) {
        final JSONObject value = new JSONObject();

        if (policy != null) {
            try {
                value.put(Constants.DICTIONARY_DATABASE, policy
                        .getDictionaryIndex());
                value.put(Constants.DICTIONARY_FIELD, policy
                        .getDictionaryField());

                value.put(Constants.FIELD_TO_RETURN, policy.getFieldToReturn());
                queryPolicyToJson.doConvert(policy, value);
            } catch (JSONException e) {
                throw new ConversionException("failed to convert " + policy, e);
            }
        }
        return value;
    }

}

package com.plexobject.docusearch.converter;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.plexobject.docusearch.query.QueryPolicy;

/**
 * 
 * @author Shahzad Bhatti
 * 
 */
public class QueryPolicyToJson implements Converter<QueryPolicy, JSONObject> {
    /**
     * 
     * @param QueryPolicy
     * 
     * @return JSON object
     */
    @Override
    public JSONObject convert(final QueryPolicy policy) {
        final JSONObject value = new JSONObject();

        if (policy != null) {
            try {

                doConvert(policy, value);
            } catch (JSONException e) {
                throw new ConversionException("failed to convert " + policy, e);
            }
        }
        return value;
    }

    void doConvert(final QueryPolicy policy, final JSONObject value)
            throws JSONException {
        final JSONArray jsonFields = new JSONArray();
        for (QueryPolicy.Field field : policy.getFields()) {
            JSONObject jsonField = new JSONObject();
            jsonField.put(Constants.NAME, field.name);
            jsonField.put(Constants.SORT_ORDER, field.sortOrder);
            jsonField.put(Constants.ASCENDING_ORDER, field.ascendingSort);
            jsonField.put(Constants.FIELD_TYPE, field.fieldType.getType());
            jsonField.put(Constants.BOOST, field.boost);
            jsonFields.put(jsonField);
        }
        value.put(Constants.FIELDS, jsonFields);
    }
}

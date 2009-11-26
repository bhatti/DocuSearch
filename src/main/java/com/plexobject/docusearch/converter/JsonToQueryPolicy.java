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
public class JsonToQueryPolicy implements Converter<JSONObject, QueryPolicy> {
    /**
     * @param value
     *            - JSON object
     * @return QueryPolicy
     */
    @Override
    public QueryPolicy convert(final JSONObject value) {
        final QueryPolicy policy = new QueryPolicy();
        try {

            if (value != null) {
                doConvert(value, policy);
            }
        } catch (JSONException e) {
            throw new ConversionException("failed to convert json", e);
        }
        return policy;
    }

    void doConvert(final JSONObject value, final QueryPolicy policy)
            throws JSONException {

        final JSONArray fields = value.getJSONArray(Constants.FIELDS);
        if (fields != null) {
            final int len = fields.length();
            for (int i = 0; i < len; i++) {
                final JSONObject field = fields.getJSONObject(i);
                final String name = field.getString(Constants.NAME);
                int sortOrder = 0;
                if (field.has(Constants.SORT_ORDER)) {
                    sortOrder = field.getInt(Constants.SORT_ORDER);
                }
                boolean ascendingSort = true;
                if (field.has(Constants.ASCENDING_ORDER)) {
                    ascendingSort = field.getBoolean(Constants.ASCENDING_ORDER);
                }
                int fieldType = 0;
                if (field.has(Constants.FIELD_TYPE)) {
                    fieldType = field.getInt(Constants.FIELD_TYPE);
                }

                float boost = 0.0F;
                if (field.has(Constants.BOOST)) {
                    boost = Float.valueOf(field.getString(Constants.BOOST))
                            .floatValue();
                }
                policy.add(name, sortOrder, ascendingSort, boost,
                        QueryPolicy.FieldType.fromType(fieldType));
            }

        }
    }
}

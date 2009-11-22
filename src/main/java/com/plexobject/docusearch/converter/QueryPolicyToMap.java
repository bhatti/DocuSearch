package com.plexobject.docusearch.converter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.plexobject.docusearch.query.QueryPolicy;

/**
 * 
 * @author Shahzad Bhatti
 * 
 */
public class QueryPolicyToMap implements
        Converter<QueryPolicy, Map<String, Object>> {

    /**
     * 
     * @param QueryPolicy
     * 
     * @return Map
     */
    @Override
    public Map<String, Object> convert(final QueryPolicy policy) {
        final Map<String, Object> value = new HashMap<String, Object>();

        if (policy != null) {
            doConvert(policy, value);
        }
        return value;
    }

    void doConvert(final QueryPolicy policy, final Map<String, Object> value) {
        final List<Object> mapFields = new ArrayList<Object>();
        for (QueryPolicy.Field field : policy.getFields()) {
            final Map<String, Object> mapField = new HashMap<String, Object>();

            mapField.put(Constants.NAME, field.name);
            mapField.put(Constants.SORT_ORDER, field.sortOrder);
            mapField.put(Constants.ASCENDING_ORDER, field.ascendingSort);
            mapField.put(Constants.FIELD_TYPE, field.fieldType.getType());
            mapField.put(Constants.BOOST, field.boost);
            mapFields.add(mapField);
        }
        value.put(Constants.FIELDS, mapFields);
    }
}

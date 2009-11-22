package com.plexobject.docusearch.converter;

import java.util.Map;

import com.plexobject.docusearch.query.LookupPolicy;

public class MapToLookupPolicy implements
        Converter<Map<String, Object>, LookupPolicy> {
    private final MapToQueryPolicy mapToQueryPolicy = new MapToQueryPolicy();

    /**
     * @param value
     *            - Map object
     * @return QueryPolicy
     */
    @Override
    public LookupPolicy convert(final Map<String, Object> value) {
        final LookupPolicy policy = new LookupPolicy();
        if (value != null) {
            policy.setFieldToReturn((String) value
                    .get(Constants.FIELD_TO_RETURN));
            mapToQueryPolicy.doConvert(value, policy);
        }
        return policy;
    }

}

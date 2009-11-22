package com.plexobject.docusearch.converter;

import java.util.HashMap;
import java.util.Map;

import com.plexobject.docusearch.query.LookupPolicy;

public class LookupPolicyToMap implements
        Converter<LookupPolicy, Map<String, Object>> {
    private final QueryPolicyToMap queryPolicyToMap = new QueryPolicyToMap();

    /**
     * 
     * @param QueryPolicy
     * 
     * @return Map
     */
    @Override
    public Map<String, Object> convert(final LookupPolicy policy) {
        final Map<String, Object> value = new HashMap<String, Object>();

        if (policy != null) {
            value.put(Constants.FIELD_TO_RETURN, policy.getFieldToReturn());

            queryPolicyToMap.doConvert(policy, value);
        }
        return value;
    }

}

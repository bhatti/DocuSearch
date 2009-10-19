package com.plexobject.docusearch.converter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.plexobject.docusearch.query.QueryPolicy;

/**
 * 
 * @author bhatti@plexobject.com
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
	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> convert(final QueryPolicy policy) {
		final Map<String, Object> value = new HashMap<String, Object>();

		if (policy != null) {
			List fields = new ArrayList();
			for (String field : policy.getFields()) {
				fields.add(field);
			}
			value.put(Constants.FIELDS, fields);
		}
		return value;
	}
}

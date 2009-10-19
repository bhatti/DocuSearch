package com.plexobject.docusearch.converter;

import java.util.List;
import java.util.Map;

import com.plexobject.docusearch.query.QueryPolicy;

/**
 * 
 * @author bhatti@plexobject.com
 * 
 */
public class MapToQueryPolicy implements
		Converter<Map<String, Object>, QueryPolicy> {
	/**
	 * @param value
	 *            - Map object
	 * @return QueryPolicy
	 */
	@SuppressWarnings("unchecked")
	@Override
	public QueryPolicy convert(final Map<String, Object> value) {
		final QueryPolicy policy = new QueryPolicy();
		if (value != null) {
			final List<String> fields = (List<String>) value
					.get(Constants.FIELDS);
			if (fields != null) {
				for (String f : fields) {
					policy.add(f);
				}

			}
		}
		return policy;
	}
}

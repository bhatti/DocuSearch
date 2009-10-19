package com.plexobject.docusearch.converter;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.plexobject.docusearch.query.QueryPolicy;

/**
 * 
 * @author bhatti@plexobject.com
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
				JSONArray fields;
				fields = value.getJSONArray(Constants.FIELDS);
				if (fields != null) {
					final int len = fields.length();
					for (int i = 0; i < len; i++) {
						final String field = fields.getString(i);
						policy.add(field);
					}

				}
			}
		} catch (JSONException e) {
			throw new ConversionException("failed to convert json", e);
		}

		return policy;
	}
}

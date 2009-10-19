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
			final JSONArray jsonFields = new JSONArray();
			for (String field : policy.getFields()) {
				jsonFields.put(field);
			}
			try {
				value.put(Constants.FIELDS, jsonFields);
			} catch (JSONException e) {
				throw new ConversionException("failed to convert json "
						+ jsonFields, e);
			}
		}
		return value;
	}
}

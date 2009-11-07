package com.plexobject.docusearch.converter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * @author Shahzad Bhatti
 * 
 */
public class JsonToJavaConverter implements Converter<Object, Object> {
	/**
	 * @param jsonObject
	 *            - raw JSON object
	 * @return Map of String to values where each value can be a list, map or
	 *         primitive type such as number, string, boolean.
	 */
	@Override
	public Object convert(final Object value) {
		try {

			if (value == null) {
				return null;
			} else if (value instanceof JSONObject) {
				return fromJsonObject((JSONObject) value);
			} else if (value instanceof JSONArray) {
				return fromJsonArray((JSONArray) value);
			} else if (value instanceof CharSequence
					&& value.toString().startsWith("{")) {
				return fromJsonObject(new JSONObject(value.toString()));
			} else if (value instanceof CharSequence
					&& value.toString().startsWith("[")) {
				return fromJsonArray(new JSONArray(value.toString()));
			} else {
				return value;
			}
		} catch (JSONException e) {
			throw new ConversionException("failed to convert json", e);
		}

	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> fromJsonObject(final JSONObject jsonObject)
			throws JSONException {
		Map<String, Object> properties = new HashMap<String, Object>();
		for (Iterator keys = jsonObject.keys(); keys.hasNext();) {
			String key = (String) keys.next();
			Object value = jsonObject.get(key);
			properties.put(key, convert(value));
		}
		return properties;
	}

	private List<Object> fromJsonArray(final JSONArray jsonArray)
			throws JSONException {
		List<Object> list = new ArrayList<Object>();
		int len = jsonArray.length();
		for (int i = 0; i < len; i++) {
			Object jsonValue = jsonArray.get(i);
			Object value = convert(jsonValue);
			list.add(value);
		}
		return list;
	}

}

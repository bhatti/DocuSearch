package com.plexobject.docusearch.converter;

import java.util.Collection;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * @author Shahzad Bhatti
 * 
 */
public class JavaToJsonConverter implements Converter<Object, Object> {
	/**
	 * @param map
	 *            - Map of String to values where each value can be a list, map
	 *            or primitive type such as number, string, boolean.
	 * @return JSONObject
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Object convert(final Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Map) {
			return fromMap((Map<String, Object>) value);
		} else if (value instanceof Collection) {
			return fromCollection((Collection<Object>) value);
		} else if (value instanceof Boolean || value instanceof Character
				|| value instanceof CharSequence || value instanceof Number
				|| value instanceof Byte) {
			return value;
		} else {
			throw new IllegalArgumentException("Unknown type ["
					+ value.getClass().getName() + "] " + value);
		}
	}

	private JSONObject fromMap(final Map<String, Object> map) {
		final JSONObject jsonObject = new JSONObject();
		for (Map.Entry<String, Object> e : map.entrySet()) {
			Object value = e.getValue();
			if (value != null) {
				try {
					jsonObject.put(e.getKey(), convert(value));
				} catch (JSONException ex) {
					throw new ConversionException("failed to convert json", ex);
				}
			}
		}
		return jsonObject;
	}

	private JSONArray fromCollection(final Collection<Object> c) {
		final JSONArray jsonArray = new JSONArray();
		for (Object value : c) {
			jsonArray.put(convert(value));
		}
		return jsonArray;
	}
}

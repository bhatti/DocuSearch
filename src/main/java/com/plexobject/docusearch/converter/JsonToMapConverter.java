package com.plexobject.docusearch.converter;

import java.util.Map;

import org.codehaus.jettison.json.JSONObject;

/**
 * @author bhatti@plexobject.com
 * 
 */
public class JsonToMapConverter implements
		Converter<JSONObject, Map<String, Object>> {
	private final Converter<Object, Object> jsonConverter = new JsonToJavaConverter();

	/**
	 * @param map
	 *            - Map of String to values where each value can be a list, map
	 *            or primitive type such as number, string, boolean.
	 * @return JSONObject
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> convert(final JSONObject json) {
		return (Map<String, Object>) jsonConverter.convert(json);
	}
}

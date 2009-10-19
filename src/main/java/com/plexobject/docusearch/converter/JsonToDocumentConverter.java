package com.plexobject.docusearch.converter;

import java.util.Map;

import com.plexobject.docusearch.domain.Document;

import org.codehaus.jettison.json.JSONObject;

/**
 * @author bhatti@plexobject.com
 *
 */
public class JsonToDocumentConverter implements Converter<JSONObject, Document> {
    private final Converter<Object, Object> jsonConverter = new JsonToJavaConverter();
	/**
	 * @param map - Map of String to values where each value can be a list, map or primitive type such as number, string, boolean.
	 * @return JSONObject
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Document convert(final JSONObject json) {
		Map<String, Object> map = (Map<String, Object>) jsonConverter.convert(json);
	 	return new Document(map);
	}
}

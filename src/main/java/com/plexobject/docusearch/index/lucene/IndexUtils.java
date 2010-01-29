package com.plexobject.docusearch.index.lucene;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class IndexUtils {
    private static final Logger LOGGER = Logger.getLogger(IndexUtils.class);
    private static final Pattern JSON_PATTERN = Pattern
            .compile("[,;:\\[\\]{}()\\s]+");

    static String getValue(final JSONObject json, final String name)
            throws JSONException {
        String value = null;
        int ndx;
        if ((ndx = name.indexOf("[")) != -1) {
            value = getArrayValue(json, name, ndx);
        } else if ((ndx = name.indexOf("{")) != -1) {
            value = getHashValue(json, name, ndx);
        } else {
            value = json.optString(name, null);
        }
        if (value != null) {
            Matcher matcher = JSON_PATTERN.matcher(value);
            value = matcher.replaceAll(" ");
        }
        return value;
    }

    static String getHashValue(final JSONObject json, final String name, int ndx)
            throws JSONException {
        String value;
        final String tagName = name.substring(0, ndx);
        value = json.optString(tagName, null);
        if (value == null) {
            // do nothing
        } else if (!value.startsWith("{")) {
            throw new IllegalStateException("Failed to get hash value for "
                    + tagName + " in " + value + " from json " + json);
        } else {
            final JSONObject jsonObject = new JSONObject(value);
            final String subscript = name.substring(ndx + 1, name.indexOf("}"));
            value = jsonObject.optString(subscript, null);
        }
        return value;
    }

    static String getArrayValue(final JSONObject json, final String name,
            int ndx) throws JSONException {
        String value;
        final String subscript = name.substring(ndx + 1, name.indexOf("]"));

        final String tagName = name.substring(0, ndx);
        value = json.optString(tagName, null);
        if (value == null) {
            // do nothing
        } else if (!value.startsWith("[")) {
            if (value != null && value.startsWith("{")) {
                final JSONObject jsonObject = new JSONObject(value);
                value = jsonObject.optString(subscript, null);
            } else {
                LOGGER.warn("Failed to get array value for " + tagName + " in "
                        + value + " from json " + json);
            }
        } else {
            final JSONArray jsonArray = new JSONArray(value);
            try {
                int offset = Integer.parseInt(subscript);
                value = jsonArray.optString(offset, null);
            } catch (NumberFormatException e) {
                StringBuilder sb = new StringBuilder();
                int len = jsonArray.length();
                for (int i = 0; i < len; i++) {
                    JSONObject elementJson = jsonArray.getJSONObject(i);
                    sb.append(elementJson.getString(subscript));
                    sb.append(" ");
                }
                value = sb.toString();
                LOGGER.info("xxxxxxxxxxxxxxxxAdding entire array of " + name
                        + "=" + value);
            }
        }
        return value;
    }

}

package com.plexobject.docusearch.converter;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.query.LookupPolicy;

public class JsonToLookupPolicyTest {
    Converter<JSONObject, LookupPolicy> converter;

    @Before
    public void setUp() throws Exception {
        converter = new JsonToLookupPolicy();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testConvert() throws JSONException {
        final JSONObject json = newLookupPolicyJSONObject();
        final LookupPolicy policy = converter.convert(json);

        Assert.assertEquals(10, policy.getFields().size());
        Map<String, Integer> count = new HashMap<String, Integer>();

        for (int i = 0; i < 10; i++) {
            count.put("name" + i, new Integer(0));
        }
        for (String name : policy.getFieldNames()) {
            count.put(name, count.get(name) + 1);
        }
        for (int i = 0; i < 10; i++) {
            int actual = count.get("name" + i);

            Assert.assertEquals(1, actual);
        }
        Assert.assertEquals("return", policy.getFieldToReturn());
    }

    private static JSONObject newLookupPolicyJSONObject() throws JSONException {
        final JSONObject policy = new JSONObject();
        policy.put(Constants.FIELD_TO_RETURN, "return");

        final JSONArray fields = new JSONArray();
        for (int i = 0; i < 10; i++) {
            final JSONObject field = new JSONObject();
            field.put(Constants.NAME, "name" + i);
            field.put(Constants.SORT_ORDER, "1");
            field.put(Constants.ASCENDING_ORDER, "true");
            field.put(Constants.BOOST, "1.1");
            fields.put(field);
        }

        policy.put(Constants.FIELDS, fields);
        return policy;
    }
}

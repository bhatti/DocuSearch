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

public class LookupPolicyToJsonTest {
    Converter<LookupPolicy, JSONObject> converter;

    @Before
    public void setUp() throws Exception {
        converter = new LookupPolicyToJson();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testConvert() throws JSONException {
        final JSONObject policy = converter.convert(newLookupPolicy());

        JSONArray fields = policy.getJSONArray(Constants.FIELDS);
        Assert.assertEquals(10, fields.length());
        Map<String, Integer> count = new HashMap<String, Integer>();
        Assert.assertEquals("return", policy
                .getString(Constants.FIELD_TO_RETURN));
        for (int i = 0; i < 10; i++) {
            count.put("name" + i, new Integer(0));
        }

        for (int i = 0; i < 10; i++) {
            JSONObject field = fields.getJSONObject(i);
            final String name = field.getString(Constants.NAME);
            final int num = Integer.parseInt(name.replace("name", ""));
            count.put(name, count.get(name) + 1);
            Assert.assertEquals(String.valueOf(num), field
                    .getString(Constants.SORT_ORDER));
            Assert.assertEquals(String.valueOf(num % 2 == 1), field
                    .getString(Constants.ASCENDING_ORDER));
            Assert.assertEquals(1.1F, new Float(field
                    .getString(Constants.BOOST)).floatValue(), 0.001);

        }
        for (int i = 0; i < 10; i++) {
            int actual = count.get("name" + i);

            Assert.assertEquals(1, actual);
        }

    }

    private static LookupPolicy newLookupPolicy() {
        final LookupPolicy policy = new LookupPolicy();
        policy.setFieldToReturn("return");
        for (int i = 0; i < 10; i++) {
            policy.add("name" + i, i, i % 2 == 1, 1.1F,
                    LookupPolicy.FieldType.STRING);
        }
        return policy;
    }

}
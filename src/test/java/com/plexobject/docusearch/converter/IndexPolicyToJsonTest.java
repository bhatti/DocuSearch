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

import com.plexobject.docusearch.converter.Constants;
import com.plexobject.docusearch.converter.Converter;
import com.plexobject.docusearch.converter.IndexPolicyToJson;
import com.plexobject.docusearch.index.IndexPolicy;

public class IndexPolicyToJsonTest {
    Converter<IndexPolicy, JSONObject> converter;

    @Before
    public void setUp() throws Exception {
        converter = new IndexPolicyToJson();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testConvert() throws JSONException {
        final JSONObject policy = converter.convert(newIndexPolicy());
        Assert.assertEquals("10", policy.getString(Constants.SCORE));
        Assert.assertEquals("20.5", policy.getString(Constants.BOOST));
        JSONArray fields = policy.getJSONArray(Constants.FIELDS);
        Assert.assertEquals(10, fields.length());
        Map<String, Integer> count = new HashMap<String, Integer>();

        for (int i = 0; i < 10; i++) {
            count.put("name" + i, new Integer(0));
        }

        for (int i = 0; i < 10; i++) {
            JSONObject field = fields.getJSONObject(i);
            final String name = field.getString(Constants.NAME);
            final int num = Integer.parseInt(name.replace("name", ""));
            count.put(name, count.get(name) + 1);
            Assert.assertEquals(String.valueOf(num % 2 == 0), field
                    .getString(Constants.STORE_IN_INDEX));
            Assert.assertEquals(String.valueOf(num % 2 == 1), field
                    .getString(Constants.ANALYZE));
            Assert.assertEquals(1.1F, new Float(field
                    .getString(Constants.BOOST)).floatValue(), 0.001);
        }
        for (int i = 0; i < 10; i++) {
            int actual = count.get("name" + i);

            Assert.assertEquals(1, actual);
        }

    }

    private static IndexPolicy newIndexPolicy() {
        final IndexPolicy policy = new IndexPolicy();
        policy.setScore(10);
        policy.setBoost(20.5F);
        for (int i = 0; i < 10; i++) {
            policy.add("name" + i, i % 2 == 0, i % 2 == 1, i % 2 == 1, 1.1F,
                    false, false, false);
        }
        return policy;
    }

}

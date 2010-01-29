package com.plexobject.docusearch.converter;

import java.util.Collection;
import java.util.TreeMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.query.QueryPolicy;

public class QueryPolicyToMapTest {
    Converter<QueryPolicy, Map<String, Object>> converter;

    @Before
    public void setUp() throws Exception {
        converter = new QueryPolicyToMap();
    }

    @After
    public void tearDown() throws Exception {
    }

    @SuppressWarnings("unchecked")
    @Test
    public final void testConvert() throws JSONException {
        final Map<String, Object> policy = converter.convert(newQueryPolicy());

        final Collection<Map<String, Object>> fields = (Collection<Map<String, Object>>) policy
                .get(Constants.FIELDS);
        Assert.assertEquals(10, fields.size());
        Map<String, Integer> count = new TreeMap<String, Integer>();

        for (int i = 0; i < 10; i++) {
            count.put("name" + i, new Integer(0));
        }

        for (Map<String, Object> field : fields) {
            final String name = (String) field.get(Constants.NAME);
            final int num = Integer.parseInt(name.replace("name", ""));
            count.put(name, count.get(name) + 1);
            Assert.assertEquals(Integer.valueOf(num), field
                    .get(Constants.SORT_ORDER));
            Assert.assertEquals(Boolean.valueOf(num % 2 == 1), field
                    .get(Constants.ASCENDING_ORDER));
            Assert
                    .assertEquals(Float.valueOf(1.1F), field
                            .get(Constants.BOOST));
        }
        for (int i = 0; i < 10; i++) {
            int actual = count.get("name" + i);

            Assert.assertEquals(1, actual);
        }

    }

    private static QueryPolicy newQueryPolicy() {
        final QueryPolicy policy = new QueryPolicy();

        for (int i = 0; i < 10; i++) {
            policy.add("name" + i, i, i % 2 == 1, 1.1F, QueryPolicy.FieldType.STRING);
        }
        return policy;
    }

}

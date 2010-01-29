package com.plexobject.docusearch.converter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.converter.Constants;
import com.plexobject.docusearch.converter.Converter;
import com.plexobject.docusearch.converter.MapToQueryPolicy;
import com.plexobject.docusearch.query.QueryPolicy;

public class MapToQueryPolicyTest {
    Converter<Map<String, Object>, QueryPolicy> converter;

    @Before
    public void setUp() throws Exception {
        converter = new MapToQueryPolicy();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testConvert() {
        final Map<String, Object> map = newQueryPolicyMap();
        final QueryPolicy policy = converter.convert(map);

        Assert.assertEquals(10, policy.getFields().size());

        Map<String, Integer> count = new TreeMap<String, Integer>();

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

    }

    private static Map<String, Object> newQueryPolicyMap() {
        final Map<String, Object> map = new TreeMap<String, Object>();

        final Collection<Map<String, Object>> fields = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 10; i++) {
            final Map<String, Object> field = new TreeMap<String, Object>();
            field.put(Constants.NAME, "name" + i);
            field.put(Constants.SORT_ORDER, i);
            field.put(Constants.ASCENDING_ORDER, i % 2 == 1);
            field.put(Constants.BOOST, "1.1");
            fields.add(field);
        }
        map.put(Constants.FIELDS, fields);
        return map;
    }

}

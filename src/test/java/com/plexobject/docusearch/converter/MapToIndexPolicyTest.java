package com.plexobject.docusearch.converter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.converter.Constants;
import com.plexobject.docusearch.converter.Converter;
import com.plexobject.docusearch.converter.MapToIndexPolicy;
import com.plexobject.docusearch.index.IndexPolicy;

public class MapToIndexPolicyTest {
    Converter<Map<String, Object>, IndexPolicy> converter;

    @Before
    public void setUp() throws Exception {
        converter = new MapToIndexPolicy();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testConvert() {
        final Map<String, Object> map = newIndexPolicyMap();
        final IndexPolicy policy = converter.convert(map);

        Assert.assertEquals(10, policy.getFields().size());
        Assert.assertEquals(10, policy.getScore());
        Assert.assertEquals(20.5, policy.getBoost(), 0.0001);
        Assert.assertEquals("std", policy.getAnalyzer());
        Assert.assertEquals("shahbhat", policy.getOwner());
        Assert.assertTrue("failed to find dictionary in " + map, policy
                .isAddToDictionary());

        Map<String, Integer> count = new HashMap<String, Integer>();

        for (int i = 0; i < 10; i++) {
            count.put("name" + i, new Integer(0));
        }
        for (IndexPolicy.Field field : policy.getFields()) {
            count.put(field.name, count.get(field.name) + 1);
            final int num = Integer.parseInt(field.name.replace("name", ""));
            Assert.assertEquals(num % 2 == 0, field.storeInIndex);

            Assert.assertEquals(num % 2 == 1, field.analyze);
            Assert.assertEquals(num % 2 != 1, field.tokenize);
            Assert.assertEquals(1.1F, field.boost, 0.001);

        }
        for (int i = 0; i < 10; i++) {
            int actual = count.get("name" + i);

            Assert.assertEquals(1, actual);
        }

    }

    private static Map<String, Object> newIndexPolicyMap() {
        final Map<String, Object> map = new HashMap<String, Object>();
        map.put(Constants.SCORE, 10);
        map.put(Constants.BOOST, 20.5);
        map.put(Constants.ANALYZER, "std");
        map.put(Constants.OWNER, "shahbhat");
        map.put(Constants.ADD_TO_DICTIONARY, Boolean.TRUE);
        final Collection<Map<String, Object>> fields = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 10; i++) {
            final Map<String, Object> field = new HashMap<String, Object>();
            field.put(Constants.NAME, "name" + i);
            field.put(Constants.STORE_IN_INDEX, i % 2 == 0);
            field.put(Constants.ANALYZE, i % 2 == 1);
            field.put(Constants.TOKENIZE, i % 2 != 1);
            field.put(Constants.BOOST, "1.1");
            fields.add(field);
        }
        map.put(Constants.FIELDS, fields);
        return map;
    }

}

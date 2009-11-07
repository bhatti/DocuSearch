package com.plexobject.docusearch.converter;

import junit.framework.Assert;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.query.RankedTerm;

public class JsonToRankedTermTest {
    JsonToRankedTerm converter;

    @Before
    public void setUp() throws Exception {
        converter = new JsonToRankedTerm();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testConvert() throws JSONException {
        JSONObject json = new JSONObject();
        json.put("name", "company");
        json.put("value", "ibm");
        json.put("frequency", "10");
        RankedTerm term = converter.convert(json);
        Assert.assertEquals("company", term.getName());
        Assert.assertEquals("ibm", term.getValue());
        Assert.assertEquals(10, term.getFrequency());
    }

}

package com.plexobject.docusearch.converter;

import junit.framework.Assert;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.query.RankedTerm;

public class RankedTermToJsonTest {
    RankedTermToJson converter;

    @Before
    public void setUp() throws Exception {
        converter = new RankedTermToJson();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testConvert() throws JSONException {
        RankedTerm term = new RankedTerm("name", "value", 10);
        JSONObject json = converter.convert(term);
        Assert.assertEquals("name", json.getString("name"));
        Assert.assertEquals("value", json.getString("value"));
        Assert.assertEquals("10", json.getString("frequency"));
    }

}

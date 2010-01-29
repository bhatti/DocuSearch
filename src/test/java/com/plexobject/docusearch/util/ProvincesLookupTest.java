package com.plexobject.docusearch.util;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.docs.DocumentPropertiesHelper;

public class ProvincesLookupTest {
    ProvincesLookup provincesLookup;

    @Before
    public void setUp() throws Exception {
        provincesLookup = new ProvincesLookup();
        provincesLookup
                .setDocumentPropertiesHelper(new DocumentPropertiesHelper());
        provincesLookup.afterPropertiesSet();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testGetStateNameByCode() {
        Assert.assertEquals("Illinois", provincesLookup.getStateNameByCode(
                "US", "IL"));
    }

    @Test
    public final void testGetStateCodes() {
        Assert.assertTrue(provincesLookup.getStateCodes("US").contains("IL"));
    }

}

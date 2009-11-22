package com.plexobject.docusearch.lucene.analyzer;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SearchSimilarityTest {
    private SearchSimilarity similarity;

    @Before
    public void setUp() throws Exception {
        similarity = new SearchSimilarity();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testQueryNorm() {
        Assert.assertEquals(0.31F, similarity.queryNorm(10), 0.1F);
    }

    @Test
    public final void testSloppyFreq() {
        Assert.assertEquals(0.0, similarity.sloppyFreq(10), 0.1F);
    }

    @Test
    public final void testTfFloat() {
        Assert.assertEquals(3.1F, similarity.tf(10), 0.1F);
    }

    @Test
    public final void testIdfIntInt() {
        Assert.assertEquals(1.59F, similarity.idf(10, 20), 0.1F);
    }

    @Test
    public final void testCoord() {
        Assert.assertEquals(0.5F, similarity.coord(10, 20), 0.1F);
    }

    @Test
    public final void testLengthNormStringInt() {
        Assert.assertEquals(0.316F, similarity.lengthNorm("field", 10), 0.1F);
    }

}

package com.plexobject.docusearch.lucene.analyzer;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BoostingSimilarityTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testScorePayloadStringByteArrayIntInt() {
        float score = new BoostingSimilarity().scorePayload("field",
                new byte[] { 1, 2, 3, 4 }, 0, 3);
        Assert.assertEquals(String.format("unexpected score %f", score), 0.0F,
                score, 0.0001F);
    }

    @Test
    public final void testScorePayloadStringByteArrayIntIntWithNull() {
        float score = new BoostingSimilarity()
                .scorePayload("field", null, 0, 0);
        Assert.assertEquals(1.0F, score, 0.0001F);
    }

}

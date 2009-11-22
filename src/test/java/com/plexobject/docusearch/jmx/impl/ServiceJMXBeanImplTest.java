package com.plexobject.docusearch.jmx.impl;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ServiceJMXBeanImplTest {
    private ServiceJMXBeanImpl mbean;

    @Before
    public void setUp() throws Exception {
        mbean = new ServiceJMXBeanImpl("service");
    }

    @After
    public void tearDown() throws Exception {
    }

    // @Test
    // TODO fix this
    public final void testGetAverageElapsedTimeInNanoSecs() {
        Assert.assertTrue(mbean.getAverageElapsedTimeInNanoSecs() > 0);
    }

    @Test
    public final void testGetSetProperty() {
        mbean.setProperty("name", "value");
        Assert.assertEquals("value", mbean.getProperty("name"));
    }

    // @Test
    // TODO fix this
    public final void testGetTotalDurationInNanoSecs() {
        Assert.assertTrue(mbean.getTotalDurationInNanoSecs() > 0);
    }

    @Test
    public final void testGetTotalErrors() {
        mbean.incrementError();
        Assert.assertEquals(1, mbean.getTotalErrors());
    }

    @Test
    public final void testGetTotalRequests() {
        mbean.incrementRequests();
        Assert.assertEquals(1, mbean.getTotalRequests());
    }

    @Test
    public final void testGetNotificationInfo() {
        Assert.assertEquals(1, mbean.getNotificationInfo().length);
    }

    @Test
    public final void testGetSetState() {
        mbean.setState("ready");
        Assert.assertEquals("ready", mbean.getState());
    }

}

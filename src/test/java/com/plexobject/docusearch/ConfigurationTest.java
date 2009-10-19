/**
 * 
 */
package com.plexobject.docusearch;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author bhatti@plexobject.com
 * 
 */
public class ConfigurationTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for
	 * {@link com.plexobject.docusearch.Configuration#getConfigDatabase()}.
	 */
	@Test
	public final void testGetConfigDatabase() {
		Assert.assertNotNull(Configuration.getInstance().getConfigDatabase());
	}

	/**
	 * Test method for
	 * {@link com.plexobject.docusearch.Configuration#getProperty(java.lang.String)}
	 * .
	 */
	@Test
	public final void testGetPropertyString() {
		Assert.assertNotNull(Configuration.getInstance().getProperty(
				"user.name"));
	}

	/**
	 * Test method for
	 * {@link com.plexobject.docusearch.Configuration#getProperty(java.lang.String)}
	 * .
	 */
	@Test
	public final void testGetPropertyStringWithNull() {
		Assert.assertNull(Configuration.getInstance().getProperty(
				"invalid.property"));
	}

	/**
	 * Test method for
	 * {@link com.plexobject.docusearch.Configuration#getProperty(java.lang.String)}
	 * .
	 */
	@Test
	public final void testGetPropertyStringWithDefault() {
		Assert.assertEquals("default", Configuration.getInstance().getProperty(
				"invalid.property", "default"));
	}

	/**
	 * Test method for
	 * {@link com.plexobject.docusearch.Configuration#getInteger(java.lang.String)}
	 * .
	 */
	@Test
	public final void testGetIntegerString() {
		Assert.assertEquals(0, Configuration.getInstance().getInteger(
				"invalid.property"));
	}

	/**
	 * Test method for
	 * {@link com.plexobject.docusearch.Configuration#getInteger(java.lang.String, int)}
	 * .
	 */
	@Test
	public final void testGetIntegerStringInt() {
		Assert.assertEquals(10, Configuration.getInstance().getInteger(
				"invalid.property", 10));
	}

	/**
	 * Test method for
	 * {@link com.plexobject.docusearch.Configuration#getDouble(java.lang.String)}
	 * .
	 */
	@Test
	public final void testGetDoubleString() {
		Assert.assertEquals(0.0, Configuration.getInstance().getDouble(
				"invalid.property"), 0.000001);
	}

	/**
	 * Test method for
	 * {@link com.plexobject.docusearch.Configuration#getDouble(java.lang.String, double)}
	 * .
	 */
	@Test
	public final void testGetDoubleStringDouble() {
		Assert.assertEquals(10.0, Configuration.getInstance().getDouble(
				"invalid.property", 10.0), 0.000001);
	}

	/**
	 * Test method for
	 * {@link com.plexobject.docusearch.Configuration#getBoolean(java.lang.String)}
	 * .
	 */
	@Test
	public final void testGetBooleanString() {
		Assert.assertFalse(Configuration.getInstance().getBoolean(
				"invalid.property"));
	}

	/**
	 * Test method for
	 * {@link com.plexobject.docusearch.Configuration#getBoolean(java.lang.String, boolean)}
	 * .
	 */
	@Test
	public final void testGetBooleanStringBoolean() {
		Assert.assertTrue(Configuration.getInstance().getBoolean(
				"invalid.property", true));
	}

	/**
	 * Test method for
	 * {@link com.plexobject.docusearch.Configuration#getLong(java.lang.String)}.
	 */
	@Test
	public final void testGetLongString() {
		Assert.assertEquals(0, Configuration.getInstance().getLong(
				"invalid.property"));
	}

	/**
	 * Test method for
	 * {@link com.plexobject.docusearch.Configuration#getLong(java.lang.String, long)}
	 * .
	 */
	@Test
	public final void testGetLongStringLong() {
		Assert.assertEquals(10, Configuration.getInstance().getLong(
				"invalid.property", 10));
	}

}

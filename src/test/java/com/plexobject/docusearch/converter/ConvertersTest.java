package com.plexobject.docusearch.converter;

import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.converter.QueryPolicyToMap;
import com.plexobject.docusearch.query.QueryPolicy;

public class ConvertersTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test(expected = NullPointerException.class)
	public final void testNullRegister1() {
		Converters.getInstance().register(null, Object.class, null);
	}

	@Test(expected = NullPointerException.class)
	public final void testNullRegister2() {
		Converters.getInstance().register(Object.class, null, null);
	}

	@Test(expected = NullPointerException.class)
	public final void testNullRegister3() {
		Converters.getInstance().register(Object.class, Object.class, null);
	}

	@Test
	public final void testRegister() {
		Converters.getInstance().register(QueryPolicy.class, Map.class,
				new QueryPolicyToMap());
	}

	@Test
	public final void testGetConverter() {
		final QueryPolicyToMap c = new QueryPolicyToMap();
		Converters.getInstance().register(QueryPolicy.class, Map.class, c);
		Assert.assertEquals(c, Converters.getInstance().getConverter(
				QueryPolicy.class, Map.class));
	}

	@Test(expected = NullPointerException.class)
	public final void testNullGetConverter1() {
		Converters.getInstance().getConverter(null, Object.class);
	}

	@Test(expected = NullPointerException.class)
	public final void testNullGetConverter2() {
		Converters.getInstance().getConverter(Object.class, null);
	}

}

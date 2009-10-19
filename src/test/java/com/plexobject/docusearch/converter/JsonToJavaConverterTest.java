/**
 * 
 */
package com.plexobject.docusearch.converter;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.plexobject.docusearch.converter.Converter;
import com.plexobject.docusearch.converter.JsonToJavaConverter;


/**
 * 
 * @author bhatti@plexobject.com
 */
public class JsonToJavaConverterTest {
	private Converter<Object, Object> converter;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		converter = new JsonToJavaConverter();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testNullConvert() {
		Assert.assertNull(converter.convert(null));
	}
	@Test
	public void testEmptyConvert() {
		Assert.assertEquals("", converter.convert(""));
	}
	@SuppressWarnings("unchecked")
	@Test
	public void testListConvert() {
		List<Object> result = (List<Object>) converter.convert("[1,\"One\",\"Two\",3]");
		Assert.assertEquals(4, result.size());
		Assert.assertEquals(1, result.get(0));
		Assert.assertEquals("One", result.get(1));
		Assert.assertEquals("Two", result.get(2));
		Assert.assertEquals(3, result.get(3));

	}
	@SuppressWarnings("unchecked")
	@Test
	public void testMapConvert() {
		final Map<String, String> result = (Map<String, String>) converter.convert("{\"A\":\"1\",\"B\":\"2\"}");
		Assert.assertEquals(2, result.size());
		Assert.assertEquals("1", result.get("A"));
		Assert.assertEquals("2", result.get("B"));
	}
	@SuppressWarnings("unchecked")
	@Test
	public void testMapWithinListConvert() {
		final Map<String, String> map1 = new TreeMap<String, String>();
		map1.put("A", "1");
		map1.put("B", "2");
		final Map<String, String> map2 = new TreeMap<String, String>();
		map2.put("C", "3");
		map2.put("D", "4");
		final Map<String, String> map3 = new TreeMap<String, String>();

		final List<? extends Object> arr1 = Arrays.asList(11, "12", 13, "14");
		final List<? extends Object> arr2 = Arrays.asList();

		List<Object> result = (List<Object>) converter.convert("[0,{\"A\":\"1\",\"B\":\"2\"},\"0\",{\"C\":\"3\",\"D\":\"4\"},[11,\"12\",13,\"14\"],{},[]]");
		Assert.assertEquals(7, result.size());
		Assert.assertEquals(0, result.get(0));
		Assert.assertEquals(map1, result.get(1));
		Assert.assertEquals("0", result.get(2));
		Assert.assertEquals(map2, result.get(3));
		Assert.assertEquals(arr1, result.get(4));
		Assert.assertEquals(map3, result.get(5));
		Assert.assertEquals(arr2, result.get(6));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testListWithinMapConvert() {
		final List<? extends Object> arr1 = Arrays.asList(11, "12", 13, "14");
		final List<? extends Object> arr2 = Arrays.asList();

		final Map<String, String> result = (Map<String, String>) converter.convert("{\"A\":[11,\"12\",13,\"14\"],\"B\":[],\"D\":\"4\"}");
		Assert.assertEquals(3, result.size());
		Assert.assertEquals(arr1, result.get("A"));
		Assert.assertEquals(arr2, result.get("B"));
		Assert.assertEquals("4", result.get("D"));
	}

}

package com.plexobject.docusearch.converter;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.plexobject.docusearch.converter.Converter;
import com.plexobject.docusearch.converter.JavaToJsonConverter;

/**
 * 
 * @author bhatti@plexobject.com
 */
public class JavaToJsonConverterTest {
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
		converter = new JavaToJsonConverter();
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

	@Test
	public void testStringConvert() {
		Assert.assertEquals("xxx", converter.convert("xxx"));
	}

	@Test
	public void testLongConvert() {
		Assert.assertEquals(1L, converter.convert(1L));
	}

	@Test
	public void testIntegerConvert() {
		Assert.assertEquals(1, converter.convert(1));
	}

	@Test
	public void testDoubleConvert() {
		Assert.assertEquals(1.1, converter.convert(1.1));
	}

	@Test
	public void testFloatConvert() {
		Assert.assertEquals(1.1F, converter.convert(1.1F));
	}

	@Test
	public void testCharConvert() {
		Assert.assertEquals('X', converter.convert('X'));
	}

	@Test
	public void testBigDecimalConvert() {
		Assert.assertEquals(new BigDecimal("1.0"), converter
				.convert(new BigDecimal("1.0")));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDateConvert() {
		converter.convert(new Date());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testListConvert() throws Exception {
		final List<? extends Object> input = Arrays.asList(1, "One", "Two", 3);
		final JSONArray expected = new JSONArray("[1,\"One\",\"Two\",3]");
		Assert.assertEquals(expected.toString(), converter.convert(input)
				.toString());
	}

	@Test
	public void testMapConvert() throws Exception {
		final Map<String, String> input = new TreeMap<String, String>();
		input.put("A", "1");
		input.put("B", "2");
		final JSONObject expected = new JSONObject("{\"A\":\"1\",\"B\":\"2\"}");
		Assert.assertEquals(expected.toString(), converter.convert(input)
				.toString());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMapWithinListConvert() throws Exception {
		final Map<String, String> map1 = new TreeMap<String, String>();
		map1.put("A", "1");
		map1.put("B", "2");
		final Map<String, String> map2 = new TreeMap<String, String>();
		map2.put("C", "3");
		map2.put("D", "4");
		final Map<String, String> map3 = new TreeMap<String, String>();

		final List<? extends Object> arr1 = Arrays.asList(11, "12", 13, "14");
		final List<? extends Object> arr2 = Arrays.asList();

		final List<? extends Object> input = Arrays.asList(0, map1, "0", map2,
				arr1, map3, arr2);
		final JSONArray expected = new JSONArray(
				"[0,{\"A\":\"1\",\"B\":\"2\"},\"0\",{\"C\":\"3\",\"D\":\"4\"},[11,\"12\",13,\"14\"],{},[]]");
		Assert.assertEquals(expected.toString(), converter.convert(input)
				.toString());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testListWithinMapConvert() throws Exception {
		final List<? extends Object> arr1 = Arrays.asList(11, "12", 13, "14");
		final List<? extends Object> arr2 = Arrays.asList();

		final Map<String, Object> input = new TreeMap<String, Object>();
		input.put("A", arr1);
		input.put("B", arr2);
		input.put("D", "4");

		final JSONObject expected = new JSONObject(
				"{\"A\":[11,\"12\",13,\"14\"],\"B\":[],\"D\":\"4\"}");
		Assert.assertEquals(expected.toString(), converter.convert(input)
				.toString());
	}

}

package com.plexobject.docusearch.domain;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.domain.Document;

/**
 * 
 * @author Shahzad Bhatti
 */
public class DocumentTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullDocument() {
		new DocumentBuilder((String)null).build();
	}

	@Test(expected = NullPointerException.class)
	public void testNullMapDocument() {
		Map<String, Object> map = null;
		new Document(map);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmptyDocument() {
		new DocumentBuilder("").build();
	}

	@Test
	public void testDefaultDocument() {
		final Document doc = new DocumentBuilder("db").build();
		Assert.assertEquals(1, doc.size());
		Assert.assertEquals("db", doc.getDatabase());
		Assert.assertNull(doc.getId());
	}

	@Test
	public void testDocument() {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("One", "1");
		map.put("Two", "2");
		final Document doc = new DocumentBuilder("db").putAll(map).setId("id")
				.setRevision("rev").build();
		Assert.assertEquals(5, doc.size());
		Assert.assertEquals("db", doc.getDatabase());
		Assert.assertEquals("id", doc.getId());
		Assert.assertEquals("rev", doc.getRevision());
		Assert.assertEquals("1", doc.get("One"));
		Assert.assertEquals("2", doc.get("Two"));
	}

}

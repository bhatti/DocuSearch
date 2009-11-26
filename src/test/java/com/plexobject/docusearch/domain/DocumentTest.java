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

    @Test
    public void testIsValidAttribute() {
        Assert.assertTrue(Document.isValidAttributeKey("key"));
        Assert.assertFalse(Document.isValidAttributeKey(Document.ID));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullDocument() {
        new DocumentBuilder((String) null).build();
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
    public void testhasDatabase() {
        final Document doc = new DocumentBuilder("db").build();
        Assert.assertTrue(doc.hasDatabase());
    }

    @Test
    public void testIsEmpty() {
        final Document doc = new DocumentBuilder("db").build();
        Assert.assertFalse(doc.isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void testContainsKeyNull() {
        final Document doc = new DocumentBuilder("db").build();
        doc.containsKey(null);
    }

    @Test(expected = ClassCastException.class)
    public void testContainsKeyNonString() {
        final Document doc = new DocumentBuilder("db").build();
        doc.containsKey(1);
    }

    @Test(expected = NullPointerException.class)
    public void testGetKeyNull() {
        final Document doc = new DocumentBuilder("db").build();
        doc.get(null);
    }

    @Test(expected = ClassCastException.class)
    public void testGetKeyNonString() {
        final Document doc = new DocumentBuilder("db").build();
        doc.get(1);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsValueNull() {
        final Document doc = new DocumentBuilder("db").build();
        doc.containsValue(null);
    }

    @Test(expected = ClassCastException.class)
    public void testContainsValueNonString() {
        final Document doc = new DocumentBuilder("db").build();
        doc.containsValue(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPut() {
        final Document doc = new DocumentBuilder("db").build();
        doc.put("name", "value");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutAll() {
        final Document doc = new DocumentBuilder("db").build();
        doc.putAll(new HashMap<String, Object>());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        final Document doc = new DocumentBuilder("db").build();
        doc.remove("name");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testClear() {
        final Document doc = new DocumentBuilder("db").build();
        doc.clear();
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

    @Test
    public void testKeySet() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("One", "1");
        map.put("Two", "2");
        final Document doc = new DocumentBuilder("db").putAll(map).setId("id")
                .setRevision("rev").build();
        Assert.assertEquals(5, doc.keySet().size());
    }

    @Test
    public void testValues() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("One", "1");
        map.put("Two", "2");
        final Document doc = new DocumentBuilder("db").putAll(map).setId("id")
                .setRevision("rev").build();
        Assert.assertEquals(5, doc.values().size());
    }

    @Test
    public void testEquals() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("One", "1");
        map.put("Two", "2");
        final Document doc1 = new DocumentBuilder("db").putAll(map).setId("id")
                .setRevision("rev").build();
        final Document doc2 = new DocumentBuilder("db").putAll(map).setId("id")
                .setRevision("rev").build();

        Assert.assertTrue(doc1.equals(doc2));
    }

    @Test
    public void testHashCode() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("One", "1");
        map.put("Two", "2");
        final Document doc = new DocumentBuilder("db").putAll(map).setId("id")
                .setRevision("rev").build();
        Assert.assertTrue(doc.hashCode() < 0);
    }

    @Test
    public void testGetPropertyWithDefault() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("One", "1");
        map.put("Two", "2");
        final Document doc = new DocumentBuilder("db").putAll(map).setId("id")
                .setRevision("rev").build();
        Assert.assertEquals("2", doc.getProperty("XXX", "2"));
    }

    @Test
    public void testGetInteger() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("One", "1");
        map.put("Two", "2");
        final Document doc = new DocumentBuilder("db").putAll(map).setId("id")
                .setRevision("rev").build();
        Assert.assertEquals(1, doc.getInteger("One"));
    }

    @Test
    public void testGetDouble() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("One", "1");
        map.put("Two", "2");
        final Document doc = new DocumentBuilder("db").putAll(map).setId("id")
                .setRevision("rev").build();
        Assert.assertEquals(1.0, doc.getDouble("One"), 0.001);
    }

    @Test
    public void testGetBoolean() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("One", "1");
        map.put("Two", "2");
        final Document doc = new DocumentBuilder("db").putAll(map).setId("id")
                .setRevision("rev").build();
        Assert.assertFalse(doc.getBoolean("xx"));
    }
}

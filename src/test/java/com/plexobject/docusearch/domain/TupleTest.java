package com.plexobject.docusearch.domain;

import org.junit.Assert;
import org.junit.Test;

import com.plexobject.docusearch.domain.Tuple;

/**
 * 
 * @author bhatti@plexobject.com
 */
public class TupleTest {
	@Test
	public void testTupleCreate() {
		final Tuple tuple = new Tuple();
		Assert.assertEquals(0, tuple.size());
	}

	@Test(expected = NullPointerException.class)
	public void testTupleNull() {
		final Tuple tuple = new Tuple((Object[]) null);
		Assert.assertEquals(0, tuple.size());
	}

	@Test
	public void testNth() {
		final Tuple tuple = new Tuple(1, 2, 3, 4, 5);
		Assert.assertEquals(5, tuple.size());
		Assert.assertEquals(1, tuple.first());
		Assert.assertEquals(2, tuple.second());
		Assert.assertEquals(3, tuple.third());
		Assert.assertEquals(5, tuple.last());
	}

	@Test
	public void testEquals() {
		final Tuple tuple1 = new Tuple(1, 2, 3, 4, 5);
		final Tuple tuple2 = new Tuple(1, 2, 3, 4, 5);
		Assert.assertEquals(tuple1, tuple2);
		final Tuple tuple3 = new Tuple(1, 2, 3, 4);
		Assert.assertFalse(tuple1.equals(tuple3));
		Assert.assertFalse(tuple1.equals(1));
	}

}

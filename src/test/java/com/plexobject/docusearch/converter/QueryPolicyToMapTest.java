package com.plexobject.docusearch.converter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.query.QueryPolicy;

public class QueryPolicyToMapTest {
	Converter<QueryPolicy, Map<String, Object>> converter;

	@Before
	public void setUp() throws Exception {
		converter = new QueryPolicyToMap();
	}

	@After
	public void tearDown() throws Exception {
	}

	@SuppressWarnings("unchecked")
	@Test
	public final void testConvert() throws JSONException {
		final Map<String, Object> policy = converter.convert(newQueryPolicy());

		Collection<String> fields = (Collection<String>) policy
				.get(Constants.FIELDS);
		Assert.assertEquals(10, fields.size());
		Map<String, Integer> count = new HashMap<String, Integer>();

		for (int i = 0; i < 10; i++) {
			count.put("name" + i, new Integer(0));
		}

		for (String name : fields) {
			count.put(name, count.get(name) + 1);

		}
		for (int i = 0; i < 10; i++) {
			int actual = count.get("name" + i);

			Assert.assertEquals(1, actual);
		}

	}

	private static QueryPolicy newQueryPolicy() {
		final QueryPolicy policy = new QueryPolicy();

		for (int i = 0; i < 10; i++) {
			policy.add("name" + i);
		}
		return policy;
	}

}

package com.plexobject.docusearch.converter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.converter.Constants;
import com.plexobject.docusearch.converter.Converter;
import com.plexobject.docusearch.converter.IndexPolicyToMap;
import com.plexobject.docusearch.index.IndexPolicy;

public class IndexPolicyToMapTest {
	Converter<IndexPolicy, Map<String, Object>> converter;

	@Before
	public void setUp() throws Exception {
		converter = new IndexPolicyToMap();
	}

	@After
	public void tearDown() throws Exception {
	}

	@SuppressWarnings("unchecked")
	@Test
	public final void testConvert() throws JSONException {
		final Map<String, Object> policy = converter.convert(newIndexPolicy());
		Assert.assertEquals(new Integer(10), policy.get(Constants.SCORE));
		Assert.assertEquals(new Float(20.5), policy.get(Constants.BOOST));
		final Collection<Map<String, Object>> fields = (Collection<Map<String, Object>>) policy
				.get(Constants.FIELDS);
		Assert.assertEquals(10, fields.size());
		Map<String, Integer> count = new HashMap<String, Integer>();

		for (int i = 0; i < 10; i++) {
			count.put("name" + i, new Integer(0));
		}

		for (Map<String, Object> field : fields) {
			final String name = (String) field.get(Constants.NAME);
			final int num = Integer.parseInt(name.replace("name", ""));
			count.put(name, count.get(name) + 1);
			Assert.assertEquals(Boolean.valueOf(num % 2 == 0), field
					.get(Constants.STORE_IN_INDEX));
			Assert.assertEquals(Boolean.valueOf(num % 2 == 1), field
					.get(Constants.ANALYZE));
			Assert
					.assertEquals(Float.valueOf(1.1F), field
							.get(Constants.BOOST));
		}
		for (int i = 0; i < 10; i++) {
			int actual = count.get("name" + i);

			Assert.assertEquals(1, actual);
		}

	}

	private static IndexPolicy newIndexPolicy() {
		final IndexPolicy policy = new IndexPolicy();
		policy.setScore(10);
		policy.setBoost(20.5F);
		for (int i = 0; i < 10; i++) {
			policy.add("name" + i, i % 2 == 0, i % 2 == 1, i % 2 == 1, 1.1F);
		}
		return policy;
	}

}

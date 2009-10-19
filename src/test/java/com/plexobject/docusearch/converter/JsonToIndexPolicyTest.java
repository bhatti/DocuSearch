package com.plexobject.docusearch.converter;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.index.IndexPolicy;

public class JsonToIndexPolicyTest {
	Converter<JSONObject, IndexPolicy> converter;

	@Before
	public void setUp() throws Exception {
		converter = new JsonToIndexPolicy();

	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testConvert() throws JSONException {
		final JSONObject json = newIndexPolicyJSONObject();
		final IndexPolicy policy = converter.convert(json);
		Assert.assertEquals(10, policy.getScore());
		Assert.assertEquals(20.0F, policy.getBoost(), 0.001);
		Assert.assertEquals(10, policy.getFields().size());
		Map<String, Integer> count = new HashMap<String, Integer>();

		for (int i = 0; i < 10; i++) {
			count.put("name" + i, new Integer(0));
		}
		for (IndexPolicy.Field field : policy.getFields()) {
			count.put(field.name, count.get(field.name) + 1);
		    Assert.assertEquals(1.1F, field.boost, 0.001);
		}
		for (int i = 0; i < 10; i++) {
			int actual = count.get("name" + i);

			Assert.assertEquals(1, actual);
		}
	}

	private static JSONObject newIndexPolicyJSONObject() throws JSONException {
		final JSONObject policy = new JSONObject();
		final JSONArray fields = new JSONArray();
		for (int i = 0; i < 10; i++) {
			final JSONObject field = new JSONObject();
			field.put(Constants.NAME, "name" + i);
			field.put(Constants.STORE_IN_INDEX, "false");
			field.put(Constants.ANALYZE, "true");
			field.put(Constants.BOOST, "1.1");
			fields.put(field);
		}
		policy.put(Constants.SCORE, 10);
		policy.put(Constants.BOOST, 20);
		policy.put(Constants.FIELDS, fields);
		return policy;
	}

}

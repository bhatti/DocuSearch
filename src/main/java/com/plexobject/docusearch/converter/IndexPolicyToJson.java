package com.plexobject.docusearch.converter;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.plexobject.docusearch.index.IndexPolicy;

/**
 * 
 * @author Shahzad Bhatti
 * 
 */
public class IndexPolicyToJson implements Converter<IndexPolicy, JSONObject> {
	/**
	 * 
	 * @param IndexPolicy
	 * 
	 * @return JSON object
	 */
	@Override
	public JSONObject convert(final IndexPolicy policy) {
		final JSONObject value = new JSONObject();

		if (policy != null) {
			try {
				value.put(Constants.SCORE, policy.getScore());
				value.put(Constants.BOOST, policy.getBoost());
				value.put(Constants.ANALYZER, policy.getAnalyzer());
				value.put(Constants.ADD_TO_DICTIONARY, policy.isAddToDictionary());
				value.put(Constants.OWNER, policy.getOwner());
				final JSONArray jsonFields = new JSONArray();
				for (IndexPolicy.Field field : policy.getFields()) {
					JSONObject jsonField = new JSONObject();
					jsonField.put(Constants.NAME, field.name);
					jsonField.put(Constants.STORE_IN_INDEX, field.storeInIndex);
					jsonField.put(Constants.TOKENIZE, field.tokenize);
					jsonField.put(Constants.ANALYZE, field.analyze);
					jsonField.put(Constants.BOOST, field.boost);
					jsonFields.put(jsonField);
				}
				value.put(Constants.FIELDS, jsonFields);
			} catch (JSONException e) {
				throw new ConversionException("failed to convert " + policy, e);
			}

		}
		return value;
	}
}

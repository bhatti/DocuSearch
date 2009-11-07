package com.plexobject.docusearch.converter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.plexobject.docusearch.index.IndexPolicy;

/**
 * 
 * @author Shahzad Bhatti
 * 
 */
public class IndexPolicyToMap implements
		Converter<IndexPolicy, Map<String, Object>> {
	/**
	 * 
	 * @param IndexPolicy
	 * 
	 * @return Map
	 */
	@Override
	public Map<String, Object> convert(final IndexPolicy policy) {
		final Map<String, Object> value = new HashMap<String, Object>();

		if (policy != null) {
			value.put(Constants.SCORE, policy.getScore());
			value.put(Constants.BOOST, policy.getBoost());
			value.put(Constants.ANALYZER, policy.getAnalyzer());
			value.put(Constants.ADD_TO_DICTIONARY, policy.isAddToDictionary());
			final List<Object> mapFields = new ArrayList<Object>();
			for (IndexPolicy.Field field : policy.getFields()) {
				final Map<String, Object> mapField = new HashMap<String, Object>();

				mapField.put(Constants.NAME, field.name);
				mapField.put(Constants.STORE_IN_INDEX, field.storeInIndex);
				mapField.put(Constants.TOKENIZE, field.tokenize);
				mapField.put(Constants.ANALYZE, field.analyze);
				mapField.put(Constants.BOOST, field.boost);
				mapFields.add(mapField);
			}
			value.put(Constants.FIELDS, mapFields);
		}
		return value;
	}
}

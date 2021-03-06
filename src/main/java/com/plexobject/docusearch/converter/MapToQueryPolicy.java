package com.plexobject.docusearch.converter;

import java.util.List;
import java.util.Map;

import com.plexobject.docusearch.query.QueryPolicy;

/**
 * 
 * @author Shahzad Bhatti
 * 
 */
public class MapToQueryPolicy implements
        Converter<Map<String, Object>, QueryPolicy> {
    /**
     * @param value
     *            - Map object
     * @return QueryPolicy
     */
    @Override
    public QueryPolicy convert(final Map<String, Object> value) {
        final QueryPolicy policy = new QueryPolicy();
        if (value != null) {
            doConvert(value, policy);
        }
        return policy;
    }

    @SuppressWarnings("unchecked")
    void doConvert(final Map<String, Object> value, final QueryPolicy policy) {
        final String sortingMultiplier = (String) value
                .get(Constants.SORTING_MULTIPLIER);
        policy.setSortingMultiplier(sortingMultiplier);
        final String analyzer = (String) value
                .get(Constants.ANALYZER);
        policy.setAnalyzer(analyzer);
        final List<Object> fields = (List<Object>) value.get(Constants.FIELDS);
        if (fields != null) {
            for (Object f : fields) {
                final Map<String, Object> field = (Map<String, Object>) f;
                final String name = (String) field.get(Constants.NAME);

                int sortOrder = 0;
                if (field.containsKey(Constants.SORT_ORDER)) {
                    sortOrder = Integer.parseInt(field
                            .get(Constants.SORT_ORDER).toString());
                }

                boolean ascendingSort = true;
                if (field.containsKey(Constants.ASCENDING_ORDER)) {
                    ascendingSort = Boolean.valueOf(field.get(
                            Constants.ASCENDING_ORDER).toString());
                }
                int fieldType = 0;
                if (field.containsKey(Constants.FIELD_TYPE)) {
                    fieldType = Integer.parseInt(field
                            .get(Constants.SORT_ORDER).toString());
                }

                float boost = 0.0F;
                if (field.containsKey(Constants.BOOST)) {
                    boost = Float
                            .valueOf(field.get(Constants.BOOST).toString())
                            .floatValue();
                }

                policy.add(name, sortOrder, ascendingSort, boost,
                        QueryPolicy.FieldType.fromType(fieldType));
            }
        }
    }
}

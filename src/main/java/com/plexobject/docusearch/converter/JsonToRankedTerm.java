package com.plexobject.docusearch.converter;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.plexobject.docusearch.query.RankedTerm;

public class JsonToRankedTerm implements Converter<JSONObject, RankedTerm> {
    /**
     * @param value
     *            - JSON object
     * @return IndexPolicy
     */
    @Override
    public RankedTerm convert(final JSONObject value) {
        try {
            return new RankedTerm(value.getString(Constants.NAME), value
                    .getString(Constants.VALUE), value
                    .getInt(Constants.FREQUENCY));
        } catch (JSONException e) {
            throw new ConversionException("failed to convert json", e);
        }
    }
}

package com.plexobject.docusearch.converter;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.plexobject.docusearch.query.RankedTerm;

public class RankedTermToJson implements Converter<RankedTerm, JSONObject> {
    /**
     * 
     * @param RankedTerm
     * 
     * @return JSON object
     */
    @Override
    public JSONObject convert(final RankedTerm term) {
        final JSONObject value = new JSONObject();

        if (term != null) {
            try {
                value.put(Constants.NAME, term.getName());
                value.put(Constants.VALUE, term.getValue());
                value.put(Constants.FREQUENCY, term.getFrequency());
            } catch (JSONException e) {
                throw new ConversionException("failed to convert " + term, e);
            }
        }
        return value;
    }

}

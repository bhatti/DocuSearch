package com.plexobject.docusearch.persistence.bdb;

import org.apache.commons.validator.GenericValidator;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.plexobject.docusearch.converter.ConversionException;
import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
class JsonDocument {
    @PrimaryKey
    private String id;
    private String json;

    JsonDocument() {
    }

    JsonDocument(String id, Document doc) {
        if (GenericValidator.isBlankOrNull(id)) {
            throw new IllegalArgumentException("id name not specified");
        }
        if (null == doc) {
            throw new IllegalArgumentException("doc name not specified");
        }
        this.id = id;
        this.json = Converters.getInstance().getConverter(Object.class,
                JSONObject.class).convert(doc).toString();
    }

    void setId(String id) {
        this.id = id;
    }

    String getId() {
        return id;
    }

    void setJson(final String json) {
        this.json = json;
    }

    String getJson() {
        return json;
    }

    Document getDocument() {
        try {
            JSONObject jsonDoc = new JSONObject(json);
            return new DocumentBuilder(Converters.getInstance().getConverter(
                    JSONObject.class, Document.class).convert(jsonDoc)).build();
        } catch (JSONException e) {
            throw new ConversionException("Failed to convert " + json, e);
        }
    }
}

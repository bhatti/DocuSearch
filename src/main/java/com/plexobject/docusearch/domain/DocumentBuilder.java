package com.plexobject.docusearch.domain;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This class represents document stored in the document-based database.
 * 
 * @author Shahzad Bhatti
 */
public class DocumentBuilder implements Builder<Document> {
    protected final Map<String, Object> properties = new HashMap<String, Object>();

    public DocumentBuilder() {
    }

    public DocumentBuilder(final String database) {
        setDatabase(database);
    }

    public DocumentBuilder(Map<? extends String, ? extends Object> properties) {
        putAll(properties);
    }

    public DocumentBuilder setDatabase(final String database) {
        this.properties.put(Document.DATABASE, database);
        return this;
    }

    public DocumentBuilder setId(final String id) {
        this.properties.put(Document.ID, id);
        return this;
    }

    public DocumentBuilder setRevision(final String rev) {
        this.properties.put(Document.REVISION, rev);
        return this;
    }

    /**
     * @param name
     *            - name of the attribute
     * @param value
     *            - attribute value
     * @return - previous attribute value associated with specified name, or
     *         null if there was no mapping for attribute name
     * @throws ClassCastException
     *             - if the name is of an inappropriate type.
     * @throws IllegalArgumentException
     *             - if length of name is zero.
     * @throws NullPointerException
     *             - if the name or value is null.
     */
    public DocumentBuilder put(final String name, final Object value) {
        if (name == null) {
            throw new NullPointerException("name is null");
        }
        if (value == null) {
            throw new NullPointerException("value is null");
        }
        if (name.length() == 0) {
            throw new IllegalArgumentException("Length of name is zero");
        }
        if (!(value instanceof String) && !(value instanceof Boolean)
                && !(value instanceof Collection) && !(value instanceof Number)
                && !(value instanceof Map)) {
            throw new ClassCastException("Illegal type ["
                    + value.getClass().getName()
                    + "], only String or Collection is supported");
        }
        properties.put(name, value);
        return this;
    }

    /**
     * @param properties
     *            - properties to add
     * @throws ClassCastException
     *             - if the name or value is of an inappropriate type.
     * @throws IllegalArgumentException
     *             - if length of name is zero.
     * @throws NullPointerException
     *             - if the name or value is null.
     */
    public DocumentBuilder putAll(
            Map<? extends String, ? extends Object> properties) {
        for (Entry<? extends String, ? extends Object> e : properties
                .entrySet()) {
            if (e.getKey() != null && e.getValue() != null) {
                put(e.getKey(), e.getValue());
            }
        }
        return this;
    }

    @Override
    public Document build() {
        return new Document(properties);
    }

}

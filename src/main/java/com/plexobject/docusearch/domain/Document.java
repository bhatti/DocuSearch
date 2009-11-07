package com.plexobject.docusearch.domain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.validator.GenericValidator;

/**
 * This class represents document stored in the document-based database. Note:
 * This is immutable and non-thread safe class.
 * 
 * @author Shahzad Bhatti
 */
public class Document implements Map<String, Object> {
    public static final String DATABASE = "dbname";
    public static final String ID = "_id";
    public static final String REVISION = "_rev";

    private final Map<String, Object> properties;

    public Document(final Map<String, Object> properties) {
        if (properties == null) {
            throw new NullPointerException("properties not specified");
        }

        if (GenericValidator.isBlankOrNull((String) properties
                .get(Document.DATABASE))) {
            throw new IllegalArgumentException("database not specified in "
                    + properties);
        }
        this.properties = Collections.unmodifiableMap(properties);

    }

    public Map<String, Object> getAttributes() {
        final Map<String, Object> target = new HashMap<String, Object>();
        for (Map.Entry<String, Object> e : entrySet()) {
            if (isValidAttributeKey(e.getKey())) {
                target.put(e.getKey(), e.getValue());
            }
        }
        return target;
    }

    public Collection<String> getAttributeNames() {
        final Collection<String> names = new ArrayList<String>();
        for (Map.Entry<String, Object> e : entrySet()) {
            if (isValidAttributeKey(e.getKey())) {
                names.add(e.getKey());
            }
        }
        return names;
    }

    public static boolean isValidAttributeKey(final String key) {
        return key != null && !key.startsWith("_") && !key.equals(DATABASE);
    }

    /**
     * @return - the name of the database that stores this document.
     */
    public String getDatabase() {
        return (String) properties.get(DATABASE);
    }

    /**
     * 
     * @return true if database attribute is defined
     */
    public boolean hasDatabase() {
        return !GenericValidator.isBlankOrNull(getDatabase());
    }

    /**
     * @return - unique uuid that identifies this document
     */
    public String getId() {
        return (String) properties.get(ID);
    }

    /**
     * @return - revision number
     */
    public String getRevision() {
        return (String) properties.get(REVISION);
    }

    /**
     * Returns the number of high level attributes, where an attribute can be a
     * simple String or Map of String/Object.
     * 
     * @return - the number of high level attributes
     */
    @Override
    public int size() {
        return properties.size();
    }

    /**
     * @return - true if this document contains no attributes
     */
    @Override
    public boolean isEmpty() {
        return properties.isEmpty();
    }

    /**
     * @param name
     *            - name of the attribute
     * @return - true if this document contains attrbute for the specified name.
     * @throws ClassCastException
     *             - if the name is of an inappropriate type.
     * @throws NullPointerException
     *             - if the name is null.
     */
    @Override
    public boolean containsKey(final Object name) {
        if (name == null) {
            throw new NullPointerException("name is null");
        }
        if (!(name instanceof String)) {
            throw new ClassCastException("Illegal type ["
                    + name.getClass().getName() + "], only String is supported");
        }
        return properties.containsKey(name);
    }

    /**
     * @param value
     *            - attribute value
     * @return - true if this document contains one or more attributes to the
     *         specified value.
     * @throws ClassCastException
     *             - if the value is not of String or Map type.
     * @throws NullPointerException
     *             - if the value is null.
     */
    @Override
    public boolean containsValue(final Object value) {
        if (value == null) {
            throw new NullPointerException("value is null");
        }
        if (!(value instanceof String) && !(value instanceof Map)) {
            throw new ClassCastException("Illegal type ["
                    + value.getClass().getName()
                    + "], only String or Map is supported");
        }
        return properties.containsValue(value);
    }

    /**
     * @param name
     *            - name of the attribute
     * @return - value of attribute for the specified name.
     * @throws ClassCastException
     *             - if the name is of an inappropriate type.
     * @throws NullPointerException
     *             - if the name is null.
     */
    @Override
    public Object get(final Object name) {
        if (name == null) {
            throw new NullPointerException("name is null");
        }
        if (!(name instanceof String)) {
            throw new ClassCastException("Illegal type ["
                    + name.getClass().getName() + "], only String is supported");
        }
        return properties.get(name);
    }

    /**
     * This method is not supported and throws UnsupportedOperationException
     */
    @Override
    public Object put(final String name, final Object value) {
        throw new UnsupportedOperationException();
    }

    /**
     * This method is not supported and throws UnsupportedOperationException
     */
    @Override
    public Object remove(final Object name) {
        throw new UnsupportedOperationException();
    }

    /**
     * This method is not supported and throws UnsupportedOperationException
     */
    @Override
    public void putAll(Map<? extends String, ? extends Object> properties) {
        throw new UnsupportedOperationException();
    }

    /**
     * This method is not supported and throws UnsupportedOperationException
     */
    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return - returns a set of names contained in the document.
     */
    @Override
    public Set<String> keySet() {
        return properties.keySet();
    }

    /**
     * @return - returns a collection of attribute values contained in the
     *         document.
     */
    @Override
    public Collection<Object> values() {
        return properties.values();
    }

    /**
     * @return - returns a set of attributes contained in the document.
     */
    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        return properties.entrySet();
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof Document)) {
            return false;
        }
        Document rhs = (Document) object;
        return new EqualsBuilder().append(this.getId(), rhs.getId()).isEquals();
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(786529047, 1924536713).append(this.getId())
                .toHashCode();
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("id", this.getId()).append(
                "map", this.properties).toString();
    }

    public String getProperty(final String key) {
        return getProperty(key, null);
    }

    public String getProperty(final String key, final String def) {
        final String value = (String) get(key);
        return value != null ? value : def;
    }

    public int getInteger(final String key) {
        return getInteger(key, 0);
    }

    public int getInteger(final String key, final int def) {
        return Integer.parseInt(getProperty(key, String.valueOf(def)));
    }

    public double getDouble(final String key) {
        return getDouble(key, 0);
    }

    public double getDouble(final String key, final double def) {
        return Double.valueOf(getProperty(key, String.valueOf(def)))
                .doubleValue();
    }

    public boolean getBoolean(final String key) {
        return getBoolean(key, false);
    }

    public boolean getBoolean(final String key, final boolean def) {
        return Boolean.valueOf(getProperty(key, String.valueOf(def)))
                .booleanValue();
    }

}

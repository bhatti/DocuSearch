package com.plexobject.docusearch.query;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class QueryPolicy {
    private final Map<String, Field> fields = new TreeMap<String, Field>();

    public enum FieldType {
        STRING(0), INTEGER(1), DOUBLE(2);
        private int type;

        private FieldType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }

        public static FieldType fromType(int type) {
            switch (type) {
            case 0:
                return STRING;
            case 1:
                return INTEGER;
            case 2:
                return DOUBLE;
            default:
                throw new IllegalArgumentException("invalid type " + type);
            }
        }
    }

    public static class Field implements Comparable<Field> {
        public final String name;
        public final int sortOrder; // used for sorting, this field must be
        // indexed, but should not be tokenized, and
        // does not need to be stored (unless you
        // happen to want it back with the rest of
        // your document data).
        public final boolean ascendingSort;
        public final float boost;
        public final FieldType fieldType;

        public Field(final String name) {
            this(name, 0, true, 0.0F, FieldType.STRING);
        }

        public Field(final String name, final int sortOrder,
                final boolean ascendingSort, final float boost,
                final FieldType fieldType) {
            this.name = name;
            this.sortOrder = sortOrder;
            this.ascendingSort = ascendingSort;
            this.boost = boost;
            this.fieldType = fieldType;
        }

        /**
         * @see java.lang.Object#equals(Object)
         */
        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Field)) {
                return false;
            }
            Field rhs = (Field) object;
            return new EqualsBuilder().append(this.name, rhs.name).isEquals();
        }

        /**
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return new HashCodeBuilder(786529047, 1924536713).append(this.name)
                    .toHashCode();
        }

        /**
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return new ToStringBuilder(this).append("name", this.name).append(
                    "sortOrder", sortOrder).append("ascendingSort",
                    ascendingSort).append("boost", this.boost).append(
                    "fieldType", fieldType).toString();
        }

        @Override
        public int compareTo(Field rhs) {
            return name.compareTo(rhs.name);
        }

    }

    public QueryPolicy() {
    }

    public QueryPolicy(final Collection<Field> fields) {
        if (fields != null) {
            for (Field field : fields) {
                add(field);
            }
        }
    }

    public void add(final String name) {
        add(new Field(name, 0, true, 0.0F, FieldType.STRING));
    }

    public void add(final String name, final int sortOrder,
            final boolean ascendingSort, final float boost,
            final FieldType fieldType) {
        add(new Field(name, sortOrder, ascendingSort, boost, fieldType));
    }

    public void add(final Field field) {
        this.fields.put(field.name, field);
    }

    public Collection<Field> getFields() {
        return this.fields.values();
    }

    public String[] getFieldNames() {
        return this.fields.keySet().toArray(new String[this.fields.size()]);
    }

    public Field getField(final String name) {
        return this.fields.get(name);

    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof QueryPolicy)) {
            return false;
        }
        QueryPolicy rhs = (QueryPolicy) object;

        return new EqualsBuilder().append(this.fields, rhs.fields).isEquals();
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(786529047, 1924536713).append(this.fields)
                .toHashCode();
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("fields", fields).toString();
    }

}

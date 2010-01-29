package com.plexobject.docusearch.index;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.validator.GenericValidator;

import com.plexobject.docusearch.converter.Constants;

/**
 * @author Shahzad Bhatti
 * 
 */
public class IndexPolicy {
    private final Map<String, Field> fields = new TreeMap<String, Field>();
    private int score;
    private float boost;
    private String analyzer;
    private boolean addToDictionary;
    private String owner = Constants.ALL_OWNER;
    private String customSortingField;
    private String customIdField;

    public static class Field implements Comparable<Field> {
        public final String name;
        public final boolean storeInIndex;
        public final String storeAs;
        public final boolean analyze;
        public final boolean tokenize;
        public final float boost;
        public final boolean htmlToText;
        public final boolean spatialLatitude;
        public final boolean spatialLongitude;

        public Field(final String name) {
            this(name, false, null, true, false, 0.0F, false, false, false);
        }

        public Field(final String name, final boolean storeInIndex,
                final String storeAs, final boolean analyze,
                final boolean tokenize, final float boost,
                final boolean htmlToText, final boolean spatialLatitude,
                final boolean spatialLongitude) {
            this.name = name;
            this.storeInIndex = storeInIndex;
            this.storeAs = storeAs;
            this.analyze = analyze;
            this.tokenize = tokenize;
            this.boost = boost;
            this.htmlToText = htmlToText;
            this.spatialLatitude = spatialLatitude;
            this.spatialLongitude = spatialLongitude;
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
                    "storeInIndex", storeInIndex).append("storeAs", storeAs)
                    .append("analyze", analyze).append("tokenize", tokenize)
                    .append("boost", this.boost).append("spatialLatitude",
                            spatialLatitude).append("spaitalLongitude",
                            this.spatialLongitude).append("htmlToText",
                            htmlToText).toString();
        }

        @Override
        public int compareTo(Field rhs) {
            return name.compareTo(rhs.name);
        }

    }

    public IndexPolicy() {
        this(null);
    }

    public IndexPolicy(final Collection<Field> fields) {
        if (fields != null) {
            for (Field field : fields) {
                add(field);
            }
        }
    }

    public void add(final String name) {
        add(new Field(name, false, null, true, false, 0.0F, false, false, false));
    }

    public void add(final String name, final boolean storeInIndex,
            final String storeAs, final boolean analyze,
            final boolean tokenize, final float boost,
            final boolean htmlToText, final boolean spatialLatitude,
            final boolean spatialLongitude) {
        add(new Field(name, storeInIndex, storeAs, analyze, tokenize, boost,
                htmlToText, spatialLatitude, spatialLongitude));
    }

    public void add(final Field field) {
        this.fields.put(field.name, field);
    }

    public Collection<Field> getFields() {
        return this.fields.values();
    }

    public Field getField(final String name) {
        return this.fields.get(name);

    }

    public void setScore(int score) {
        this.score = score;
    }

    public int getScore() {
        return score;
    }

    public void setBoost(float boost) {
        this.boost = boost;
    }

    public float getBoost() {
        return boost;
    }

    public String getAnalyzer() {
        return analyzer;
    }

    public void setAnalyzer(final String analyzer) {
        this.analyzer = analyzer;
    }

    public void setAddToDictionary(boolean addToDictionary) {
        this.addToDictionary = addToDictionary;
    }

    public boolean isAddToDictionary() {
        return addToDictionary;
    }

    public void setOwner(final String owner) {
        this.owner = owner == null ? Constants.ALL_OWNER : owner;
    }

    public String getOwner() {
        return owner;
    }

    public boolean hasOwner() {
        return !GenericValidator.isBlankOrNull(owner);
    }

    public Field getLatitudeField() {
        for (Field field : getFields()) {
            if (field.spatialLatitude) {
                return field;
            }
        }
        return null;
    }

    public Field getLongitudeField() {
        for (Field field : getFields()) {
            if (field.spatialLongitude) {
                return field;
            }
        }
        return null;
    }

    /**
     * @return the customSortingField
     */
    public String getCustomSortingField() {
        return customSortingField;
    }

    /**
     * @return whether customSortingField is defined
     */
    public boolean hasCustomSortingField() {
        return !GenericValidator.isBlankOrNull(customSortingField);
    }

    /**
     * @param customSortingField
     *            the customSortingField to set
     */
    public void setCustomSortingField(String customSortingField) {
        this.customSortingField = customSortingField;
    }


    /**
     * @return the customIdField
     */
    public String getCustomIdField() {
        return customIdField;
    }

    /**
     * @return whether customIdField is defined
     */
    public boolean hasCustomIdField() {
        return !GenericValidator.isBlankOrNull(customIdField);
    }

    /**
     * @param customIdField
     *            the customIdField to set
     */
    public void setCustomIdField(String customIdField) {
        this.customIdField = customIdField;
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof IndexPolicy)) {
            return false;
        }
        IndexPolicy rhs = (IndexPolicy) object;

        return new EqualsBuilder().append(this.score, rhs.score).append(
                this.boost, rhs.boost).append(this.fields, rhs.fields)
                .isEquals();
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(786529047, 1924536713).append(this.score)
                .append(this.boost).append(this.fields).toHashCode();
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("score", this.score).append(
                "boost", boost).append("addToDictionary", this.addToDictionary)
                .append("analyzer", this.analyzer).append("owner", owner)
                .append("fields", fields).toString();
    }

}

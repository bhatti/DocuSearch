package com.plexobject.docusearch.index;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * @author Shahzad Bhatti
 * 
 */
public class IndexPolicy {
    private final Map<String, Field> fields = new HashMap<String, Field>();
    private int score;
    private float boost;
    private String analyzer;
    private boolean addToDictionary;

    public static class Field implements Comparable<Field> {
        public final String name;
        public final boolean storeInIndex;
        public final boolean analyze;
        public final boolean tokenize;
        public final float boost;

        public Field(final String name) {
            this(name, false, true, false, 0.0F);
        }

        public Field(final String name, final boolean storeInIndex,
                final boolean analyze, final boolean tokenize, final float boost) {
            this.name = name;
            this.storeInIndex = storeInIndex;
            this.analyze = analyze;
            this.tokenize = tokenize;
            this.boost = boost;
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
                    "storeInIndex", storeInIndex).append("analyze", analyze)
                    .append("tokenize", tokenize).append("boost", this.boost)
                    .toString();
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
        add(new Field(name, false, true, false, 0.0F));
    }

    public void add(final String name, final boolean storeInIndex,
            final boolean analyze, final boolean tokenize, final float boost) {
        add(new Field(name, storeInIndex, analyze, tokenize, boost));
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
                this.boost, rhs.boost).append(
                new TreeMap<String, Field>(this.fields),
                new TreeMap<String, Field>(rhs.fields)).isEquals();
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
                .append("analyzer", this.analyzer).append("fields", fields)
                .toString();
    }

}

package com.plexobject.docusearch.query;

import java.util.Collection;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class LookupPolicy extends QueryPolicy {
    private String fieldToReturn;
    private String dictionaryIndex;
    private String dictionaryField;

    public LookupPolicy() {
    }

    public LookupPolicy(final String fieldToReturn,
            final Collection<Field> fields) {
        super(fields);
        setFieldToReturn(fieldToReturn);
    }

    public void setFieldToReturn(final String fieldToReturn) {
        this.fieldToReturn = fieldToReturn;
    }

    public String getFieldToReturn() {
        return fieldToReturn;
    }

    /**
     * @return the dictionaryIndex
     */
    public String getDictionaryIndex() {
        return dictionaryIndex;
    }

    /**
     * @param dictionaryIndex
     *            the dictionaryIndex to set
     */
    public void setDictionaryIndex(String dictionaryIndex) {
        this.dictionaryIndex = dictionaryIndex;
    }

    /**
     * @return the dictionaryField
     */
    public String getDictionaryField() {
        return dictionaryField;
    }

    /**
     * @param dictionaryField
     *            the dictionaryField to set
     */
    public void setDictionaryField(String dictionaryField) {
        this.dictionaryField = dictionaryField;
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof LookupPolicy)) {
            return false;
        }
        LookupPolicy rhs = (LookupPolicy) object;
        if (!super.equals(object)) {
            return false;
        }
        return new EqualsBuilder()
                .append(this.fieldToReturn, rhs.fieldToReturn).isEquals();
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return super.toString()
                + new ToStringBuilder(this).append("dictionaryField",
                        dictionaryField).append("dictionaryIndex",
                        dictionaryIndex).append("fieldToReturn",
                        fieldToReturn).toString();
    }
}

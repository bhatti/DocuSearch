package com.plexobject.docusearch.query;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class RankedTerm {
    private final int frequency;
    private final String name;
    private final String value;

    public RankedTerm(String name, String value, int rank) {
        this.name = name;
        this.value = value;
        this.frequency = rank;
    }

    public int getFrequency() {
        return frequency;
    }

    public String getValue() {
        return value;
    }

    public String getName() {
        return name;
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof RankedTerm)) {
            return false;
        }
        RankedTerm rhs = (RankedTerm) object;
        return new EqualsBuilder().append(this.frequency, rhs.frequency)
                .append(this.name, rhs.name).isEquals();
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
        return new ToStringBuilder(this).append("rank", this.frequency).append(
                "name", this.name).append("value", this.value).toString();
    }
}

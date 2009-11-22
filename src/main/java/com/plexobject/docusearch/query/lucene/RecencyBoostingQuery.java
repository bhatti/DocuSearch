package com.plexobject.docusearch.query.lucene;

import java.util.Arrays;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.search.function.CustomScoreQuery;

public class RecencyBoostingQuery extends CustomScoreQuery {
    private static final long serialVersionUID = 1L;
    int[] daysAgo;
    double multiplier;
    int maxDaysAgo;

    public RecencyBoostingQuery(org.apache.lucene.search.Query q,
            int[] daysAgo, double multiplier, int maxDaysAgo) {
        super(q);
        this.daysAgo = Arrays.copyOf(daysAgo, daysAgo.length);
        this.multiplier = multiplier;
        this.maxDaysAgo = maxDaysAgo;
    }

    public float customScore(int doc, float subQueryScore, float valSrcScore) {
        if (daysAgo[doc] < maxDaysAgo) {
            float boost = (float) (multiplier * (maxDaysAgo - daysAgo[doc]) / maxDaysAgo);
            return (float) (subQueryScore * (1.0 + boost));
        } else {
            return subQueryScore;
        }
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof RecencyBoostingQuery)) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        RecencyBoostingQuery rhs = (RecencyBoostingQuery) object;
        return new EqualsBuilder().append(this.daysAgo, rhs.daysAgo).append(
                this.multiplier, rhs.multiplier).append(maxDaysAgo,
                rhs.maxDaysAgo).isEquals();
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(786529047, 1924536713).append(this)
                .toHashCode();
    }

};
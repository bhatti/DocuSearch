package com.plexobject.docusearch.query;


import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.lucene.document.DateTools;

public class QueryCriteria {
    public static final String KEYWORDS = "keywords";
    public static final String RECENCY_MAX_DAYS = "recencyMaxDays";
    public static final String RECENCY_MULTIPLIER = "recencyMultiplier";
    public static final String INDEX_DATE_BEGIN = "indexDateBegin";
    public static final String INDEX_DATE_END = "indexDateEnd";
    public static final String SCORE_QUERY = "scoreQuery";

    private final Map<String, String> options = new TreeMap<String, String>();

    public QueryCriteria() {

    }

    public void setScoreQuery() {
        options.put(SCORE_QUERY, String.valueOf(Boolean.TRUE));
    }

    public QueryCriteria setKeywords(final String keywords) {
        options.put(KEYWORDS, keywords);
        return this;
    }

    public QueryCriteria setRecencyFactor(final int maxDays,
            final double multiplier) {
        options.put(RECENCY_MAX_DAYS, String.valueOf(maxDays));
        options.put(RECENCY_MULTIPLIER, String.valueOf(multiplier));
        return this;
    }

    public QueryCriteria setIndexDateRange(final Date begin, final Date end) {
        options.put(INDEX_DATE_BEGIN, DateTools.dateToString(begin,
                DateTools.Resolution.MILLISECOND));
        options.put(INDEX_DATE_END, DateTools.dateToString(end,
                DateTools.Resolution.MILLISECOND));
        return this;
    }

    public boolean isScoreQuery() {
        return options.get(SCORE_QUERY) != null;
    }

    public String getKeywords() {
        return options.get(KEYWORDS);
    }

    public boolean hasKeywords() {
        return options.get(KEYWORDS) != null;
    }

    public int getRecencyMaxDays() {
        return getInteger(RECENCY_MAX_DAYS);
    }

    public double getRecencyFactor() {
        return getDouble(RECENCY_MULTIPLIER);
    }

    public boolean hasRecency() {
        return options.get(RECENCY_MAX_DAYS) != null
                && options.get(RECENCY_MULTIPLIER) != null;
    }

    public boolean hasIndexDateRange() {
        return options.get(INDEX_DATE_BEGIN) != null
                && options.get(INDEX_DATE_END) != null;

    }

    public String getIndexStartDateRange() {
        return options.get(INDEX_DATE_BEGIN);
    }

    public String getIndexEndDateRange() {
        return options.get(INDEX_DATE_END);
    }

    private int getInteger(final String key) {
        final String value = options.get(key);
        return value == null ? 0 : Integer.parseInt(value);
    }

    private double getDouble(final String key) {
        final String value = options.get(key);
        return value == null ? 0 : Double.valueOf(value).doubleValue();
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof QueryCriteria)) {
            return false;
        }
        QueryCriteria rhs = (QueryCriteria) object;
        return new EqualsBuilder().append(this.options, rhs.options).isEquals();
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(786529047, 1924536713).append(this.options)
                .toHashCode();
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("options", this.options)
                .toString();
    }
}

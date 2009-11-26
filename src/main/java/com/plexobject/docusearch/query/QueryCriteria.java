package com.plexobject.docusearch.query;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

public class QueryCriteria {
    public static final String KEYWORDS = "keywords";
    public static final String RECENCY_MAX_DAYS = "recencyMaxDays";
    public static final String RECENCY_MULTIPLIER = "recencyMultiplier";
    public static final String INDEX_DATE_BEGIN = "indexDateBegin";
    public static final String INDEX_DATE_END = "indexDateEnd";
    public static final String SCORE_QUERY = "scoreQuery";
    public static final String LATITUDE = "latitude";
    public static final String LONGITUDE = "longitude";
    public static final String ZIPCODE = "zipCode";
    public static final String RADIUS = "radius";
    public static final String OWNER = "owner";
    public static final String FUZZY_SEARCH_FOR_NO_RESULTS = "fuzzySearchForNoResults";

    final Map<String, String> options = new TreeMap<String, String>();

    QueryCriteria(final Map<String, String> options) {
        this.options.putAll(options);
    }

    public boolean isScoreQuery() {
        return options.get(SCORE_QUERY) != null;
    }

    public String getOwner() {
        return options.get(OWNER);
    }

    public boolean hasOwner() {
        return options.get(OWNER) != null;
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

    public double getLatitude() {
        return getDouble(LATITUDE);
    }

    public boolean hasLatitude() {
        return options.get(LATITUDE) != null;
    }

    public double getLongitude() {
        return getDouble(LONGITUDE);
    }

    public boolean hasLongitude() {
        return options.get(LONGITUDE) != null;
    }

    public String getZipcode() {
        return options.get(ZIPCODE);
    }

    public boolean hasZipcode() {
        return options.get(ZIPCODE) != null;
    }

    private int getInteger(final String key) {
        final String value = options.get(key);
        return value == null ? 0 : Integer.parseInt(value);
    }

    public double getRadius() {
        return getDouble(RADIUS);
    }

    public boolean hasRadius() {
        return options.get(RADIUS) != null;
    }

    public boolean isSpatialQuery() {
        return ((hasLongitude() && hasLatitude()) || hasZipcode())
                && hasRadius();
    }

    public boolean isFuzzySearchForNoResults() {
        return options.get(FUZZY_SEARCH_FOR_NO_RESULTS) == String
                .valueOf(Boolean.TRUE);
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

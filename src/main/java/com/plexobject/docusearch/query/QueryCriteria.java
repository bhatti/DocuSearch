package com.plexobject.docusearch.query;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.validator.GenericValidator;

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
    public static final String CITY = "city";
    public static final String STATE = "state";
    public static final String COUNTRY = "country";
    public static final String REGION = "region";
    public static final String RADIUS = "radius";
    public static final String OWNER = "owner";
    public static final String SORT_BY = "sortBy";
    public static final String ALWAYS = "always";

    public static final String ASCENDING_SORT = "ascendingSort";

    public static final String FUZZY_SEARCH_FOR_NO_RESULTS = "fuzzySearchForNoResults";

    final Map<String, String> options = new TreeMap<String, String>();

    QueryCriteria(final Map<String, String> options) {
        this.options.putAll(options);
    }

    public boolean isScoreQuery() {
        return has(SCORE_QUERY);
    }

    public String getOwner() {
        return options.get(OWNER);
    }

    public boolean hasOwner() {
        return has(OWNER);
    }

    public String getKeywords() {
        return options.get(KEYWORDS);
    }

    public boolean hasKeywords() {
        return has(KEYWORDS);
    }

    public int getRecencyMaxDays() {
        return getInteger(RECENCY_MAX_DAYS);
    }

    public double getRecencyFactor() {
        return getDouble(RECENCY_MULTIPLIER);
    }

    public boolean hasRecency() {
        return has(RECENCY_MAX_DAYS) && has(RECENCY_MULTIPLIER);
    }

    public boolean hasIndexDateRange() {
        return has(INDEX_DATE_BEGIN) && has(INDEX_DATE_END);

    }

    public String getIndexStartDateRange() {
        return options.get(INDEX_DATE_BEGIN);
    }

    public boolean isAlways() {
        return getBoolean(ALWAYS);
    }

    public String getIndexEndDateRange() {
        return options.get(INDEX_DATE_END);
    }

    public boolean hasSortBy() {
        return has(SORT_BY);
    }

    public boolean isAscendingSort() {
        return String.valueOf(Boolean.TRUE).equals(options.get(ASCENDING_SORT));
    }

    public String getSortBy() {
        return options.get(SORT_BY);
    }

    public double getLatitude() {
        return getDouble(LATITUDE);
    }

    public boolean hasLatitude() {
        return has(LATITUDE);
    }

    public double getLongitude() {
        return getDouble(LONGITUDE);
    }

    public boolean hasLongitude() {
        return has(LONGITUDE);
    }

    public String getZipcode() {
        return options.get(ZIPCODE);
    }

    public boolean hasZipcode() {
        return has(ZIPCODE);
    }

    public String getCity() {
        return options.get(CITY);
    }

    public boolean hasCity() {
        return has(CITY);
    }

    public String getState() {
        return options.get(STATE);
    }

    public boolean hasState() {
        return has(STATE);
    }

    public String getCountry() {
        return options.get(COUNTRY);
    }

    public boolean hasCountry() {
        return has(COUNTRY);
    }

    public String getRegion() {
        return options.get(REGION);
    }

    public boolean hasRegion() {
        return has(REGION);
    }

    private int getInteger(final String key) {
        final String value = options.get(key);
        return value == null ? 0 : Integer.parseInt(value);
    }

    public double getRadius() {
        return getDouble(RADIUS);
    }

    public boolean hasRadius() {
        return has(RADIUS);
    }

    public boolean isSpatialQuery() {
        return hasLongitude() && hasLatitude() && hasRadius();
    }

    public boolean isGeoQuery() {
        return hasZipcode() || hasCity() || hasState() || hasCountry()
                || hasRegion();
    }

    public boolean isFuzzySearchForNoResults() {
        return String.valueOf(Boolean.TRUE).equals(
                options.get(FUZZY_SEARCH_FOR_NO_RESULTS));
    }

    private double getDouble(final String key) {
        final String value = options.get(key);
        return value == null ? 0 : Double.valueOf(value).doubleValue();
    }

    private boolean getBoolean(final String key) {
        final String value = options.get(key);
        return value == null ? false : Boolean.valueOf(value).booleanValue();
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
        return new ToStringBuilder(this).append("geo", isGeoQuery()).append(
                "spatial", isSpatialQuery()).append("options", this.options)
                .toString();
    }

    private boolean has(String property) {
        return !GenericValidator.isBlankOrNull(options.get(property));
    }

}

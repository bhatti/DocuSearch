package com.plexobject.docusearch.query;

import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.lucene.document.DateTools;

import com.plexobject.docusearch.converter.Constants;
import com.plexobject.docusearch.domain.Builder;

public class CriteriaBuilder implements Builder<QueryCriteria> {
    private final Map<String, String> options = new TreeMap<String, String>();

    public CriteriaBuilder() {
        this((Map<String, String>) null);
    }

    public CriteriaBuilder(QueryCriteria criteria) {
        this(criteria.options);
    }

    public CriteriaBuilder(Map<String, String> options) {
        setOwner(Constants.ALL_OWNER);
        setFuzzySearchForNoResults(true);
        if (options != null) {
            this.options.putAll(options);
        }
    }

    public CriteriaBuilder setScoreQuery() {
        options.put(QueryCriteria.SCORE_QUERY, String.valueOf(Boolean.TRUE));
        return this;
    }

    public CriteriaBuilder setKeywords(final String keywords) {
        options.put(QueryCriteria.KEYWORDS, keywords.toLowerCase());
        return this;
    }

    public CriteriaBuilder setRecencyFactor(final int maxDays,
            final double multiplier) {
        options.put(QueryCriteria.RECENCY_MAX_DAYS, String.valueOf(maxDays));
        options.put(QueryCriteria.RECENCY_MULTIPLIER, String
                .valueOf(multiplier));
        return this;
    }

    public CriteriaBuilder setIndexDateRange(final Date begin, final Date end) {
        options.put(QueryCriteria.INDEX_DATE_BEGIN, DateTools.dateToString(
                begin, DateTools.Resolution.MILLISECOND));
        options.put(QueryCriteria.INDEX_DATE_END, DateTools.dateToString(end,
                DateTools.Resolution.MILLISECOND));
        return this;
    }

    public CriteriaBuilder setOwner(final String owner) {
        this.options.put(QueryCriteria.OWNER,
                owner == null ? Constants.ALL_OWNER : owner);
        return this;
    }

    public CriteriaBuilder setLatitude(double latitude) {
        options.put(QueryCriteria.LATITUDE, String.valueOf(latitude));
        return this;
    }

    public CriteriaBuilder setLongitude(double longitude) {
        options.put(QueryCriteria.LONGITUDE, String.valueOf(longitude));
        return this;
    }

    public CriteriaBuilder setZipcode(String zipcode) {
        options.put(QueryCriteria.ZIPCODE, zipcode);
        return this;
    }

    public CriteriaBuilder setRadius(double radius) {
        options.put(QueryCriteria.RADIUS, String.valueOf(radius));
        return this;
    }

    public CriteriaBuilder setFuzzySearchForNoResults(boolean fuzzy) {
        options.put(QueryCriteria.FUZZY_SEARCH_FOR_NO_RESULTS, String
                .valueOf(Boolean.TRUE));
        return this;
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof CriteriaBuilder)) {
            return false;
        }
        CriteriaBuilder rhs = (CriteriaBuilder) object;
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

    @Override
    public QueryCriteria build() {
        return new QueryCriteria(options);
    }

}

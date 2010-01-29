package com.plexobject.docusearch.query;

import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.lucene.document.DateTools;

import com.plexobject.docusearch.converter.Constants;
import com.plexobject.docusearch.domain.Builder;

public class CriteriaBuilder implements Builder<QueryCriteria> {
    private static final Pattern BAD_CHARACTERS = Pattern
            .compile("[^\\w\\s\\+\\-\\(\\)\\?\\*\"\\.'~\\{\\}:\\[\\]]");
    private static final String[] RENAME_WORDS = new String[] { "shoes",
            "shoe", "books", "book" };

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
        if (keywords != null) {
            String cleanKeywords = BAD_CHARACTERS.matcher(keywords).replaceAll(
                    "").toLowerCase().trim();

            for (int i = 0; i < RENAME_WORDS.length; i = i + 2) {
                cleanKeywords = cleanKeywords.replaceAll(RENAME_WORDS[i],
                        RENAME_WORDS[i + 1]);
            }
            options.put(QueryCriteria.KEYWORDS, cleanKeywords);
        }
        return this;
    }

    public CriteriaBuilder setRecencyFactor(final int maxDays,
            final double multiplier) {
        if (maxDays > 0 && multiplier != 0) {
            options
                    .put(QueryCriteria.RECENCY_MAX_DAYS, String
                            .valueOf(maxDays));
            options.put(QueryCriteria.RECENCY_MULTIPLIER, String
                    .valueOf(multiplier));
        }
        return this;
    }

    public CriteriaBuilder setIndexDateRange(final Date begin, final Date end) {
        if (begin != null && end != null) {
            options.put(QueryCriteria.INDEX_DATE_BEGIN, DateTools.dateToString(
                    begin, DateTools.Resolution.DAY));
            options.put(QueryCriteria.INDEX_DATE_END, DateTools.dateToString(
                    end, DateTools.Resolution.DAY));
        }
        return this;
    }

    public CriteriaBuilder setOwner(final String owner) {
        this.options.put(QueryCriteria.OWNER,
                owner == null ? Constants.ALL_OWNER : owner);
        return this;
    }

    public CriteriaBuilder setSortBy(final String field, final boolean ascending) {
        if (field != null) {
            this.options.put(QueryCriteria.SORT_BY, field);
            this.options.put(QueryCriteria.ASCENDING_SORT, String
                    .valueOf(Boolean.TRUE));
        }
        return this;
    }

    public CriteriaBuilder setLatitude(double latitude) {
        if (latitude != 0) {
            options.put(QueryCriteria.LATITUDE, String.valueOf(latitude));
        }
        return this;
    }

    public CriteriaBuilder setLongitude(double longitude) {
        if (longitude != 0) {
            options.put(QueryCriteria.LONGITUDE, String.valueOf(longitude));
        }
        return this;
    }

    public CriteriaBuilder setZipcode(String zipcode) {
        if (zipcode != null) {
            options.put(QueryCriteria.ZIPCODE, zipcode);
        }
        return this;
    }

    public CriteriaBuilder setCity(String city) {
        if (city != null) {
            options.put(QueryCriteria.CITY, city);
        }
        return this;
    }

    public CriteriaBuilder setState(String state) {
        if (state != null) {
            options.put(QueryCriteria.STATE, state);
        }
        return this;
    }

    public CriteriaBuilder setCountry(String country) {
        if (country != null) {
            options.put(QueryCriteria.COUNTRY, country);
        }
        return this;
    }

    public CriteriaBuilder setRegion(String region) {
        if (region != null) {
            options.put(QueryCriteria.REGION, region);
        }
        return this;
    }

    public CriteriaBuilder setRadius(double radius) {
        if (radius > 0) {
            options.put(QueryCriteria.RADIUS, String.valueOf(radius));
        }
        return this;
    }

    public CriteriaBuilder setFuzzySearchForNoResults(boolean fuzzy) {
        options.put(QueryCriteria.FUZZY_SEARCH_FOR_NO_RESULTS, String
                .valueOf(Boolean.TRUE));
        return this;
    }

    public CriteriaBuilder setAlways() {
        options.put(QueryCriteria.ALWAYS, String.valueOf(Boolean.TRUE));
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

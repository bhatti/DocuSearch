/**
 * 
 */
package com.plexobject.docusearch.query.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.misc.ChainedFilter;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.similar.MoreLikeThis;
import org.apache.lucene.spatial.tier.DistanceFieldComparatorSource;
import org.apache.lucene.spatial.tier.DistanceQueryBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.SearchException;
import com.plexobject.docusearch.converter.Constants;
import com.plexobject.docusearch.domain.Tuple;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.lucene.analyzer.BoostingSimilarity;
import com.plexobject.docusearch.lucene.analyzer.SimilarityHelper;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.query.CriteriaBuilder;
import com.plexobject.docusearch.query.LookupPolicy;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.RankedTerm;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.SearchDocBuilder;
import com.plexobject.docusearch.query.SearchDocList;

/**
 * @author Shahzad Bhatti
 * 
 */
public class QueryImpl implements Query {
    private static final Logger LOGGER = Logger.getLogger(QueryImpl.class);
    private static final int MAX_LIMIT = Configuration.getInstance()
            .getPageSize();
    private static final int DEFAULT_LIMIT = Configuration.getInstance()
            .getInteger("lucene.default.paging.size", 20);
    private static final Pattern FUZZY_PATTERN = Pattern.compile("(\\s+|\\z)");

    enum QueryType {
        DEFAULT, FUZZY, PREFIX, REGEX, WILDCARD, NUMBER_RANGE, TERM_RANGE, HIT
    }

    private final IndexReader reader;
    private final IndexSearcher searcher;
    private final String index;

    public QueryImpl(final File dir) {
        this(LuceneUtils.toFSDirectory(dir), dir.getName());
    }

    public QueryImpl(final Directory dir, final String index) {
        try {
            this.index = index;
            reader = IndexReader.open(dir, true); // reopen()
            searcher = new IndexSearcher(reader);
            searcher.setSimilarity(new BoostingSimilarity());
        } catch (CorruptIndexException e) {
            throw new SearchException(e);
        } catch (IOException e) {
            throw new SearchException(e);
        }
    }

    @Override
    public SearchDocList search(final QueryCriteria criteria,
            final IndexPolicy indexPolicy, final QueryPolicy queryPolicy,
            final boolean includeSuggestions, int start, int limit) {
        final boolean includeExplanation = false;
        Tuple tuple = doSearch(criteria, indexPolicy, queryPolicy,
                includeSuggestions, includeExplanation, QueryType.DEFAULT,
                start, limit);
        final Integer total = tuple.first();
        final double[][] docsAndScores = tuple.second();
        final Collection<String> similarWords = tuple.third();
        final DistanceQueryBuilder distanceQueryBuilder = tuple.get(4);
        try {
            final SearchDocList results = convert(start, limit, total,
                    docsAndScores, similarWords, indexPolicy,
                    distanceQueryBuilder);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("query " + criteria + " with policy "
                        + queryPolicy + ", returned " + results);
            }
            return results;
        } catch (CorruptIndexException e) {
            throw new SearchException("failed to search " + criteria, e);
        } catch (IOException e) {
            throw new SearchException("failed to search " + criteria, e);
        }
    }

    @Override
    public Collection<String> explain(final QueryCriteria criteria,
            final IndexPolicy indexPolicy, final QueryPolicy queryPolicy,
            final int start, final int limit) {
        final boolean includeSuggestions = false;
        final boolean includeExplanation = true;
        Tuple tuple = doSearch(criteria, indexPolicy, queryPolicy,
                includeSuggestions, includeExplanation, QueryType.DEFAULT,
                start, limit);
        final Collection<String> explanations = tuple.get(3);
        return explanations;
    }

    @Override
    public List<String> partialLookup(QueryCriteria criteria,
            final IndexPolicy indexPolicy, LookupPolicy lookupPolicy, int limit) {
        final boolean includeSuggestions = false;
        final boolean includeExplanation = false;
        Tuple tuple = doSearch(criteria, indexPolicy, lookupPolicy,
                includeSuggestions, includeExplanation, QueryType.PREFIX, 0,
                limit);
        final Integer total = tuple.first();
        final double[][] docsAndScores = tuple.second();

        // fuzzy matching if non-matched
        if (docsAndScores.length == 0 && criteria.isFuzzySearchForNoResults()
                && criteria.getKeywords().indexOf("~") == -1) {
            final QueryCriteria newCriteria = toFuzzyCriteria(criteria);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("partialLookup fuzzy searching " + newCriteria);
            }
            return partialLookup(newCriteria, indexPolicy, lookupPolicy, limit);
        }

        final Collection<String> similarWords = tuple.third();
        try {
            SearchDocList matches = convert(0, limit, total, docsAndScores,
                    similarWords, indexPolicy, null);
            List<String> results = new ArrayList<String>();
            for (SearchDoc doc : matches) {
                final String keyword = (String) doc.get(lookupPolicy
                        .getFieldToReturn());
                if (keyword != null) {
                    results.add(keyword);
                } else {
                    LOGGER.warn("Could not find field "
                            + lookupPolicy.getFieldToReturn() + " in " + doc);
                }
            }
            if (results.size() < limit) {
                results.addAll(SimilarityHelper.getInstance().prefixMatch(
                        index, criteria.getKeywords(), limit - results.size()));
            }
            return results;
        } catch (CorruptIndexException e) {
            throw new SearchException("failed to search " + criteria, e);
        } catch (IOException e) {
            throw new SearchException("failed to search " + criteria, e);
        }
    }

    @Override
    public SearchDocList moreLikeThis(final String externalId,
            final int luceneId, final IndexPolicy indexPolicy,
            final QueryPolicy queryPolicy, int start, int limit) {
        final int maxDoc = reader.maxDoc();
        if (luceneId >= maxDoc) {
            throw new IllegalArgumentException("docId exceeds # of documents "
                    + maxDoc);
        }
        if (start <= 0) {
            start = 0;
        }
        if (limit <= 0) {
            limit = DEFAULT_LIMIT;
        }

        final int max = Math.min(maxDoc, MAX_LIMIT);

        if (limit > max) {
            limit = max;
        }
        final Timer timer = Metric.newTimer("QueryImpl.moreLikeThis");
        try {
            MoreLikeThis mlt = new MoreLikeThis(reader);
            mlt.setFieldNames(queryPolicy.getFieldNames());
            mlt.setMinTermFreq(1);
            mlt.setMinDocFreq(1);

            if (LOGGER.isDebugEnabled()) {
                final org.apache.lucene.document.Document doc = reader
                        .document(luceneId);
                LOGGER.debug("Finding similar documents for " + doc);
            }

            final org.apache.lucene.search.Query query = mlt.like(luceneId);

            final Tuple queryResults = new QueryUtils(reader, searcher)
                    .doQuery(query, null, QueryUtils.getScore(queryPolicy),
                            start, limit);
            final double[][] docsAndScores = queryResults.second();
            final Integer available = queryResults.first();

            SearchDocList results = convert(start, limit, available,
                    docsAndScores, null, null, null);

            // remove all matches that include original search criteria
            // including
            // document id and lucene id
            for (Iterator<SearchDoc> it = results.iterator(); it.hasNext();) {
                final SearchDoc doc = it.next();
                if (doc.getId().equals(externalId)
                        || doc.getHitDocumentNumber() == luceneId) {
                    it.remove();
                }
            }
            return results;
        } catch (CorruptIndexException e) {
            throw new SearchException("failed to moreLikeThis " + luceneId, e);
        } catch (IOException e) {
            throw new SearchException("failed to moreLikeThis " + luceneId, e);
        } finally {
            if (searcher != null) {
                try {
                    searcher.close();
                } catch (CorruptIndexException e) {
                    LOGGER.error(e);
                } catch (IOException e) {
                    LOGGER.error(e);
                }
            }
            timer.stop();
        }
    }

    @Override
    public Collection<RankedTerm> getTopRankingTerms(final QueryPolicy policy,
            final int maxTerms) {
        final int numTerms = Math.max(maxTerms, 64);
        final Map<String, Boolean> junkWords = new HashMap<String, Boolean>();
        final PriorityQueue<RankedTerm> rtq = new PriorityQueue<RankedTerm>(
                Math.max(numTerms, 64), new Comparator<RankedTerm>() {

                    @Override
                    public int compare(final RankedTerm first,
                            final RankedTerm second) {
                        return first.getFrequency() - second.getFrequency();

                    }
                });

        try {
            final TermEnum terms = reader.terms();
            int minFreq = 0;
            while (terms.next()) {
                String field = terms.term().field();
                if (junkWords != null
                        && junkWords.get(terms.term().text()) != null) {
                    continue;
                }

                if (policy.getField(field) != null) {
                    if (terms.docFreq() > minFreq) {
                        rtq.add(new RankedTerm(terms.term().field(), terms
                                .term().text(), terms.docFreq()));
                        if (rtq.size() >= numTerms) {
                            rtq.poll(); // remove lowest
                            minFreq = rtq.peek().getFrequency();
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new SearchException("failed to get top terms for  " + policy,
                    e);
        }

        return rtq;

    }

    // ///////////////////////////////////////////////////////////////
    private Tuple doSearch(final QueryCriteria criteria,
            final IndexPolicy indexPolicy, final QueryPolicy queryPolicy,
            final boolean includeSuggestions, final boolean includeExplanation,
            final QueryType queryType, int start, int limit) {
        if (start <= 0) {
            start = 0;
        }
        if (limit <= 0) {
            limit = DEFAULT_LIMIT;
        }
        final int max = Math.min(reader.maxDoc(), MAX_LIMIT);

        if (limit > max) {
            limit = max;
        }
        final Timer timer = Metric.newTimer("QueryImpl.search");
        final Collection<String> similarWords = includeSuggestions ? new HashSet<String>()
                : null;
        final Collection<String> explanations = includeExplanation ? new ArrayList<String>()
                : null;
        final DistanceQueryBuilder distanceQueryBuilder = criteria
                .isSpatialQuery() ? QueryUtils.getDistanceQueryBuilder(
                indexPolicy.getLatitudeField().name, indexPolicy
                        .getLongitudeField().name, criteria) : null;
        // Get the id of the box that contains latitude, longitude at this zoom
        // level. It will be 2 integers separated by a dot, and representable as
        // a sortable double. e.g. -3.004 is x=-3 , y=4 (y coordinate is zero
        // padded)
        final DistanceFieldComparatorSource dsort = distanceQueryBuilder != null ? new DistanceFieldComparatorSource(
                distanceQueryBuilder.getDistanceFilter())
                : null;
        Filter filter = distanceQueryBuilder != null ? distanceQueryBuilder
                .getFilter() : null;

        try {
            org.apache.lucene.search.Query q = criteria.isScoreQuery() ? QueryUtils
                    .createQuery(QueryType.HIT, null)
                    : QueryUtils.keywordsQuery(
                            LuceneUtils.getDefaultAnalyzer(), index, criteria
                                    .getKeywords(), queryPolicy, similarWords,
                            queryType);

            if (criteria.hasOwner()) {
                Filter securityFilter = QueryUtils.securityFilter(criteria
                        .getOwner());
                if (securityFilter != null) {
                    filter = filter == null ? securityFilter
                            : new ChainedFilter(new Filter[] { filter,
                                    securityFilter });
                }
            }
            if (criteria.hasRecency()) {
                q = new QueryUtils(reader, searcher).boostQuery(q, criteria
                        .getRecencyMaxDays(), criteria.getRecencyFactor());

            }

            if (criteria.hasIndexDateRange()) {
                q = QueryUtils.indexDateRange(q, criteria
                        .getIndexStartDateRange(), criteria
                        .getIndexEndDateRange());

            }
            final Sort sort = criteria.isSpatialQuery() ? new Sort(
                    new SortField("distance", dsort)) : QueryUtils
                    .getScore(queryPolicy);
            final Tuple queryResults = new QueryUtils(reader, searcher)
                    .doQuery(q, filter, sort, start, limit);

            final double[][] docsAndScores = queryResults.second();

            // do fuzzy search if non-matched
            if (docsAndScores.length == 0 && start == 0
                    && criteria.isFuzzySearchForNoResults()
                    && criteria.getKeywords().indexOf("~") == -1) {
                final QueryCriteria newCriteria = toFuzzyCriteria(criteria);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("doSearch fuzzy searching " + newCriteria);
                }
                return doSearch(newCriteria, indexPolicy, queryPolicy,
                        includeSuggestions, includeExplanation, queryType,
                        start, limit);
            }

            final Integer available = queryResults.first();
            if (includeExplanation) {
                for (double[] docAndScore : docsAndScores) {
                    explanations.add(searcher.explain(q, (int) docAndScore[0])
                            .toString());
                }
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Searched using geo " + criteria.isSpatialQuery()
                        + ", filter " + filter + ", sort " + sort
                        + ", criteria " + criteria + " found "
                        + docsAndScores.length);
            }
            return new Tuple(available, docsAndScores, similarWords,
                    explanations, distanceQueryBuilder);
        } catch (CorruptIndexException e) {
            throw new SearchException("failed to search " + criteria, e);
        } catch (IOException e) {
            throw new SearchException("failed to search " + criteria, e);
        } catch (ParseException e) {
            throw new SearchException("failed to search " + criteria, e);
        } catch (java.text.ParseException e) {
            throw new SearchException("failed to search " + criteria, e);
        } finally {
            if (searcher != null) {
                try {
                    searcher.close();
                } catch (CorruptIndexException e) {
                    LOGGER.error(e);
                } catch (IOException e) {
                    LOGGER.error(e);
                }
            }
            timer.stop();
        }
    }

    private QueryCriteria toFuzzyCriteria(final QueryCriteria criteria) {
        Matcher matcher = FUZZY_PATTERN.matcher(criteria.getKeywords());

        final String newKeywords = matcher.replaceAll("~ ").replace("*", "")
                .trim();
        final QueryCriteria newCriteria = new CriteriaBuilder(criteria)
                .setFuzzySearchForNoResults(false).setKeywords(newKeywords)
                .build();
        return newCriteria;
    }

    @SuppressWarnings("unchecked")
    private SearchDocList convert(final int start, final int pageSize,
            final int totalHits, final double[][] docsAndScores,
            final Collection<String> similarWords,
            final IndexPolicy indexPolicy,
            final DistanceQueryBuilder distanceQueryBuilder)
            throws CorruptIndexException, IOException {
        final Collection<SearchDoc> results = new ArrayList<SearchDoc>();
        // this will add a field called _localTierXX where XX isthe value of
        // startTier in the loop
        Map<Integer, Double> distances = distanceQueryBuilder != null ? distanceQueryBuilder
                .getDistanceFilter().getDistances()
                : null;
        for (double[] docAndScore : docsAndScores) {
            if (docAndScore == null) {
                throw new NullPointerException("null docs for "
                        + docsAndScores.length + ": "
                        + Arrays.asList(docsAndScores));
            }
            final int doc = (int) docAndScore[0];
            final float score = (float) docAndScore[1];
            final org.apache.lucene.document.Document searchDoc = reader
                    .document(doc);
            final List<Field> fields = searchDoc.getFields();

            final Map<String, Object> map = new HashMap<String, Object>();
            for (Field field : fields) {
                map.put(field.name(), field.stringValue());
            }
            if (distances != null && indexPolicy != null) {
                Double distance = distances.get(doc);
                map.put(indexPolicy.getLatitudeField().name, NumericUtils
                        .prefixCodedToDouble(searchDoc.get(indexPolicy
                                .getLatitudeField().name)));
                map.put(indexPolicy.getLongitudeField().name, NumericUtils
                        .prefixCodedToDouble(searchDoc.get(indexPolicy
                                .getLongitudeField().name)));
                map.put(Constants.DISTANCE, distance);
            }

            final SearchDoc result = new SearchDocBuilder().putAll(map)
                    .setScore(score).seHitDocumentNumber(doc).build();
            if (!results.contains(result)) {
                results.add(result);
            }
        }
        return new SearchDocList(start, pageSize, totalHits, results,
                similarWords);
    }
    // TODO Do we need SpanQuery, PerFieldAnalyzerWrapper?
}

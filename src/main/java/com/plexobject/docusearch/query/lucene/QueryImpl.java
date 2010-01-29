/**
 * 
 */
package com.plexobject.docusearch.query.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
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
import com.plexobject.docusearch.cache.CachedMap;
import com.plexobject.docusearch.converter.Constants;
import com.plexobject.docusearch.domain.Tuple;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.lucene.analyzer.BoostingSimilarity;
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
    private static final Pattern BAD_CHARACTERS = Pattern.compile("[^\\w\\s]");
    private static final Logger LOGGER = Logger.getLogger(QueryImpl.class);
    private static final int MAX_LIMIT = Configuration.getInstance()
            .getPageSize();
    private static final int DEFAULT_LIMIT = Configuration.getInstance()
            .getInteger("lucene.default.paging.size", 20);
    private static final Pattern FUZZY_PATTERN = Pattern.compile("\\z"); // (\\s+|\\z)

    enum QueryType {
        DEFAULT, OR, FUZZY, PREFIX, REGEX, WILDCARD, NUMBER_RANGE, TERM_RANGE, HIT
    }

    private final Directory dir;
    private IndexReader reader;
    private IndexSearcher searcher;
    private final String index;

    private Map<LookupPolicy, QueryImpl> lookupQueries = new CachedMap<LookupPolicy, QueryImpl>();

    public QueryImpl(final String indexName) {
        this(new File(LuceneUtils.INDEX_DIR, indexName));
    }

    public QueryImpl(final File dir) {
        this(LuceneUtils.toFSDirectory(dir), dir.getName());
    }

    public QueryImpl(final Directory dir, final String index) {
        if (dir == null) {
            throw new NullPointerException("dir is null");
        }
        if (index == null) {
            throw new NullPointerException("index is null");
        }
        try {
            this.dir = dir;
            this.index = index;
            reader = IndexReader.open(dir, true); // reopen()
            searcher = new IndexSearcher(reader);
            searcher.setSimilarity(new BoostingSimilarity());
        } catch (CorruptIndexException e) {
            LOGGER.fatal("Index file is corrupted " + dir, e);
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

        Filter filter = null;

        Tuple tuple = doSearch(criteria, indexPolicy, queryPolicy, filter,
                includeSuggestions, includeExplanation, QueryType.DEFAULT,
                start, limit, true);
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
    public Collection<String> explainSearch(final QueryCriteria criteria,
            final IndexPolicy indexPolicy, final QueryPolicy queryPolicy,
            final int start, final int limit) {
        final boolean includeSuggestions = false;
        final boolean includeExplanation = true;

        Filter filter = null;

        final Tuple tuple = doSearch(criteria, indexPolicy, queryPolicy,
                filter, includeSuggestions, includeExplanation,
                QueryType.DEFAULT, start, limit, true);
        final Collection<String> explanations = tuple.get(3);
        return explanations;
    }

    @Override
    public List<String> partialLookup(final QueryCriteria rawCriteria,
            final IndexPolicy indexPolicy, LookupPolicy lookupPolicy, int limit) {
        final QueryCriteria criteria = toPartialFuzzyCriteria(rawCriteria);
        try {
            Set<String> results = doPartialLookup(criteria, indexPolicy,
                    lookupPolicy, limit);
            if (results.size() < limit) {
                final LookupPolicy queryPolicy = new LookupPolicy();
                queryPolicy.add(lookupPolicy.getDictionaryField());
                queryPolicy.setFieldToReturn(lookupPolicy.getDictionaryField());
                queryPolicy.setDictionaryField(lookupPolicy
                        .getDictionaryField());
                try {
                    Set<String> newResults = getLookupQuery(lookupPolicy)
                            .doPartialLookup(criteria, indexPolicy,
                                    queryPolicy, limit * 2);
                    for (String w : newResults) {
                        if (results.size() > limit) {
                            break;
                        }
                        results.add(w);
                    }
                } catch (Exception e) {
                    LOGGER.warn("Failed to lookup in dictionary due to " + e);
                }
            }
            List<String> sortedList = new ArrayList<String>(results);
            Collections.sort(sortedList);
            return sortedList;
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
                    .doQuery(query, null, QueryUtils
                            .getScoreUsingPolicy(queryPolicy), start, limit);
            final double[][] docsAndScores = queryResults.second();
            final Integer available = queryResults.first();

            SearchDocList results = convert(start, limit, available,
                    docsAndScores, null, null, null);

            // remove all matches that include original search criteria
            // including
            // document id and lucene id
            for (Iterator<SearchDoc> it = results.iterator(); it.hasNext();) {
                final SearchDoc doc = it.next();
                // TODO fix
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
            timer.stop();
        }
    }

    @Override
    public Collection<RankedTerm> getTopRankingTerms(final QueryPolicy policy,
            final int maxTerms) {
        final int numTerms = Math.max(maxTerms, 64);
        final Map<String, Boolean> junkWords = new TreeMap<String, Boolean>();
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

    @Override
    public String toString() {
        return "QueryImpl " + dir;
    }

    // ///////////////////////////////////////////////////////////////
    private Tuple doSearch(final QueryCriteria criteria,
            final IndexPolicy indexPolicy, final QueryPolicy queryPolicy,
            final Filter queryFilter, final boolean includeSuggestions,
            final boolean includeExplanation, final QueryType queryType,
            int start, int limit, boolean retryFuzzySearchIfNonMatches) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("###doSearch searching " + criteria + " on " + dir
                    + " using " + queryPolicy);
        }

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
        final List<Filter> filters = new ArrayList<Filter>();
        if (queryFilter != null) {
            filters.add(queryFilter);
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
        if (distanceQueryBuilder != null) {
            filters.add(distanceQueryBuilder.getFilter());
        }
        //
        if (!criteria.hasKeywords() && !criteria.isScoreQuery()
                && !criteria.isAlways()) {
            return new Tuple(0, new double[0][], similarWords, explanations,
                    distanceQueryBuilder);
        }
        final Analyzer analyzer = queryPolicy.hasAnalyzer() ? LuceneUtils
                .getDefaultAnalyzer() : LuceneUtils.getAnalyzer(queryPolicy
                .getAnalyzer());

        try {
            org.apache.lucene.search.Query q = null;
            if (criteria.isScoreQuery()) {
                q = QueryUtils.createQuery(QueryType.HIT, null);
            } else if (criteria.isAlways()) {
                q = QueryUtils.alwaysQuery();
            } else {
                q = QueryUtils.keywordsQuery(analyzer, index, criteria
                        .getKeywords(), queryPolicy, similarWords, queryType);
            }
            if (criteria.hasOwner()) {
                Filter securityFilter = QueryUtils.securityFilter(criteria
                        .getOwner());
                if (securityFilter != null) {
                    filters.add(securityFilter);
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
            Sort sort = null;
            if (criteria.isSpatialQuery()) {
                sort = new Sort(new SortField("distance", dsort));
            } else if (criteria.hasSortBy()) {
                sort = QueryUtils.getScoreUsingCriteria(criteria.getSortBy(),
                        queryPolicy);
            } else {
                sort = QueryUtils.getScoreUsingPolicy(queryPolicy);
            }
            final Filter filter = filters.size() == 0 ? null
                    : filters.size() == 1 ? filters.get(0) : new ChainedFilter(
                            filters.toArray(new Filter[filters.size()]));

            final Tuple queryResults = new QueryUtils(reader, searcher)
                    .doQuery(q, filter, sort, start, limit);

            final double[][] docsAndScores = queryResults.second();

            // do fuzzy search if non-matched
            if (retryFuzzySearchIfNonMatches && docsAndScores.length == 0
                    && start == 0 && criteria.isFuzzySearchForNoResults()
                    && criteria.getKeywords().indexOf(",") == -1
                    && criteria.getKeywords().indexOf("\"") == -1
                    && criteria.getKeywords().indexOf("'") == -1
                    && criteria.getKeywords().indexOf("*") == -1
                    && criteria.getKeywords().indexOf("~") == -1) {
                final QueryCriteria newCriteria = toFuzzyCriteria(criteria);

                return doSearch(newCriteria, indexPolicy, queryPolicy,
                        queryFilter, includeSuggestions, includeExplanation,
                        queryType, start, limit, retryFuzzySearchIfNonMatches);
            } else if (retryFuzzySearchIfNonMatches
                    && docsAndScores.length == 0 && start == 0
                    && criteria.getKeywords().indexOf("*") == -1
                    && criteria.getKeywords().indexOf("\"") == -1
                    && criteria.getKeywords().indexOf("'") == -1
                    && criteria.getKeywords().indexOf("~") != -1) {
                final QueryCriteria newCriteria = toPartialFuzzyCriteria(criteria);

                return doSearch(newCriteria, indexPolicy, queryPolicy,
                        queryFilter, includeSuggestions, includeExplanation,
                        queryType, start, limit, false);
            }
            final Integer available = queryResults.first();
            if (includeExplanation) {
                for (double[] docAndScore : docsAndScores) {
                    explanations.add(searcher.explain(q, (int) docAndScore[0])
                            .toString());
                }
            }
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Searched using geo " + criteria.isSpatialQuery()
                        + ", filter " + filter + ", sort " + sort
                        + ", criteria " + criteria + " on " + dir + ", query "
                        + q.getClass().getName() + ", analyzer " + analyzer
                        + ", found " + docsAndScores.length + ", available "
                        + available);
            }
            return new Tuple(available, docsAndScores, similarWords,
                    explanations, distanceQueryBuilder);
        } catch (CorruptIndexException e) {
            throw new SearchException("failed to search " + criteria, e);
        } catch (IOException e) {
            throw new SearchException("failed to search " + criteria, e);
        } catch (ParseException e) {
            LOGGER.warn("Failed to parse " + criteria + " due to " + e);
            final String cleanKeywords = BAD_CHARACTERS.matcher(
                    criteria.getKeywords()).replaceAll("").toLowerCase().trim();
            if (cleanKeywords.length() == 0) {
                return new Tuple(0, new double[0][], similarWords,
                        explanations, distanceQueryBuilder);
            } else {
                return doSearch(new CriteriaBuilder(criteria).setKeywords(
                        cleanKeywords).build(), indexPolicy, queryPolicy,
                        queryFilter, includeSuggestions, includeExplanation,
                        queryType, start, limit, retryFuzzySearchIfNonMatches);
            }
        } catch (java.text.ParseException e) {
            throw new SearchException("failed to search " + criteria, e);
        } finally {
            timer.stop();
        }
    }

    @Override
    public void close() {
        if (searcher != null) {
            try {
                searcher.close();
            } catch (CorruptIndexException e) {
                LOGGER.error(e);
            } catch (IOException e) {
                LOGGER.error(e);
            }
        }
        if (reader != null) {
            try {
                reader.close();
            } catch (CorruptIndexException e) {
                LOGGER.error(e);
            } catch (IOException e) {
                LOGGER.error(e);
            }
        }
        searcher = null;
        reader = null;
    }

    private QueryCriteria toFuzzyCriteria(final QueryCriteria criteria) {
        Matcher matcher = FUZZY_PATTERN.matcher(criteria.getKeywords());

        final String newKeywords = matcher.replaceAll("~0.8 ").replace("*", "")
                .trim();
        final QueryCriteria newCriteria = new CriteriaBuilder(criteria)
                .setFuzzySearchForNoResults(false).setKeywords(newKeywords)
                .build();
        return newCriteria;
    }

    private QueryCriteria toPartialFuzzyCriteria(final QueryCriteria criteria) {
        Matcher matcher = FUZZY_PATTERN.matcher(criteria.getKeywords());

        final String newKeywords = matcher.replaceAll("*").replace("~0.8", "")
                .trim();
        final QueryCriteria newCriteria = new CriteriaBuilder(criteria)
                .setFuzzySearchForNoResults(false).setKeywords(newKeywords)
                .build();
        return newCriteria;
    }

    @SuppressWarnings("unchecked")
    private SearchDocList convert(final int start, final int pageSize,
            int totalHits, final double[][] docsAndScores,
            final Collection<String> similarWords,
            final IndexPolicy indexPolicy,
            final DistanceQueryBuilder distanceQueryBuilder)
            throws CorruptIndexException, IOException {
        final List<SearchDoc> results = new ArrayList<SearchDoc>();
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

            final Map<String, Object> map = new TreeMap<String, Object>();
            for (Field field : fields) {
                map.put(field.name(), field.stringValue());
            }

            //
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
            final SearchDocBuilder resultBuilder = new SearchDocBuilder()
                    .putAll(map).setScore(score).seHitDocumentNumber(doc);

            SearchDoc result = null;

            // add scores for duplicates
            int oldIndex = results.indexOf(resultBuilder.build());
            if (oldIndex == -1) {
                result = resultBuilder.build();
                results.add(result);
            } else {
                final SearchDoc oldResult = results.get(oldIndex);
                resultBuilder.setScore(score + oldResult.getScore());
                result = resultBuilder.build();
                results.set(oldIndex, result);
                totalHits--;
            }
        }

        return new SearchDocList(start, pageSize, totalHits, results,
                similarWords);
    }

    private Set<String> doPartialLookup(QueryCriteria criteria,
            final IndexPolicy indexPolicy, final LookupPolicy lookupPolicy,
            int limit) throws CorruptIndexException, IOException {
        final boolean includeSuggestions = false;
        final boolean includeExplanation = false;
        Tuple tuple = doSearch(criteria, indexPolicy, lookupPolicy, null,
                includeSuggestions, includeExplanation, QueryType.PREFIX, 0,
                limit, false);
        final Integer total = tuple.first();
        final double[][] docsAndScores = tuple.second();

        // fuzzy matching if non-matched
        if (docsAndScores.length == 0 && criteria.isFuzzySearchForNoResults()
                && criteria.getKeywords().indexOf(",") == -1
                && criteria.getKeywords().indexOf("*") == -1
                && criteria.getKeywords().indexOf("\"") == -1
                && criteria.getKeywords().indexOf("'") == -1
                && criteria.getKeywords().indexOf("~") == -1) {
            final QueryCriteria newCriteria = toFuzzyCriteria(criteria);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("doPartialLookup fuzzy searching " + newCriteria);
            }
            return doPartialLookup(newCriteria, indexPolicy, lookupPolicy,
                    limit);
        }
        final Collection<String> similarWords = tuple.third();

        SearchDocList matches = convert(0, limit, total, docsAndScores,
                similarWords, indexPolicy, null);
        Set<String> results = new HashSet<String>();
        for (SearchDoc doc : matches) {
            try {
                final String keyword = (String) doc.get(lookupPolicy
                        .getFieldToReturn());
                if (keyword != null) {
                    results.add(keyword);
                } else {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Could not find field "
                                + lookupPolicy.getFieldToReturn() + " in "
                                + doc + " in " + dir);
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to lookup due to " + e);
            }
        }
        return results;

    }

    protected QueryImpl getLookupQuery(LookupPolicy lookupPolicy) {
        synchronized (lookupQueries) {
            QueryImpl q = lookupQueries.get(lookupPolicy);
            if (q == null) {
                if (lookupPolicy.getDictionaryIndex() == null) {
                    throw new IllegalArgumentException(
                            "Missing dictionary index in " + lookupPolicy);
                }
                q = new QueryImpl(lookupPolicy.getDictionaryIndex());
                lookupQueries.put(lookupPolicy, q);
            }
            return q;
        }
    }
}

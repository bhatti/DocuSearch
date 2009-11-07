/**
 * 
 */
package com.plexobject.docusearch.query.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.FieldCacheTermsFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.RangeFilter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.function.CustomScoreQuery;
import org.apache.lucene.search.function.FieldScoreQuery;
import org.apache.lucene.search.similar.MoreLikeThis;
import org.apache.lucene.store.Directory;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.SearchException;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.Tuple;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.lucene.analyzer.BoostingSimilarity;
import com.plexobject.docusearch.lucene.analyzer.SimilarityHelper;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
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
@SuppressWarnings("deprecation")
public class QueryImpl implements Query {

    private static final Logger LOGGER = Logger.getLogger(QueryImpl.class);
    private static final int MAX_LIMIT = Configuration.getInstance()
            .getPageSize();
    private static final int MAX_MAX_DAYS = Configuration.getInstance()
            .getInteger("lucene.recency.max.days", 2 * 365);
    private static final int DEFAULT_LIMIT = Configuration.getInstance()
            .getInteger("lucene.default.paging.size", 20);
    private static final double DEFAULT_MULTIPLIER = Configuration
            .getInstance().getDouble("lucene.recency.multiplier", 2.0);
    private static final int MSEC_PER_DAY = 24 * 3600 * 1000;

    static class RecencyBoostingQuery extends CustomScoreQuery {
        private static final long serialVersionUID = 1L;
        int[] daysAgo;
        double multiplier;
        int maxDaysAgo;

        public RecencyBoostingQuery(org.apache.lucene.search.Query q,
                int[] daysAgo, double multiplier, int maxDaysAgo) {
            super(q);
            this.daysAgo = daysAgo;
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
            return new EqualsBuilder().append(this.daysAgo, rhs.daysAgo)
                    .append(this.multiplier, rhs.multiplier).append(maxDaysAgo,
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
            final QueryPolicy policy, final boolean includeSuggestions,
            int start, int limit) {
        Tuple tuple = doSearch(criteria, policy, includeSuggestions, false,
                start, limit);
        final Integer total = tuple.first();
        final ScoreDoc[] docs = tuple.second();
        final Collection<String> similarWords = tuple.third();
        try {
            return convert(searcher, start, limit, total, docs, similarWords);
        } catch (CorruptIndexException e) {
            throw new SearchException("failed to search " + criteria, e);
        } catch (IOException e) {
            throw new SearchException("failed to search " + criteria, e);
        }
    }

    @Override
    public Collection<String> explain(final QueryCriteria criteria,
            final QueryPolicy policy, final int start, final int limit) {
        Tuple tuple = doSearch(criteria, policy, false, true, start, limit);
        final Collection<String> explanations = tuple.last();
        return explanations;
    }

    @Override
    public SearchDocList moreLikeThis(int docId, QueryPolicy policy, int start,
            int limit) {
        final int maxDoc = reader.maxDoc();
        if (docId >= maxDoc) {
            throw new IllegalArgumentException("docId exceeds # of documents "
                    + maxDoc);
        }
        if (start <= 0) {
            start = 1;
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
            mlt.setFieldNames(policy.getFieldNames());
            mlt.setMinTermFreq(1);
            mlt.setMinDocFreq(1);

            if (LOGGER.isDebugEnabled()) {
                final org.apache.lucene.document.Document doc = reader
                        .document(docId);
                LOGGER.debug("Finding similar documents for " + doc);
            }

            final org.apache.lucene.search.Query query = mlt.like(docId);

            final HitCollector hc = new HitPageCollector(start, limit);
            searcher.search(query, null, hc);
            final ScoreDoc[] docs = ((HitPageCollector) hc).getScores();
            return convert(searcher, start, limit, ((HitPageCollector) hc)
                    .getTotalAvailable(), docs, null);
        } catch (CorruptIndexException e) {
            throw new SearchException("failed to moreLikeThis " + docId, e);
        } catch (IOException e) {
            throw new SearchException("failed to moreLikeThis " + docId, e);
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

    private Tuple doSearch(final QueryCriteria criteria,
            final QueryPolicy policy, final boolean includeSuggestions,
            final boolean includeExplanation, int start, int limit) {
        if (start <= 0) {
            start = 1;
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
        try {
            org.apache.lucene.search.Query q = null;

            if (criteria.isScoreQuery()) {
                q = hitQuery(searcher);
            } else if (criteria.hasKeywords()) {
                q = keywordsQuery(searcher, criteria, policy, similarWords);
            } else {
                throw new SearchException("Illegal criteria " + criteria);
            }

            if (criteria.hasRecency()) {
                q = boostQuery(q, criteria.getRecencyMaxDays(), criteria
                        .getRecencyFactor());

            }

            if (criteria.hasIndexDateRange()) {
                q = indexDateRange(q, criteria.getIndexStartDateRange(),
                        criteria.getIndexEndDateRange());

            }

            final HitCollector hc = new HitPageCollector(start, limit);
            searcher.search(q, null, hc);
            final ScoreDoc[] docs = ((HitPageCollector) hc).getScores();
            if (includeExplanation) {
                for (ScoreDoc doc : docs) {
                    explanations.add(searcher.explain(q, doc.doc).toString());
                }
            }
            return new Tuple(((HitPageCollector) hc).getTotalAvailable(), docs,
                    similarWords, explanations);
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

    @SuppressWarnings("unchecked")
    private SearchDocList convert(final IndexSearcher searcher,
            final int start, final int pageSize, final int totalHits,
            final ScoreDoc[] scoreDocs, final Collection<String> similarWords)
            throws CorruptIndexException, IOException {
        final Collection<SearchDoc> results = new ArrayList<SearchDoc>();
        for (ScoreDoc scoreDoc : scoreDocs) {
            final org.apache.lucene.document.Document searchDoc = reader
                    .document(scoreDoc.doc);
            final List<Field> fields = searchDoc.getFields();

            final Map<String, Object> map = new HashMap<String, Object>();
            for (Field field : fields) {
                map.put(field.name(), field.stringValue());
            }
            final SearchDoc result = new SearchDocBuilder().putAll(map)
                    .setScore(scoreDoc.score).seHitDocumentNumber(scoreDoc.doc)
                    .build();
            if (!results.contains(result)) {
                results.add(result);
            }
        }
        return new SearchDocList(start, pageSize, totalHits, results,
                similarWords);
    }

    private org.apache.lucene.search.Query hitQuery(final IndexSearcher searcher)
            throws IOException {
        return new FieldScoreQuery(SearchDoc.SCORE, FieldScoreQuery.Type.BYTE);

    }

    private org.apache.lucene.search.Query keywordsQuery(
            final IndexSearcher searcher, final QueryCriteria criteria,
            final QueryPolicy policy, final Collection<String> similarWords)
            throws IOException, ParseException {
        if (GenericValidator.isBlankOrNull(criteria.getKeywords())) {
            throw new IllegalArgumentException("keywords not specified");
        }
        final BooleanQuery query = new BooleanQuery();
        final boolean wildMatch = criteria.getKeywords().indexOf('*') != -1;
        for (QueryPolicy.Field field : policy.getFields()) {
            if (GenericValidator.isBlankOrNull(field.name)) {
                throw new IllegalArgumentException(
                        "field name not specified for " + field + " in policy "
                                + policy);
            }
            final Term term = new Term(field.name, criteria.getKeywords());

            final org.apache.lucene.search.Query q = wildMatch ? new WildcardQuery(
                    term)
                    : field.fuzzyMatch ? new FuzzyQuery(term) : new TermQuery(
                            term);
            if (field.boost != 0) {
                q.setBoost(field.boost);
            }
            query.add(q, BooleanClause.Occur.SHOULD);
        }
        if (similarWords != null) {
            final String similar = SimilarityHelper.getInstance().didYouMean(
                    index, criteria.getKeywords());
            if (similar != null) {
                similarWords.add(similar);
            }
        }
        // TermFreqVector vector = reader.getTermFreqVector(id, "name");
        // TODO add sorting
        return query;
    }

    private org.apache.lucene.search.Query boostQuery(
            final org.apache.lucene.search.Query q, int maxDays,
            double multiplier) throws IOException, ParseException,
            java.text.ParseException {
        if (maxDays <= 0) {
            maxDays = searcher.maxDoc();
        }
        if (maxDays > MAX_MAX_DAYS) {
            maxDays = MAX_MAX_DAYS;
        }
        if (multiplier == 0) {
            multiplier = DEFAULT_MULTIPLIER;
        }
        final int[] daysAgo = new int[Math.min(searcher.maxDoc(), maxDays * 10)];
        long now = System.currentTimeMillis();

        for (int i = 0; i < daysAgo.length; i++) {
            if (!reader.isDeleted(i)) {
                long then = DateTools.stringToTime(reader.document(i).get(
                        "indexDate"));

                daysAgo[i] = (int) ((now - then) / MSEC_PER_DAY);
            }
        }
        return new RecencyBoostingQuery(q, daysAgo, multiplier, maxDays);
    }

    private org.apache.lucene.search.Query indexDateRange(
            org.apache.lucene.search.Query q, String indexStartDateRange,
            String indexEndDateRange) {
        // final RangeQuery rangeQuery = new RangeQuery(q, "indexDate",
        // indexStartDateRange, indexEndDateRange, true, true);
        // rangeQuery.setConstantScoreRewrite(true);
        // return rangeQuery;
        return null;
    }

    @SuppressWarnings("unused")
    private Filter indexDateRangeFilter(final String indexStartDateRange,
            final String indexEndDateRange) {
        return new RangeFilter("indexDate", indexStartDateRange,
                indexEndDateRange, true, true);
    }

    @SuppressWarnings("unused")
    private Filter indexDateCachedFilter(final String indexStartDateRange,
            final String indexEndDateRange) {
        return new FieldCacheTermsFilter("indexDate", new String[] {
                indexStartDateRange, indexEndDateRange });
    }

    @SuppressWarnings("unused")
    private Filter securityFilter(final String owner) {
        TermQuery securityFilter = new TermQuery(new Term("owner", owner));
        return new QueryWrapperFilter(securityFilter);
    }

    @SuppressWarnings("unused")
    private Filter cachingFilter(final Filter filter) {
        return new CachingWrapperFilter(filter);
    }

    @SuppressWarnings("unused")
    private Sort getScore(final QueryPolicy policy) {
        List<SortField> scores = new ArrayList<SortField>();
        scores.add(SortField.FIELD_SCORE);
        for (QueryPolicy.Field field : policy.getFields()) {
            if (field.sortOrder > 0) {
                scores.add(SortField.FIELD_DOC);
            }
        }
        for (QueryPolicy.Field field : policy.getFields()) {
            if (field.sortOrder > 0) {
                scores.set(field.sortOrder, new SortField(field.name,
                        field.ascendingSort));
            }
        }
        return new Sort(scores.toArray(new SortField[scores.size()]));
    }

    public boolean existsDocument(final String id) {
        Term term = new Term(Document.ID, id);
        try {
            return searcher.docFreq(term) > 0;
        } catch (IOException ie) {
        }
        return false;
    }
    // TODO Do we need SpanQuery, PerFieldAnalyzerWrapper?
}

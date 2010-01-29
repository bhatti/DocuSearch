package com.plexobject.docusearch.query.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.FieldCacheTermsFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.function.CustomScoreQuery;
import org.apache.lucene.search.function.FieldScoreQuery;
import org.apache.lucene.spatial.tier.DistanceQueryBuilder;
import org.apache.lucene.util.Version;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.SortedIntDocSet;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.converter.Constants;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.Tuple;
import com.plexobject.docusearch.lucene.analyzer.SimilarityHelper;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.lucene.QueryImpl.QueryType;
import com.plexobject.docusearch.util.TimeUtils;

public class QueryUtils {
    private static final Logger LOGGER = Logger.getLogger(QueryUtils.class);
    private static final int MAX_MAX_DAYS = Configuration.getInstance()
            .getInteger("lucene.recency.max.days", 2 * 365);
    @SuppressWarnings("unused")
    private static final Pattern KEYWORD_SPLIT_PATTERN = Pattern
            .compile("['\"\\.,;:\\[\\]{}()\\s]");

    private static final double DEFAULT_MULTIPLIER = Configuration
            .getInstance().getDouble("lucene.recency.multiplier", 2.0);
    private static final int MSEC_PER_DAY = 24 * 3600 * 1000;
    private static SearchScheme searchScheme = SearchScheme.SORT_WITH_TOPDOCS;

    public enum SearchScheme {
        SORT_WITH_TOPDOCS, SORT_WITH_COLLECTOR
    }

    private final IndexReader reader;
    private final IndexSearcher searcher;

    QueryUtils(final IndexReader reader, final IndexSearcher searcher) {
        this.reader = reader;
        this.searcher = searcher;
    }

    public static SearchScheme getSearchScheme() {
        return searchScheme;
    }

    public static void setSearchScheme(SearchScheme searchScheme) {
        if (searchScheme != QueryUtils.SearchScheme.SORT_WITH_COLLECTOR) {
            QueryUtils.searchScheme = searchScheme;
        }
    }

    /**
     * Returns documents matching both <code>query</code> and the intersection
     * of the <code>filterList</code>, sorted by <code>sort</code>.
     * <p>
     * This method is cache aware and may retrieve <code>filter</code> from the
     * cache or make an insertion into the cache as a result of this call.
     * <p>
     * 
     * @param query
     * @param filterList
     *            may be null
     * @param lsort
     *            criteria by which to sort (if null, query relevance is used)
     * @param offset
     *            offset into the list of documents to return
     * @param len
     *            maximum number of documents to return
     * @return DocList meeting the specified criteria, should <b>not</b> be
     *         modified by the caller.
     * @throws IOException
     */
    Tuple doQuery(final org.apache.lucene.search.Query query,
            final Filter filter, final Sort sort, final int offset,
            final int len) throws IOException {
        final int maxDocRequested = reader.maxDoc();
        TopDocs docs = null;

        switch (searchScheme) {

        case SORT_WITH_COLLECTOR: // TODO fix this
            final DocSetCollector collector = new DocSetCollector(
                    maxDocRequested);
            searcher.search(query.weight(searcher), filter, collector);
            int sliceLen = collector.getNumHits(); // Math.min(maxDocRequested,
            // collector.getNumHits());
            if (sliceLen < 0) {
                sliceLen = 0;
            }
            final DocList superset = sortDocSet(query, collector, sort,
                    sliceLen);
            final DocList slice = superset.subset(offset, len);
            final double[][] docsAndScores = new double[sliceLen][];
            final DocIterator it = slice.iterator();
            for (int i = 0; it.hasNext(); i++) {
                docsAndScores[i] = new double[] { it.nextDoc(), it.score() };
            }
            return new Tuple(collector.getNumHits(), docsAndScores);
        case SORT_WITH_TOPDOCS:
            TopDocsCollector tdc = TopFieldCollector.create(sort,
                    maxDocRequested, true, true, false, true);
            //
            searcher.search(query, filter, tdc);

            docs = tdc.topDocs();
            return sliceResults(offset, len, docs);
        default:
            docs = searcher.search(query.weight(searcher), filter,
                    maxDocRequested);
            return sliceResults(offset, len, docs);
        }
    }

    private Tuple sliceResults(final int offset, final int len, TopDocs docs) {
        int maxDocsToReturn = Math.max(0, Math.min(docs.scoreDocs.length
                - offset, len));
        // LOGGER.info("max results to return " + maxDocsToReturn +
        // ", doc length " + docs.scoreDocs.length);
        final double[][] docsAndScores = new double[maxDocsToReturn][];

        for (int i = 0; i < docs.scoreDocs.length; i++) {
            final ScoreDoc field = docs.scoreDocs[i];
            if (i < offset) {
                continue;
            } else if (i - offset == maxDocsToReturn) {
                break;
            }
            // LOGGER.info("setting " + (i - offset) + " : " + field.doc);

            docsAndScores[i - offset] = new double[] { field.doc, field.score };
        }
        return new Tuple(docs.scoreDocs.length, docsAndScores); // docs.totalHits
    }

    private DocList sortDocSet(final org.apache.lucene.search.Query query,
            final DocSetCollector collector, final Sort sort, final int nDocs)
            throws IOException {
        final DocSet set = collector.getDocSet();
        // bit of a hack to tell if a set is sorted
        boolean inOrder = set instanceof BitDocSet
                || set instanceof SortedIntDocSet;
        final boolean needScores = true;
        final TopDocsCollector topCollector = TopFieldCollector.create(sort,
                nDocs, false, needScores, needScores, inOrder);
        final int base = 0;

        // topCollector.setScorer(new QueryScorer(query, reader, null));
        topCollector.setNextReader(reader, base);

        DocIterator iter = set.iterator();
        final float maxScore = collector.getTopScore();
        int totalHits = collector.getNumHits();
        //
        final int nDocsReturned = nDocs;
        final int[] ids = new int[nDocsReturned];
        final float[] scores = new float[nDocsReturned];
        int i = 0;
        while (iter.hasNext()) {
            int doc = iter.nextDoc();
            // topCollector.collect(doc - base);
            ids[i] = doc;
            scores[i] = iter.score();
            i++;
        }

        // TopDocs topDocs = topCollector.topDocs(0, nDocs);
        // int totalHits = topCollector.getTotalHits();
        //
        // final float maxScore = totalHits > 0 ? topDocs.getMaxScore() : 0.0f;
        // int nDocsReturned = topDocs.scoreDocs.length;
        //
        // final int[] ids = new int[nDocsReturned];
        // final float[] scores = new float[nDocsReturned];
        // for (int i = 0; i < nDocsReturned; i++) {
        // ScoreDoc scoreDoc = topDocs.scoreDocs[i];
        // ids[i] = scoreDoc.doc;
        // if (scores != null) {
        // scores[i] = scoreDoc.score;
        // }
        // }

        int sliceLen = Math.min(nDocs, nDocsReturned);
        if (sliceLen < 0) {
            sliceLen = 0;
        }

        return new DocSlice(0, sliceLen, ids, scores, totalHits, maxScore);
    }

    static org.apache.lucene.search.Query createQuery(
            final QueryType queryType, final Term term) {
        switch (queryType) {
        case FUZZY:
            return new FuzzyQuery(term);
        case PREFIX:
            return new PrefixQuery(term);
            // case REGEX:
            // return new RegexQuery(term);
        case WILDCARD:
            return new WildcardQuery(term);
        case HIT:
            return new FieldScoreQuery(SearchDoc.SCORE,
                    FieldScoreQuery.Type.BYTE);
        case OR:
            return new TermQuery(term);
        default:
            return new TermQuery(term);
        }

    }

    public static DistanceQueryBuilder getDistanceQueryBuilder(
            final String latField, final String lngField,
            final QueryCriteria criteria) {
        double latitude = criteria.getLatitude();
        double longitude = criteria.getLongitude();

        final DistanceQueryBuilder distanceQueryBuilder = new DistanceQueryBuilder(
                latitude, longitude, criteria.getRadius(), latField, lngField,
                Constants.TIER_PREFIX, true);
        return distanceQueryBuilder;
    }

    org.apache.lucene.search.Query boostQuery(
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
        long now = TimeUtils.getCurrentTimeMillis();

        for (int i = 0; i < daysAgo.length; i++) {
            if (!reader.isDeleted(i)) {
                long then = DateTools.stringToTime(reader.document(i).get(
                        "indexDate"));

                daysAgo[i] = (int) ((now - then) / MSEC_PER_DAY);
            }
        }
        return new RecencyBoostingQuery(q, daysAgo, multiplier, maxDays);
    }

    static org.apache.lucene.search.Query indexDateRange(
            org.apache.lucene.search.Query q, String indexStartDateRange,
            String indexEndDateRange) {

        Filter filter = new TermRangeFilter("indexDate", indexStartDateRange,
                indexEndDateRange, true, true);
        return new FilteredQuery(q, filter);
    }

    static Filter indexDateRangeFilter(final String indexStartDateRange,
            final String indexEndDateRange) {
        return new TermRangeFilter("indexDate", indexStartDateRange,
                indexEndDateRange, true, true);
    }

    static Filter indexDateCachedFilter(final String indexStartDateRange,
            final String indexEndDateRange) {
        return new FieldCacheTermsFilter("indexDate", new String[] {
                indexStartDateRange, indexEndDateRange });
    }

    static Filter securityFilter(final String owner) {
        if (owner != null && !Constants.ALL_OWNER.equals(owner)) {
            BooleanQuery securityFilter = new BooleanQuery();
            securityFilter.add(new TermQuery(new Term("owner", owner)),
                    BooleanClause.Occur.SHOULD);
            securityFilter.add(new TermQuery(new Term("owner",
                    Constants.ALL_OWNER)), BooleanClause.Occur.SHOULD);
            return cachingFilter(new QueryWrapperFilter(securityFilter));
        } else {
            return null;
        }
    }

    static Filter cachingFilter(final Filter filter) {
        return new CachingWrapperFilter(filter);
    }

    public static Sort getScoreUsingCriteria(final String fieldName,
            final QueryPolicy policy) {
        List<SortField> scores = new ArrayList<SortField>();

        for (QueryPolicy.Field field : policy.getFields()) {
            if (field.name.toLowerCase().contains(fieldName)) {
                scores.add(field.sortOrder, new SortField(field.name,
                        getType(field), field.ascendingSort));
            }
        }
        return new Sort(scores.toArray(new SortField[scores.size()]));
    }

    public static Sort getScoreUsingPolicy(final QueryPolicy policy) {
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
                        getType(field), field.ascendingSort));
            }
        }
        return new Sort(scores.toArray(new SortField[scores.size()]));
    }

    static org.apache.lucene.search.Query keywordsQuery(
            final Analyzer analyzer, final String index, final String keywords,
            final QueryPolicy queryPolicy,
            final Collection<String> similarWords, final QueryType queryType)
            throws IOException, ParseException {
        if (GenericValidator.isBlankOrNull(keywords)) {
            throw new IllegalArgumentException("keywords not specified");
        }
        final List<String> fields = new ArrayList<String>();
        final Map<String, Float> boosts = new TreeMap<String, Float>();

        for (QueryPolicy.Field field : queryPolicy.getFields()) {
            if (GenericValidator.isBlankOrNull(field.name)) {
                throw new IllegalArgumentException(
                        "field name not specified for " + field + " in policy "
                                + queryPolicy);
            }
            fields.add(field.name);
            if (field.boost != 0) {
                boosts.put(field.name, field.boost);
            }
        }
        final MultiFieldQueryParser parser = new MultiFieldQueryParser(
                Version.LUCENE_CURRENT, fields
                        .toArray(new String[fields.size()]), analyzer, boosts);
        parser.setAllowLeadingWildcard(queryType == QueryType.PREFIX
                || queryType == QueryType.WILDCARD);
        parser.setDefaultOperator(QueryParser.Operator.AND);
        // parser.setFuzzyPrefixLength(3);
        // parser.setPhraseSlop(1);
        // TODO fix this
        if (similarWords != null) {
            final String similar = SimilarityHelper.getInstance().didYouMean(
                    index, keywords);
            if (similar != null && !keywords.equalsIgnoreCase(similar)) {
                similarWords.add(similar);
            }
        }

        final Query q = parser.parse(keywords);
        if (queryPolicy.hasSortingMultiplier()) {
            final FieldScoreQuery qf = new FieldScoreQuery(queryPolicy
                    .getSortingMultiplier(), FieldScoreQuery.Type.FLOAT);

            final CustomScoreQuery query = new CustomScoreQuery(q, qf) {
                private static final long serialVersionUID = 1L;

                @Override
                public float customScore(int doc, float subQueryScore,
                        float valSrcScore) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Applying "
                                + (subQueryScore * valSrcScore) + " to " + doc);
                    }
                    return subQueryScore * valSrcScore;
                }

                @Override
                public String name() {
                    return "CustomScoreQuery";
                }
            };
            return query;
        } else {
            // query.add(q, BooleanClause.Occur.SHOULD);
            // TermFreqVector vector = reader.getTermFreqVector(id, "name");
            // TODO add sorting
            return q;
        }
    }

    boolean existsDocument(final String id) {
        Term term = new Term(Document.ID, id);
        try {
            return searcher.docFreq(term) > 0;
        } catch (IOException ie) {
        }
        return false;
    }

    public static Query alwaysQuery() {
        return new TermQuery(new Term(Constants.ALWAYS_MATCH, String
                .valueOf(Boolean.TRUE)));
    }

    public static Query orQuery(final String[] keywords,
            final QueryPolicy queryPolicy) {
        if (keywords == null || keywords.length == 0) {
            throw new IllegalArgumentException("keywords not specified");
        }
        BooleanQuery query = new BooleanQuery();
        for (String keyword : keywords) {
            for (QueryPolicy.Field field : queryPolicy.getFields()) {
                if (GenericValidator.isBlankOrNull(field.name)) {
                    throw new IllegalArgumentException(
                            "field name not specified for " + field
                                    + " in policy " + queryPolicy);
                }
                TermQuery termQuery = new TermQuery(new Term(field.name,
                        keyword));
                query.add(termQuery, BooleanClause.Occur.SHOULD);
            }
        }
        // query.setMinimumNumberShouldMatch(1);
        return query;
    }

    private static int getType(QueryPolicy.Field field) {
        int type = SortField.INT;
        switch (field.fieldType) {
        case DOUBLE:
            type = SortField.DOUBLE;
            break;
        case INTEGER:
            type = SortField.LONG;
            break;
        case STRING:
            type = SortField.STRING;
            break;
        }
        return type;
    }

}

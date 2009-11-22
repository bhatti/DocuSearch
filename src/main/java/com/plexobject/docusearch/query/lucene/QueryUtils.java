package com.plexobject.docusearch.query.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.validator.GenericValidator;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.FieldCacheTermsFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.RangeFilter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.function.FieldScoreQuery;
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
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.lucene.QueryImpl.QueryType;

@SuppressWarnings("deprecation")
public class QueryUtils {
    private static final int MAX_MAX_DAYS = Configuration.getInstance()
            .getInteger("lucene.recency.max.days", 2 * 365);
    private static final Pattern KEYWORD_SPLIT_PATTERN = Pattern
            .compile("['\"\\.,;:\\[\\]{}()\\s]");
    private static final double DEFAULT_MULTIPLIER = Configuration
            .getInstance().getDouble("lucene.recency.multiplier", 2.0);
    private static final int MSEC_PER_DAY = 24 * 3600 * 1000;
    private static SearchScheme searchScheme = SearchScheme.SORT_WITH_TOPDOCS;

    public enum SearchScheme {
        NO_SORT, SORT_WITH_TOPDOCS, SORT_WITH_COLLECTOR
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
        final int maxDocRequested = Math.max(reader.maxDoc(), offset + len);
        TopDocs docs = null;

        switch (searchScheme) {
        case SORT_WITH_TOPDOCS:
            TopDocsCollector tdc = TopFieldCollector.create(sort,
                    maxDocRequested, true, true, false, true);

            searcher.search(query, filter, tdc);

            docs = tdc.topDocs();
            return sliceResults(offset, len, docs);
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
        case NO_SORT:
        default:
            docs = searcher.search(query.weight(searcher), filter,
                    maxDocRequested);
            return sliceResults(offset, len, docs);
        }
    }

    private Tuple sliceResults(final int offset, final int len, TopDocs docs) {
        double[][] docsAndScores;
        int maxDocsToReturn;
        maxDocsToReturn = Math.min(docs.scoreDocs.length - offset, len);
        docsAndScores = new double[maxDocsToReturn][];

        for (int i = 0; i < docs.scoreDocs.length; i++) {
            final ScoreDoc field = docs.scoreDocs[i];
            if (i < offset) {
                continue;
            } else if (i - offset == maxDocsToReturn) {
                break;
            }
            docsAndScores[i - offset] = new double[] { field.doc, field.score };
        }
        return new Tuple(docs.totalHits, docsAndScores);
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
        default:
            return new TermQuery(term);
        }

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

    static org.apache.lucene.search.Query indexDateRange(
            org.apache.lucene.search.Query q, String indexStartDateRange,
            String indexEndDateRange) {
        // final RangeQuery rangeQuery = new RangeQuery(q, "indexDate",
        // indexStartDateRange, indexEndDateRange, true, true);
        // rangeQuery.setConstantScoreRewrite(true);
        // return rangeQuery;
        return null;
    }

    static Filter indexDateRangeFilter(final String indexStartDateRange,
            final String indexEndDateRange) {
        return new RangeFilter("indexDate", indexStartDateRange,
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

    static Sort getScore(final QueryPolicy policy) {
        List<SortField> scores = new ArrayList<SortField>();
        scores.add(SortField.FIELD_SCORE);

        for (QueryPolicy.Field field : policy.getFields()) {
            if (field.sortOrder > 0) {
                scores.add(SortField.FIELD_DOC);
            }
        }

        for (QueryPolicy.Field field : policy.getFields()) {
            // TODO add types
            if (field.sortOrder > 0) {
                scores.set(field.sortOrder, new SortField(field.name,
                        SortField.INT, field.ascendingSort));
            }
        }

        return new Sort(scores.toArray(new SortField[scores.size()]));
    }

    static org.apache.lucene.search.Query keywordsQuery(final String index,
            final String keywords, final QueryPolicy policy,
            final Collection<String> similarWords, final QueryType queryType)
            throws IOException, ParseException {
        if (GenericValidator.isBlankOrNull(keywords)) {
            throw new IllegalArgumentException("keywords not specified");
        }
        final BooleanQuery query = new BooleanQuery();
        for (QueryPolicy.Field field : policy.getFields()) {
            if (GenericValidator.isBlankOrNull(field.name)) {
                throw new IllegalArgumentException(
                        "field name not specified for " + field + " in policy "
                                + policy);
            }
            final String[] words = keywords.startsWith("\"")
                    || keywords.startsWith("'") ? new String[] { keywords
                    .substring(1, keywords.length() - 1) }
                    : KEYWORD_SPLIT_PATTERN.split(keywords);
            float orderFactor = 1.0F; // give higher boost to last words
            for (String word : words) {
                word = word.trim();
                if (word.length() == 0) {
                    continue;
                }
                final Term term = new Term(field.name, word);

                final org.apache.lucene.search.Query q = QueryUtils
                        .createQuery(queryType, term);
                if (field.boost != 0) {
                    q.setBoost(field.boost * orderFactor);
                }

                query.add(q, BooleanClause.Occur.SHOULD);
                orderFactor += 0.2F;
            }
        }
        if (similarWords != null) {
            final String similar = SimilarityHelper.getInstance().didYouMean(
                    index, keywords);
            if (similar != null && !keywords.equalsIgnoreCase(similar)) {
                similarWords.add(similar);
            }
        }
        // TermFreqVector vector = reader.getTermFreqVector(id, "name");
        // TODO add sorting
        return query;
    }

    boolean existsDocument(final String id) {
        Term term = new Term(Document.ID, id);
        try {
            return searcher.docFreq(term) > 0;
        } catch (IOException ie) {
        }
        return false;
    }
}

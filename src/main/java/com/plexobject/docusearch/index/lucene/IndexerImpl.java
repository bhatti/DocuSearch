package com.plexobject.docusearch.index.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.spatial.tier.projections.CartesianTierPlotter;
import org.apache.lucene.spatial.tier.projections.IProjector;
import org.apache.lucene.spatial.tier.projections.SinusoidalProjector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.ibm.icu.util.Calendar;
import com.plexobject.docusearch.cache.CacheFlusher;
import com.plexobject.docusearch.converter.Constants;
import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.converter.HtmlToTextConverter;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.Pair;
import com.plexobject.docusearch.domain.Tuple;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.index.Indexer;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.lucene.analyzer.SimilarityHelper;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.util.TimeUtils;

/**
 * @author Shahzad Bhatti
 * 
 */
// http://www.liferay.com/web/guest/community/forums/-/message_boards/message/4048205
// http://www.opensubscriber.com/message/java-user@lucene.apache.org/3646117.html
public class IndexerImpl implements Indexer {
    private static final Logger LOGGER = Logger.getLogger(IndexerImpl.class);
    private static final boolean OPTIMIZE = true;
    private int numIndexed;
    private final Map<String, Boolean> INDEXED_FIELDS = new TreeMap<String, Boolean>();
    private final HtmlToTextConverter htmlToTextConverter = new HtmlToTextConverter();

    //
    private final String indexName;

    private final Directory dir;

    public IndexerImpl(final File dir) {
        this(LuceneUtils.toFSDirectory(dir), dir.getName());
    }

    public IndexerImpl(final Directory dir, final String indexName) {
        this.dir = dir;
        this.indexName = indexName;
    }

    @Override
    public synchronized int index(final IndexPolicy policy,
            final Iterator<List<Document>> docsIt, final String secondaryId,
            final boolean deleteExisting) {
        if (policy == null) {
            throw new NullPointerException("index policy not specified");
        }
        IndexWriter writer = null;
        IndexReader reader = null;
        IndexSearcher searcher = null;
        Analyzer analyzer = null;
        final Timer timer = Metric.newTimer("IndexerImpl.index");
        int succeeded = 0;
        int count = 0;
        try {
            Tuple tuple = open(policy);

            writer = tuple.first();
            reader = tuple.second();
            searcher = tuple.third();
            analyzer = writer.getAnalyzer();
            while (docsIt.hasNext()) {
                List<Document> docs = docsIt.next();
                for (Document doc : docs) {
                    try {
                        index(count++, writer, policy, doc, secondaryId,
                                searcher, deleteExisting);
                        succeeded++;

                        if (succeeded % 1000 == 0) {
                            timer.lapse("--succeeded indexing " + succeeded
                                    + " documents with analyzer " + analyzer);
                        }
                    } catch (final Exception e) {
                        LOGGER.error("Error indexing " + doc, e);
                    }
                }
            }
        } catch (final Exception e) {
            LOGGER.error("Error indexing documents", e);
        } finally {
            timer.stop("succeeded indexing " + succeeded + "/" + count
                    + " documents with analyzer " + analyzer);
            close(writer, reader, searcher);
            try {
                if (policy.isAddToDictionary()) {
                    SimilarityHelper.getInstance().saveTrainingSpellChecker(
                            indexName);
                }
            } catch (Exception e) {
                LOGGER.error("failed to add spellings", e);
            }
            // flushing cache after reindex
            CacheFlusher.getInstance().flushCaches();
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("#### Indexed " + succeeded + "/" + count
                    + " with analyzer " + analyzer);
        }
        return succeeded;
    }

    /**
     * This method removes documents with given id
     * 
     * @return number of documents that were indexed successfully.
     */
    @Override
    public synchronized int removeIndexedDocuments(final String database,
            String secondaryIdName,
            final Collection<Pair<String, String>> primaryAndSecondaryIds,
            int olderThanDays) {
        IndexWriter writer = null;
        IndexReader reader = null;
        IndexSearcher searcher = null;
        final BooleanQuery topQuery = new BooleanQuery();

        int before = 0;
        int after = 0;
        int failed = 0;
        final Timer timer = Metric.newTimer("IndexerImpl.index");
        try {
            Tuple tuple = open(null);

            writer = tuple.first();
            reader = tuple.second();
            searcher = tuple.third();

            Date startDate = new Date(0L);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(TimeUtils.getCurrentTime());
            calendar.add(Calendar.DATE, -olderThanDays);
            Date endDate = calendar.getTime();

            if (primaryAndSecondaryIds != null
                    && primaryAndSecondaryIds.size() > 0) {
                for (Pair<String, String> primaryAndSecondaryId : primaryAndSecondaryIds) {
                    try {
                        final BooleanQuery booleanQuery = new BooleanQuery();
                        Filter filter = null;

                        booleanQuery.add(new TermQuery(new Term(Document.ID,
                                primaryAndSecondaryId.getFirst())), Occur.MUST);
                        if (!GenericValidator
                                .isBlankOrNull(primaryAndSecondaryId
                                        .getSecond())) {
                            booleanQuery.add(new TermQuery(new Term(
                                    Document.DATABASE, database)), Occur.MUST);
                            booleanQuery.add(new TermQuery(new Term(
                                    secondaryIdName, primaryAndSecondaryId
                                            .getSecond())), Occur.MUST);
                        }
                        if (olderThanDays > 0) {
                            final String indexStartDateRange = DateTools
                                    .dateToString(startDate,
                                            DateTools.Resolution.DAY);
                            final String indexEndDateRange = DateTools
                                    .dateToString(endDate,
                                            DateTools.Resolution.DAY);
                            filter = new TermRangeFilter("indexDate",
                                    indexStartDateRange, indexEndDateRange,
                                    true, true);
                        }
                        Query q = filter == null ? booleanQuery
                                : new FilteredQuery(booleanQuery, filter);
                        topQuery.add(q, Occur.SHOULD);
                    } catch (Exception e) {
                        LOGGER.error("Failed to remove index for "
                                + primaryAndSecondaryId, e);
                        failed++;
                    }
                }
            } else if (olderThanDays > 0) {
                final BooleanQuery booleanQuery = new BooleanQuery();
                Filter filter = null;

                booleanQuery.add(new TermQuery(new Term(Document.DATABASE,
                        database)), Occur.MUST);
                final String indexStartDateRange = DateTools.dateToString(
                        startDate, DateTools.Resolution.DAY);
                final String indexEndDateRange = DateTools.dateToString(
                        endDate, DateTools.Resolution.DAY);
                filter = new TermRangeFilter("indexDate", indexStartDateRange,
                        indexEndDateRange, true, true);
                Query q = new FilteredQuery(booleanQuery, filter);
                topQuery.add(q, Occur.SHOULD);
            }

            before = searcher.search(topQuery, 1).totalHits;
            writer.deleteDocuments(topQuery);

        } catch (final Exception e) {
            LOGGER.error("Faield to remove index", e);
        } finally {
            timer.stop("succeeded removing index "
                    + (primaryAndSecondaryIds != null ? primaryAndSecondaryIds
                            .size() : 0) + ", before " + before + ", after "
                    + after);
            close(writer, reader, searcher);
            after = getCount(topQuery);
            // flushing cache after reindex
            CacheFlusher.getInstance().flushCaches();
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("#### Removed index documents "
                    + (primaryAndSecondaryIds != null ? primaryAndSecondaryIds
                            .size() : 0) + ", before " + before + ", after "
                    + after + ", failed " + failed);
        }
        return before - after;
    }

    IndexWriter createWriter(final IndexPolicy policy) throws IOException {
        return LuceneUtils.newWriter(dir, policy != null ? policy.getAnalyzer()
                : null);
        // return LuceneUtils.newThreadedWriter(dir);
    }

    @SuppressWarnings("deprecation")
    private synchronized void index(final int count, final IndexWriter writer,
            final IndexPolicy policy, final Document doc,
            final String secondaryId, final IndexSearcher searcher,
            final boolean deleteExisting) throws CorruptIndexException,
            IOException {
        final Map<String, Object> map = doc.getAttributes();
        final JSONObject json = Converters.getInstance().getConverter(
                Object.class, JSONObject.class).convert(map);
        final org.apache.lucene.document.Document ldoc = new org.apache.lucene.document.Document();
        // for always matching
        ldoc.add(new Field(Constants.ALWAYS_MATCH,
                String.valueOf(Boolean.TRUE), Field.Store.NO,
                Field.Index.NOT_ANALYZED));

        ldoc.add(new Field(Document.DATABASE, doc.getDatabase(),
                Field.Store.YES, Field.Index.NOT_ANALYZED));
        ldoc.add(new Field(Document.ID, doc.getId(), Field.Store.YES,
                Field.Index.NOT_ANALYZED));
        //
        if (doc.hasSecondaryId()) {
            ldoc.add(new Field(Document.SECONDARY_ID, doc.getSecondaryId(),
                    Field.Store.YES, Field.Index.NOT_ANALYZED));
        }

        if (policy.hasOwner()) {
            ldoc.add(new Field(Constants.OWNER, policy.getOwner(),
                    Field.Store.YES, Field.Index.NOT_ANALYZED));
        }

        ldoc.add(new Field("indexDate", DateTools.dateToString(TimeUtils
                .getCurrentTime(), DateTools.Resolution.DAY), Field.Store.YES,
                Field.Index.NOT_ANALYZED));

        if (policy.getBoost() > 0) {
            ldoc.setBoost(policy.getBoost());
        }
        if (policy.getScore() > 0) {
            ldoc.add(new Field("score", Integer.toString(policy.getScore()),
                    Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
        }
        final Float scoreMultiplier = (Float) doc
                .get(Constants.SCORE_MULTIPLIER);
        if (scoreMultiplier != null) {
            ldoc.setBoost(scoreMultiplier);
        }
        double spatialLatitude = 0;
        double spatialLongitude = 0;
        for (String name : doc.getAttributeNames()) {
            try {
                IndexPolicy.Field field = policy.getField(name);
                if (field == null) {
                    if (!INDEXED_FIELDS.containsKey(name)) {
                        INDEXED_FIELDS.put(name, Boolean.TRUE);
                    }
                    continue; // skip field that are not specified in the policy
                } else {
                    if (!INDEXED_FIELDS.containsKey(name)) {
                        INDEXED_FIELDS.put(name, Boolean.TRUE);
                    }
                }
                String value = IndexUtils.getValue(json, name);
                if (value != null) {
                    if (value.length() > 0
                            && (field.spatialLatitude || field.spatialLongitude)) {
                        double d = Double.valueOf(value);
                        if (field.spatialLatitude) {
                            spatialLatitude = d;
                        } else if (field.spatialLongitude) {
                            spatialLongitude = d;
                        }
                        value = NumericUtils.doubleToPrefixCoded(d);
                    } else if (value.length() > 0 && field.htmlToText) {
                        value = htmlToTextConverter.convert(value);
                    }
                    value = value.toLowerCase();
                    final String storeAs = field.storeAs != null
                            && field.storeAs.length() > 0 ? field.storeAs : doc
                            .getDatabase()
                            + "." + name;

                    final Field.Store store = field.storeInIndex
                            || field.spatialLatitude || field.spatialLongitude ? Field.Store.YES
                            : Field.Store.NO;
                    final Field.Index index = field.tokenize ? Field.Index.TOKENIZED
                            : field.analyze ? Field.Index.ANALYZED
                                    : Field.Index.NOT_ANALYZED;
                    final Field.TermVector termVector = field.tokenize ? Field.TermVector.YES
                            : Field.TermVector.NO;
                    final Field locField = field.spatialLatitude
                            || field.spatialLongitude ? new Field(storeAs,
                            value, Field.Store.YES, Field.Index.NOT_ANALYZED)
                            : new Field(storeAs, value, store, index,
                                    termVector);
                    locField.setBoost(field.boost);
                    ldoc.add(locField);
                    if (policy.isAddToDictionary()) {
                        SimilarityHelper.getInstance().trainSpellChecker(
                                indexName, value);
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Indexing " + name + " using " + locField
                                + ", doc " + doc + ", policy " + policy);
                    }
                }
            } catch (JSONException e) {
                LOGGER.error("Failed to index value for " + name + " from "
                        + json + " due to ", e);
                throw new RuntimeException(e.toString());
            }
        }

        if (count == 0 && LOGGER.isInfoEnabled() && INDEXED_FIELDS.size() > 2) {
            LOGGER.info("Will index " + INDEXED_FIELDS.keySet() + " field for "
                    + indexName + " from " + doc.getDatabase());
        }
        //
        if (spatialLatitude != 0 && spatialLongitude != 0) {
            final IProjector projector = new SinusoidalProjector();
            // 9 = 100 miles
            int startTier = 4; // 5; // About 1000 mile bestFit
            final int endTier = 25; // 15; // about 1 mile bestFit
            for (; startTier <= endTier; startTier++) {
                CartesianTierPlotter ctp = new CartesianTierPlotter(startTier,
                        projector, Constants.TIER_PREFIX);
                final double boxId = ctp.getTierBoxId(spatialLatitude,
                        spatialLongitude);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("*********Adding field "
                            + ctp.getTierFieldName() + ":" + boxId
                            + ", spatialLatitude " + spatialLatitude
                            + ", spatialLongitude " + spatialLongitude);
                }
                ldoc.add(new Field(ctp.getTierFieldName(), NumericUtils
                        .doubleToPrefixCoded(boxId), Field.Store.YES,
                        Field.Index.NOT_ANALYZED_NO_NORMS));
            }
        }

        boolean newDocument = true;
        boolean deletedOldDocs = false;

        if (deleteExisting || !GenericValidator.isBlankOrNull(secondaryId)) {
            writer.deleteDocuments(LuceneUtils.docQuery(doc.getDatabase(), doc
                    .getId(), secondaryId, secondaryId != null ? (String) doc
                    .get(secondaryId) : null));
            deletedOldDocs = true;
        } else {
            final int[] counts = new int[1];
            searcher.search(LuceneUtils
                    .docQuery(doc.getDatabase(), doc.getId()), null,
                    new Collector() {
                        @Override
                        public boolean acceptsDocsOutOfOrder() {
                            return false;
                        }

                        @Override
                        public void collect(int hits) throws IOException {
                            counts[0] += hits;
                        }

                        @Override
                        public void setNextReader(IndexReader arg0, int arg1)
                                throws IOException {
                        }

                        @Override
                        public void setScorer(Scorer arg0) throws IOException {
                        }
                    });
            newDocument = counts[0] == 0;
        }

        if (newDocument) {
            writer.addDocument(ldoc);
        } else {
            Term idTerm = new Term(Document.ID, doc.getId());
            writer.updateDocument(idTerm, ldoc);
        }

        if (++numIndexed % 1000 == 0 && LOGGER.isInfoEnabled()) {
            LOGGER.info(numIndexed + ": Indexing " + doc.getId()
                    + " secondaryId " + secondaryId + ", newDocument "
                    + newDocument + ", deletedOldDocs " + deletedOldDocs
                    + " with policy " + policy);
        }
    }

    private Tuple open(final IndexPolicy policy) throws IOException {
        final IndexWriter writer = createWriter(policy);
        final IndexReader reader = IndexReader.open(dir, false);
        final IndexSearcher searcher = new IndexSearcher(reader);

        return new Tuple(writer, reader, searcher);
    }

    private int getCount(Query q) {
        IndexReader reader = null;
        IndexSearcher searcher = null;

        try {
            reader = IndexReader.open(dir, false);
            searcher = new IndexSearcher(reader);
            return searcher.search(q, 1).totalHits;
        } catch (CorruptIndexException e) {
            LOGGER.error("failed to get count", e);
            return 0;
        } catch (IOException e) {
            LOGGER.error("failed to get count", e);
            return 0;
        } finally {
            try {
                reader.close();
            } catch (Exception e) {
                LOGGER.error("failed to close reader", e);
            }
            try {
                searcher.close();
            } catch (Exception e) {
                LOGGER.error("failed to close searcher", e);
            }
        }
    }

    private void close(final IndexWriter writer, final IndexReader reader,
            final IndexSearcher searcher) {
        if (writer != null) {
            try {
                if (OPTIMIZE) {
                    writer.optimize();
                }
            } catch (Exception e) {
                LOGGER.error("failed to optimize", e);
            } finally {
                try {
                    writer.close();
                } catch (Exception e) {
                    LOGGER.error("failed to close", e);
                }
            }
        }
        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
                LOGGER.error("failed to close reader", e);
            }
        }
        if (searcher != null) {
            try {
                searcher.close();
            } catch (Exception e) {
                LOGGER.error("failed to close searcher", e);
            }
        }

    }
}

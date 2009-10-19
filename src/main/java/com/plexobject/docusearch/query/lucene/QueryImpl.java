/**
 * 
 */
package com.plexobject.docusearch.query.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.function.CustomScoreQuery;
import org.apache.lucene.search.function.FieldScoreQuery;
import org.apache.lucene.store.Directory;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.SearchException;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.SearchDocBuilder;
import com.plexobject.docusearch.query.SearchDocList;

/**
 * @author bhatti@plexobject.com
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

	private IndexReader reader;
	private IndexSearcher searcher;

	public QueryImpl(final File dir) {
		this(LuceneUtils.toFSDirectory(dir));
	}

	public QueryImpl(final Directory dir) {
		try {
			reader = IndexReader.open(dir, true); // reopen()
			searcher = new IndexSearcher(reader);
		} catch (CorruptIndexException e) {
			throw new SearchException(e);
		} catch (IOException e) {
			throw new SearchException(e);
		}
	}

	@Override
	public SearchDocList search(final QueryCriteria criteria, int start,
			int limit) {
		if (start <= 0) {
			start = 1;
		}
		if (limit <= 0) {
			limit = DEFAULT_LIMIT;
		}
		if (limit > MAX_LIMIT) {
			limit = DEFAULT_LIMIT;
		}
		try {
			org.apache.lucene.search.Query q = null;

			if (criteria.isScoreQuery()) {
				q = hitQuery(searcher);
			} else if (criteria.hasKeywords()) {
				q = keywordsQuery(searcher, criteria);
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

			return convert(searcher, ((HitPageCollector) hc)
					.getTotalAvailable(), docs);
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
		}
	}

	@SuppressWarnings("unchecked")
	private SearchDocList convert(final IndexSearcher searcher,
			final int totalHits, final ScoreDoc[] scoreDocs)
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
		return new SearchDocList(totalHits, results);
	}

	private org.apache.lucene.search.Query hitQuery(final IndexSearcher searcher)
			throws IOException {
		return new FieldScoreQuery(SearchDoc.SCORE, FieldScoreQuery.Type.BYTE);

	}

	private org.apache.lucene.search.Query keywordsQuery(
			final IndexSearcher searcher, final QueryCriteria criteria)
			throws IOException, ParseException {
		if (criteria.hasFields()) {
			final String[] fields = criteria.getFields();
			final BooleanClause.Occur[] occurs = new BooleanClause.Occur[fields.length];
			for (int i = 0; i < fields.length; i++) {
				occurs[i] = BooleanClause.Occur.SHOULD;
			}
			return MultiFieldQueryParser.parse(criteria.getKeywords(), fields,
					occurs, LuceneUtils.getDefaultAnalyzer());
		} else {
			QueryParser parser = new QueryParser("content", LuceneUtils
					.getDefaultAnalyzer());
			return parser.parse(criteria.getKeywords());
		}
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
		return null; // TODO new RangeQuery(q, "indexDate", indexStartDateRange,
		// indexEndDateRange, true, true)
	}

	public boolean existsDocument(final String id) {
		Term term = new Term(Document.ID, id);
		try {
			return searcher.docFreq(term) > 0;
		} catch (IOException ie) {
		}
		return false;
	}

}

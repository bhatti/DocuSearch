package com.plexobject.docusearch.lucene;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.solr.analysis.SynonymMap;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.index.lucene.ThreadedIndexWriter;
import com.plexobject.docusearch.lucene.analyzer.SynonymAnalyzer;

/**
 * @author bhatti@plexobject.com
 * 
 */
public final class LuceneUtils {
	private static final Logger LOGGER = Logger.getLogger(LuceneUtils.class);
	private static final String LUCENE_ANALYZER = "lucene.analyzer";
	private static final String SYNONYM_analyzerType = "SynonymAnalyzer";
	private static final String analyzerType = Configuration.getInstance()
			.getProperty(LUCENE_ANALYZER);

	public static final String DEFAULT_OPERATOR = System.getProperty(
			"lucene.operator", "OR");
	public static final File INDEX_DIR = new File(System
			.getProperty("user.home"), System.getProperty("lucene.dir",
			".lucene"));

	public static final int RAM_BUF = Integer.getInteger("lucene.ram", 16);

	public static final int BATCH_SIZE = Integer
			.getInteger("lucene.batch", 200);

	public static final int COMMIT_MIN = Integer.getInteger(
			"lucene.commit.min", 5000);

	private static final boolean LUCENE_DEBUG = Boolean
			.getBoolean("lucene.debug");

	private static Analyzer defaultAnalyzer;

	private LuceneUtils() {
	}

	public static synchronized Analyzer getDefaultAnalyzer() {
		if (defaultAnalyzer == null) {
			if (SYNONYM_analyzerType.equals(analyzerType)) {
				final SynonymMap map = new SynonymMap(true);
				defaultAnalyzer = new SynonymAnalyzer(map);

			} else {
				defaultAnalyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
			}
			if (LOGGER.isInfoEnabled()) {
				LOGGER.info("Default analyzer is " + defaultAnalyzer);
			}
		}
		return defaultAnalyzer;
	}

	public static synchronized void setDefaultAnalyzer(final Analyzer a) {
		defaultAnalyzer = a;
	}

	public static Query docQuery(final String viewname, final String id) {
		BooleanQuery q = new BooleanQuery();
		q.add(new TermQuery(new Term(Document.DATABASE, viewname)), Occur.MUST);
		q.add(new TermQuery(new Term(Document.ID, id)), Occur.MUST);
		return q;
	}

	public static IndexWriter newWriter(final Directory dir) throws IOException {
		final IndexWriter writer = new IndexWriter(dir, getDefaultAnalyzer(),
				MaxFieldLength.UNLIMITED);

		return configWriter(writer);
	}

	public static IndexWriter newThreadedWriter(final Directory dir)
			throws IOException {
		final IndexWriter writer = new ThreadedIndexWriter(dir, getDefaultAnalyzer(), true,
				MaxFieldLength.UNLIMITED);
		return configWriter(writer);
	}

	public static String parseDate(String s) throws ParseException {
		return DateTools.dateToString(new SimpleDateFormat("yyyy-MM-dd")
				.parse(s), DateTools.Resolution.MILLISECOND);
	}

	private static IndexWriter configWriter(final IndexWriter writer) {
		// Customize merge policy.
		final LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy(writer);
		mp.setMergeFactor(Integer.MAX_VALUE);
		mp.setMaxMergeMB(1000);
		mp.setUseCompoundFile(false);
		writer.setMergePolicy(mp);

		writer.setRAMBufferSizeMB(RAM_BUF);

		if (LUCENE_DEBUG) {
			writer.setInfoStream(System.err);
		}

		return writer;
	}

	public static Directory toFSDirectory(final File dir) {
		// Create index directory if missing.
		if (!dir.exists()) {
			if (!dir.mkdirs()) {
				LOGGER.fatal("Unable to create index dir "
						+ dir.getAbsolutePath());
				throw new Error("Unable to create index dir "
						+ dir.getAbsolutePath());
			}
		}

		// Verify index directory is writable.
		if (!dir.canWrite()) {
			LOGGER.fatal(dir.getAbsolutePath() + " is not writable.");
			throw new Error(dir.getAbsolutePath() + " is not writable.");
		}

		try {
			final Directory d = FSDirectory.open(dir);

			// Check index prior to startup if it exists.
			if (IndexReader.indexExists(d)) {
				final CheckIndex check = new CheckIndex(d);
				final CheckIndex.Status status = check.checkIndex();
				if (status.clean) {
					LOGGER.debug("Index is clean.");
				} else {
					LOGGER.warn("Index is not clean.");
				}
			}
			if (IndexWriter.isLocked(d)) {
				LOGGER.warn("***Unlocking " + d + " directory for indexing");
				IndexWriter.unlock(d);
			}
			return d;

		} catch (IOException e) {
			LOGGER.error("Failed to unlock index", e);
			throw new RuntimeException(e);
		}
	}

	public static Token[] tokensFromAnalysis(Analyzer analyzer, String text)
			throws IOException {
		TokenStream stream = analyzer.tokenStream("contents", new StringReader(
				text));

		return tokensFromAnalysis(stream);
	}

	@SuppressWarnings("deprecation")
	public static Token[] tokensFromAnalysis(TokenStream stream)
			throws IOException {
		final List<Token> tokenList = new ArrayList<Token>();
		while (true) {
			if (!stream.incrementToken()) {
				break;
			}
			tokenList.add(stream.next());
		}

		return (Token[]) tokenList.toArray(new Token[0]);
	}

}

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

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.index.lucene.ThreadedIndexWriter;
import com.plexobject.docusearch.lucene.analyzer.SynonymAnalyzer;

/**
 * @author Shahzad Bhatti
 * 
 */
public final class LuceneUtils {
    private static final Logger LOGGER = Logger.getLogger(LuceneUtils.class);
    private static final String LUCENE_ANALYZER = "lucene.analyzer";
    private static final String SYNONYM_ANALYZER_TYPE = "SynonymAnalyzer";
    private static final String ANALYZER_TYPE = Configuration.getInstance()
            .getProperty(LUCENE_ANALYZER, SYNONYM_ANALYZER_TYPE);

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

    private static volatile Analyzer defaultAnalyzer;

    private LuceneUtils() {
    }

    public static Analyzer getDefaultAnalyzer() {
        if (defaultAnalyzer == null) {
            synchronized (ANALYZER_TYPE) {
                if (defaultAnalyzer == null) {
                    defaultAnalyzer = getAnalyzer(ANALYZER_TYPE);
                }
            }
        }
 
        return defaultAnalyzer;
    }

    public static Analyzer getAnalyzer(final String type) {
        if (SYNONYM_ANALYZER_TYPE.equalsIgnoreCase(type)) {
            return new SynonymAnalyzer();

        } else {
            return new StandardAnalyzer(Version.LUCENE_CURRENT);
        }
    }

    public static void setDefaultAnalyzer(final Analyzer analyzer) {
        synchronized (ANALYZER_TYPE) {
            defaultAnalyzer = analyzer;
        }
    }

    public static Query docQuery(final String viewname, final String id) {
        BooleanQuery q = new BooleanQuery();
        q.add(new TermQuery(new Term(Document.DATABASE, viewname)), Occur.MUST);
        q.add(new TermQuery(new Term(Document.ID, id)), Occur.MUST);
        return q;
    }

    public static IndexWriter newWriter(final Directory dir,
            final String analyzer) throws IOException {
        if (IndexWriter.isLocked(dir)) {
            LOGGER.warn("***Unlocking " + dir + " directory for indexing");
            IndexWriter.unlock(dir);
        }
        final IndexWriter writer = new IndexWriter(
                dir,
                analyzer == null ? getDefaultAnalyzer() : getAnalyzer(analyzer),
                MaxFieldLength.UNLIMITED);

        return configWriter(writer);
    }

    public static IndexWriter newThreadedWriter(final Directory dir)
            throws IOException {
        final IndexWriter writer = new ThreadedIndexWriter(dir,
                getDefaultAnalyzer(), true, MaxFieldLength.UNLIMITED);
        return configWriter(writer);
    }

    public static String parseDate(String s) throws ParseException {
        return DateTools.dateToString(new SimpleDateFormat("yyyy-MM-dd")
                .parse(s), DateTools.Resolution.MILLISECOND);
    }

    private static IndexWriter configWriter(final IndexWriter writer) {
        // Customize merge policy.
        final LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy(writer);
        mp.setMergeFactor(10000);
        mp.setMaxMergeMB(1000);
        mp.setUseCompoundFile(false);
        writer.setMergePolicy(mp);
        writer.setMaxBufferedDocs(10000);
        writer.setMaxMergeDocs(10000);
        writer.setMaxFieldLength(10000);
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

    /***
     * Return a list of tokens according to a test string format: a b c =>
     * returns List<Token> [a,b,c] a/b => tokens a and b share the same spot
     * (b.positionIncrement=0) a,3/b/c => a,b,c all share same position
     * (a.positionIncrement=3, b.positionIncrement=0, c.positionIncrement=0)
     * a,1,10,11 => "a" with positionIncrement=1, startOffset=10, endOffset=11
     */
    public static List<Token> tokens(String str) {
        String[] arr = str.split(" ");
        List<Token> result = new ArrayList<Token>();
        for (int i = 0; i < arr.length; i++) {
            String[] toks = arr[i].split("/");
            String[] params = toks[0].split(",");

            int posInc;
            int start;
            int end;

            if (params.length > 1) {
                posInc = Integer.parseInt(params[1]);
            } else {
                posInc = 1;
            }

            if (params.length > 2) {
                start = Integer.parseInt(params[2]);
            } else {
                start = 0;
            }

            if (params.length > 3) {
                end = Integer.parseInt(params[3]);
            } else {
                end = start + params[0].length();
            }

            Token t = new Token(params[0].toCharArray(), 0, params[0].length(),
                    start, end);
            t.setType("TEST");
            t.setPositionIncrement(posInc);

            result.add(t);
            for (int j = 1; j < toks.length; j++) {
                t = new Token(toks[j].toCharArray(), 0, toks[j].length(), 0, 0);
                t.setType("TEST");
                t.setPositionIncrement(0);
                result.add(t);
            }
        }
        return result;
    }

}

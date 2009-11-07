package com.plexobject.docusearch.lucene.analyzer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.search.spell.SpellChecker;

public class SimilarityHelper {
    @SuppressWarnings("unused")
    private static final String TRAINING_SPELL_CHECKER_EXT = ".tsc";
    @SuppressWarnings("unused")
    private static final String COMPILED_SPELLING_FILE_EXT = ".csc";
    @SuppressWarnings("unused")
    private static final Logger LOGGER = Logger
            .getLogger(SimilarityHelper.class);
    @SuppressWarnings("unused")
    private final static int NGRAM_LENGTH = 7;
    @SuppressWarnings("unused")
    private final static double MATCH_WEIGHT = 0.0;
    @SuppressWarnings("unused")
    private final static double DELETE_WEIGHT = -5.0;
    @SuppressWarnings("unused")
    private final static double INSERT_WEIGHT = -5.0;
    @SuppressWarnings("unused")
    private final static double SUBSTITUTE_WEIGHT = -5.0;
    @SuppressWarnings("unused")
    private final static double TRANSPOSE_WEIGHT = -5.0;
    @SuppressWarnings("unused")
    private volatile Map<String, SpellChecker> scMap = new HashMap<String, SpellChecker>();
    //private volatile Map<String, CompiledSpellChecker> cscMap = new HashMap<String, CompiledSpellChecker>();
    //private volatile Map<String, TrainSpellChecker> tscMap = new HashMap<String, TrainSpellChecker>();
    private static final SimilarityHelper INSTANCE = new SimilarityHelper();

    public static SimilarityHelper getInstance() {
        return INSTANCE;
    }

    public String didYouMean(final String index, final String contents) {
        String similar = null;
/*
        try {
            similar = getCompiledSpellChecker(index).didYouMean(contents);
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
            LOGGER.warn(
                    "Failed to get didYouMean " + index + " -- " + contents, e);
        }
        try {
            if (similar == null) {
                final String[] matched = getSpellChecker(index).suggestSimilar(
                        contents, 1);
                if (matched.length > 0) {
                    similar = matched[0];
                }
            }
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
            LOGGER.warn(
                    "Failed to get didYouMean " + index + " -- " + contents, e);
        }
*/
        return similar;
    }

    public void trainSpellChecker(final String index, final String contents) {
	/*
        try {

            getTrainSpellChecker(index).train(contents);
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
            LOGGER.warn("Failed to traing " + index + " -- " + contents, e);
        }
	*/
    }

    public void saveTrainingSpellChecker(final String index) throws IOException {
	/*
        saveCompiledSpellChecker(getTrainSpellChecker(index), new File(
                LuceneUtils.INDEX_DIR, index + TRAINING_SPELL_CHECKER_EXT), new File(
                LuceneUtils.INDEX_DIR, index + COMPILED_SPELLING_FILE_EXT));
	*/
    }

    // PRIVATE METHODS
    // 
    /*
    private SpellChecker getSpellChecker(final String index) throws IOException {
        SpellChecker sc = scMap.get(index);
        if (sc == null) {
            synchronized (scMap) {
                if (sc == null) {
                    sc = getSpellChecker(new File(LuceneUtils.INDEX_DIR, index
                            + ".sc"));
                    scMap.put(index, sc);
                }
            }
        }
        return sc;
    }

    private TrainSpellChecker getTrainSpellChecker(final String index)
            throws IOException {
        TrainSpellChecker tsc = tscMap.get(index);
        if (tsc == null) {
            synchronized (tscMap) {
                if (tsc == null) {
                    tsc = getTrainSpellChecker(new File(LuceneUtils.INDEX_DIR,
                            index + TRAINING_SPELL_CHECKER_EXT));
                    tscMap.put(index, tsc);
                }
            }
        }
        return tsc;
    }

    private CompiledSpellChecker getCompiledSpellChecker(final String index)
            throws IOException {
        CompiledSpellChecker csc = cscMap.get(index);
        if (csc == null) {
            synchronized (cscMap) {
                if (csc == null) {
                    csc = getCompiledSpellChecker(new File(
                            LuceneUtils.INDEX_DIR, index
                                    + COMPILED_SPELLING_FILE_EXT));
                    cscMap.put(index, csc);
                }
            }
        }
        return csc;
    }

    private CompiledSpellChecker getCompiledSpellChecker(final File file)
            throws IOException {
        if (file.exists()) {
            InputStream in = null;
            try {
                in = new FileInputStream(file);
                final ObjectInputStream objIn = new ObjectInputStream(
                        new BufferedInputStream(in));
                return (CompiledSpellChecker) objIn.readObject();
            } catch (Throwable e) {
                LOGGER.error("Failed to initialize CompiledSpellChecker "
                        + file, e);
                throw new IOException(
                        "Failed to initialize CompiledSpellChecker " + file, e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                    }
                }
            }
        }
        throw new FileNotFoundException(file + " does not exist");
    }

    private TrainSpellChecker getTrainSpellChecker(final File file)
            throws IOException {
        if (file.exists()) {
            InputStream in = null;
            try {
                in = new FileInputStream(file);
                final ObjectInputStream objIn = new ObjectInputStream(
                        new BufferedInputStream(in));
                return (TrainSpellChecker) objIn.readObject();
            } catch (Throwable e) {
                LOGGER.error("Failed to initialize CompiledSpellChecker "
                        + file, e);
                throw new IOException(
                        "Failed to initialize CompiledSpellChecker " + file, e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                    }
                }
            }
        } else {
            FixedWeightEditDistance fixedEdit = new FixedWeightEditDistance(
                    MATCH_WEIGHT, DELETE_WEIGHT, INSERT_WEIGHT,
                    SUBSTITUTE_WEIGHT, TRANSPOSE_WEIGHT);
            NGramProcessLM lm = new NGramProcessLM(NGRAM_LENGTH);
            TokenizerFactory tokenizerFactory = new StandardBgramTokenizerFactory();
            return new TrainSpellChecker(lm, fixedEdit, tokenizerFactory);
        }
    }

    private void saveCompiledSpellChecker(final TrainSpellChecker tsc,
            final File tscFile, final File cscFile) throws IOException {
        ObjectOutputStream objOut = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(tscFile)));
        objOut.writeObject(tsc);
        objOut.close();
        tsc.pruneTokens(NGRAM_LENGTH);
        objOut = new ObjectOutputStream(new BufferedOutputStream(
                new FileOutputStream(cscFile)));
        tsc.compileTo(objOut);
        objOut.close();
    }

    private SpellChecker getSpellChecker(final File file) throws IOException {
        if (file.exists()) {
            Directory spellDir = FSDirectory.open(file);
            return new SpellChecker(spellDir);
        }
        throw new FileNotFoundException(file + " does not exist");
    }
	*/
}

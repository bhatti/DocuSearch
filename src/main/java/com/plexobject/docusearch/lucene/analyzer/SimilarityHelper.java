package com.plexobject.docusearch.lucene.analyzer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.plexobject.docusearch.lucene.LuceneUtils;

public class SimilarityHelper {
    static final String TRAINING_SPELL_CHECKER_EXT = ".tsc";
    static final String COMPILED_SPELLING_FILE_EXT = ".csc";
    private static final Logger LOGGER = Logger
            .getLogger(SimilarityHelper.class);
    final static int NGRAM_LENGTH = 7;
    // private final static double MATCH_WEIGHT = 0.0;
    // private final static double DELETE_WEIGHT = -5.0;
    // private final static double INSERT_WEIGHT = -5.0;
    // private final static double SUBSTITUTE_WEIGHT = -5.0;
    // private final static double TRANSPOSE_WEIGHT = -5.0;
    // private final static boolean checkUncompileSpellings = false;
    private volatile Map<String, SpellChecker> scMap = new TreeMap<String, SpellChecker>();
    // private volatile Map<String, CompiledSpellChecker> cscMap = new
    // TreeMap<String, CompiledSpellChecker>();
    // private volatile Map<String, TrainSpellChecker> tscMap = new
    // TreeMap<String, TrainSpellChecker>();
    private volatile Map<String, TreeSet<String>> tokenSets = new TreeMap<String, TreeSet<String>>();

    private static final SimilarityHelper INSTANCE = new SimilarityHelper();
    private static boolean disableDidYouMean = true;

    public static SimilarityHelper getInstance() {
        return INSTANCE;
    }

    public String didYouMean(final String index, final String contents) {

        String similar = null;
        // if (!disableDidYouMean) {
        // try {
        // similar = getCompiledSpellChecker(index).didYouMean(contents);
        // } catch (FileNotFoundException e) {
        // LOGGER.warn("Failed to find compiled spellings for " + index
        // + " to check against '" + contents + "'--" + e);
        // } catch (IOException e) {
        // LOGGER.warn("Failed to get didYouMean " + index + " -- "
        // + contents, e);
        // }
        // try {
        // if (checkUncompileSpellings && similar == null) {
        // final String[] matched = getSpellChecker(index)
        // .suggestSimilar(contents, 1);
        // if (matched.length > 0) {
        // similar = matched[0];
        // }
        // }
        // } catch (FileNotFoundException e) {
        // LOGGER.warn("Failed to find spellings for " + index
        // + " to check against '" + contents + "'--" + e);
        // } catch (IOException e) {
        // LOGGER.warn("Failed to get didYouMean " + index + " -- "
        // + contents, e);
        // }
        // }
        if (contents.equals(similar)) {
            similar = null;
        }
        return similar;
    }

    @SuppressWarnings("unused")
    private List<String> prefixMatch(final String index, final String prefix,
            int max) {
        List<String> match = new ArrayList<String>();
        if (!disableDidYouMean) {
            try {
                final TreeSet<String> tokens = getCompiledTokenSet(index);
                Set<String> tailSet = tokens.tailSet(prefix);
                for (String tail : tailSet) {
                    if (tail.startsWith(prefix)) {
                        match.add(tail);
                        if (match.size() == max) {
                            break;
                        }
                    }
                }
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("prefixMatch '" + prefix + "' found "
                            + tokens.size() + ", tailset " + tailSet.size()
                            + ", match " + match);
                }
            } catch (FileNotFoundException e) {
                LOGGER.warn("Failed to find prefixMatch for " + index
                        + " to check against '" + prefix + "'--" + e);
            } catch (IOException e) {
                LOGGER.warn("Failed to get prefixMatch " + index + " -- "
                        + prefix, e);
            }
        }
        return match;
    }

    public void trainSpellChecker(final String index, final String contents) {
        // try {
        // getTrainSpellChecker(index).train(contents);
        // } catch (FileNotFoundException e) {
        // LOGGER.warn("Spell checker file not found for  " + index + " -- "
        // + contents);
        // } catch (IOException e) {
        // LOGGER.warn("Failed to traing " + index + " -- " + contents, e);
        // }
    }

    public void saveTrainingSpellChecker(final String index) throws IOException {
        // saveCompiledSpellChecker(getTrainSpellChecker(index), new File(
        // LuceneUtils.INDEX_DIR, index + TRAINING_SPELL_CHECKER_EXT),
        // new File(LuceneUtils.INDEX_DIR, index
        // + COMPILED_SPELLING_FILE_EXT));
        // scMap.remove(index);
        // cscMap.remove(index);
        // tscMap.remove(index);
        // tokenSets.remove(index);
    }

    // PRIVATE METHODS
    @SuppressWarnings("unused")
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

    private TreeSet<String> getCompiledTokenSet(final String index)
            throws IOException {
        synchronized (index) {
            TreeSet<String> set = tokenSets.get(index);
            // if (set == null) {
            // Set<String> tokenSet = getCompiledSpellChecker(index)
            // .tokenSet();
            // if (tokenSet instanceof TreeSet) {
            // set = (TreeSet<String>) tokenSet;
            // } else {
            // set = new TreeSet<String>(tokenSet);
            // }
            // tokenSets.put(index, set);
            // }
            return set;
        }
    }

    // private CompiledSpellChecker getCompiledSpellChecker(final String index)
    // throws IOException {
    // CompiledSpellChecker csc = cscMap.get(index);
    // if (csc == null) {
    // synchronized (cscMap) {
    // if (csc == null) {
    // csc = getCompiledSpellChecker(new File(
    // LuceneUtils.INDEX_DIR, index
    // + COMPILED_SPELLING_FILE_EXT));
    // cscMap.put(index, csc);
    // }
    // }
    // }
    // return csc;
    // }
    //
    // private CompiledSpellChecker getCompiledSpellChecker(final File file)
    // throws IOException {
    // if (file.exists()) {
    // InputStream in = null;
    // try {
    // in = new FileInputStream(file);
    // final ObjectInputStream objIn = new ObjectInputStream(
    // new BufferedInputStream(in));
    // return (CompiledSpellChecker) objIn.readObject();
    // } catch (OutOfMemoryError e) {
    // LOGGER.error(
    // "Ran out of memory while initializing CompiledSpellChecker, disabling it "
    // + file, e);
    // disableDidYouMean = true;
    // throw new IOException(
    // "Ran out of memory while initializing CompiledSpellChecker "
    // + file + ", free "
    // + (Runtime.getRuntime().freeMemory() / 1024)
    // + ", max "
    // + (Runtime.getRuntime().totalMemory() / 1024));
    //
    // } catch (Throwable e) {
    // LOGGER.error("Failed to initialize CompiledSpellChecker "
    // + file, e);
    // throw new IOException(
    // "Failed to initialize CompiledSpellChecker " + file, e);
    // } finally {
    // if (in != null) {
    // try {
    // in.close();
    // } catch (IOException e) {
    // }
    // }
    // }
    // }
    // throw new FileNotFoundException(file + " does not exist");
    // }
    //
    // private TrainSpellChecker getTrainSpellChecker(final String index)
    // throws IOException {
    // TrainSpellChecker tsc = tscMap.get(index);
    // if (tsc == null) {
    // synchronized (tscMap) {
    // if (tsc == null) {
    // tsc = getTrainSpellChecker(new File(LuceneUtils.INDEX_DIR,
    // index + TRAINING_SPELL_CHECKER_EXT));
    // tscMap.put(index, tsc);
    // }
    // }
    // }
    // return tsc;
    // }
    //
    // private TrainSpellChecker getTrainSpellChecker(final File file)
    // throws IOException {
    // if (file.exists()) {
    // InputStream in = null;
    // try {
    // in = new FileInputStream(file);
    // final ObjectInputStream objIn = new ObjectInputStream(
    // new BufferedInputStream(in));
    // return (TrainSpellChecker) objIn.readObject();
    // } catch (Throwable e) {
    // LOGGER.error("Failed to initialize CompiledSpellChecker "
    // + file, e);
    // return getDefaultTrainSpellChecker();
    // } finally {
    // if (in != null) {
    // try {
    // in.close();
    // } catch (IOException e) {
    // }
    // }
    // }
    // } else {
    // return getDefaultTrainSpellChecker();
    // }
    // }
    //
    // private TrainSpellChecker getDefaultTrainSpellChecker() {
    // FixedWeightEditDistance fixedEdit = new FixedWeightEditDistance(
    // MATCH_WEIGHT, DELETE_WEIGHT, INSERT_WEIGHT, SUBSTITUTE_WEIGHT,
    // TRANSPOSE_WEIGHT);
    // NGramProcessLM lm = new NGramProcessLM(NGRAM_LENGTH);
    // TokenizerFactory tokenizerFactory = new StandardBgramTokenizerFactory();
    // return new TrainSpellChecker(lm, fixedEdit, tokenizerFactory);
    // }
    //
    // private void saveCompiledSpellChecker(final TrainSpellChecker tsc,
    // final File tscFile, final File cscFile) throws IOException {
    // saveCompiledSpellChecker(tsc, tscFile);
    //
    // final ObjectOutputStream objOut = new ObjectOutputStream(
    // new BufferedOutputStream(new FileOutputStream(cscFile)));
    // tsc.compileTo(objOut);
    // objOut.close();
    // }
    //
    // private void saveCompiledSpellChecker(final TrainSpellChecker tsc,
    // final File tscFile) throws IOException, FileNotFoundException {
    // tsc.pruneTokens(NGRAM_LENGTH);
    //
    // ObjectOutputStream objOut = new ObjectOutputStream(
    // new BufferedOutputStream(new FileOutputStream(tscFile)));
    // objOut.writeObject(tsc);
    // objOut.close();
    // }

    private SpellChecker getSpellChecker(final File file) throws IOException {
        if (file.exists()) {
            Directory spellDir = FSDirectory.open(file);
            return new SpellChecker(spellDir);
        }
        throw new FileNotFoundException(file + " does not exist");
    }

    SimilarityHelper() {

    }
}

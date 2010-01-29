package com.plexobject.docusearch.lucene.analyzer;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.lucene.LuceneUtils;

public class SimilarityHelperTest {
    private static final String INDEX = "delete_me";
    private static final String TEXT = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent augue augue, tempor a placerat vitae, rutrum ut felis. "
            + "Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Quisque aliquet orci a urna porttitor iaculis eget nec lorem. "
            + "In porttitor tempus tortor, ac convallis tortor ultrices in. Duis accumsan iaculis leo et convallis. Nulla sit amet faucibus metus. Nullam neque ligula, "
            + "tincidunt et scelerisque vitae, dictum sit amet dolor. Nulla euismod erat eget metus facilisis at tristique dolor gravida. Donec consectetur rhoncus justo. "
            + "Nunc ultrices sapien eget nisi laoreet lobortis. Cras feugiat varius feugiat. Vivamus tempus, ipsum eu ornare vehicula, quam justo vulputate erat, "
            + "sit amet adipiscing nisi massa quis massa. Donec laoreet ultricies quam ut euismod. Etiam at risus sit amet felis aliquet imperdiet eu a dolor. "
            + "Vivamus ultricies mollis augue a adipiscing. Integer vel turpis orci. Fusce adipiscing, massa vel hendrerit consequat, odio elit egestas quam, "
            + "rutrum fermentum eros dolor nec diam. Suspendisse suscipit euismod ultrices.";
    private static final String[] WORDS = TEXT.split(" ");

    @Before
    public void setUp() throws Exception {
        new File(LuceneUtils.INDEX_DIR, INDEX
                + SimilarityHelper.COMPILED_SPELLING_FILE_EXT).delete();
        new File(LuceneUtils.INDEX_DIR, INDEX
                + SimilarityHelper.TRAINING_SPELL_CHECKER_EXT).delete();
    }

    @After
    public void tearDown() throws Exception {
        new File(LuceneUtils.INDEX_DIR, INDEX
                + SimilarityHelper.COMPILED_SPELLING_FILE_EXT).delete();
        new File(LuceneUtils.INDEX_DIR, INDEX
                + SimilarityHelper.TRAINING_SPELL_CHECKER_EXT).delete();
    }

    @Test
    public final void trainAndVerify() throws IOException {
        try {
            final SimilarityHelper helper = new SimilarityHelper();
            // TODO fix it
            for (int i = 0; i < SimilarityHelper.NGRAM_LENGTH; i++) {
                for (String word : WORDS) {
                    word = word.replace('.', ' ').toLowerCase().trim();
                    helper.trainSpellChecker(INDEX, word);
                }
            }
            helper.saveTrainingSpellChecker(INDEX);

            for (String word : WORDS) {
                if (word.length() > 3) {
                    String prefix = word.substring(0, 3).replace('.', ' ')
                            .toLowerCase().trim();
                    helper.didYouMean(INDEX, prefix);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

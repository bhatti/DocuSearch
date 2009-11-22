package com.plexobject.docusearch.util;

import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TrieTest {
    private static final String TEXT = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent augue augue, tempor a placerat vitae, rutrum ut felis. "
            + "Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Quisque aliquet orci a urna porttitor iaculis eget nec lorem. "
            + "In porttitor tempus tortor, ac convallis tortor ultrices in. Duis accumsan iaculis leo et convallis. Nulla sit amet faucibus metus. Nullam neque ligula, "
            + "tincidunt et scelerisque vitae, dictum sit amet dolor. Nulla euismod erat eget metus facilisis at tristique dolor gravida. Donec consectetur rhoncus justo. "
            + "Nunc ultrices sapien eget nisi laoreet lobortis. Cras feugiat varius feugiat. Vivamus tempus, ipsum eu ornare vehicula, quam justo vulputate erat, "
            + "sit amet adipiscing nisi massa quis massa. Donec laoreet ultricies quam ut euismod. Etiam at risus sit amet felis aliquet imperdiet eu a dolor. "
            + "Vivamus ultricies mollis augue a adipiscing. Integer vel turpis orci. Fusce adipiscing, massa vel hendrerit consequat, odio elit egestas quam, "
            + "rutrum fermentum eros dolor nec diam. Suspendisse suscipit euismod ultrices.";
    private static final String[] LINES = TEXT.split(".");
    private static final String[] WORDS = TEXT.split(" ");

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCompletion() {
        Trie trie = new Trie();
        for (String line : LINES) { // contains filenames from ~/tmp/
            trie.load(line);
        }
        for (String word : WORDS) {
            String prefix = word.length() > 2 ? word.substring(0,
                    word.length() - 2) : word;
            Assert.assertFalse("Could not find matches for " + prefix, trie
                    .matchPrefix(prefix));

            List<String> completions = trie.findCompletions(prefix);
            Assert.assertFalse("Could not find matches for " + prefix,
                    completions.size() > 0);
        }
    }
}

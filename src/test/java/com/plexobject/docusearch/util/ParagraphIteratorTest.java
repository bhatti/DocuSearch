package com.plexobject.docusearch.util;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ParagraphIteratorTest {
    private static final String TEXT = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent augue augue, tempor a placerat vitae, rutrum ut felis. "
            + "Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Quisque aliquet orci a urna porttitor iaculis eget nec lorem. "
            + "In porttitor tempus tortor, ac convallis tortor ultrices in. Duis accumsan iaculis leo et convallis. Nulla sit amet faucibus metus. Nullam neque ligula, "
            + "tincidunt et scelerisque vitae, dictum sit amet dolor. Nulla euismod erat eget metus facilisis at tristique dolor gravida. Donec consectetur rhoncus justo. "
            + "Nunc ultrices sapien eget nisi laoreet lobortis. Cras feugiat varius feugiat. Vivamus tempus, ipsum eu ornare vehicula, quam justo vulputate erat, "
            + "sit amet adipiscing nisi massa quis massa. Donec laoreet ultricies quam ut euismod. Etiam at risus sit amet felis aliquet imperdiet eu a dolor. "
            + "Vivamus ultricies mollis augue a adipiscing. Integer vel turpis orci. Fusce adipiscing, massa vel hendrerit consequat, odio elit egestas quam, "
            + "rutrum fermentum eros dolor nec diam. Suspendisse suscipit euismod ultrices.";

    private ParagraphIterator iterator;

    @Before
    public void setUp() throws Exception {
        iterator = new ParagraphIterator(TEXT);
    }

    @After
    public void tearDown() throws Exception {
    }

    // TODO
    // @Test
    public final void testHasIterator() {
        int total = 0;
        while (iterator.hasNext()) {
            iterator.next();
            total++;
        }
        Assert.assertEquals(19, total);
    }

    @Test(expected = UnsupportedOperationException.class)
    public final void testRemove() {
        while (iterator.hasNext()) {
            iterator.remove();
        }
    }

}

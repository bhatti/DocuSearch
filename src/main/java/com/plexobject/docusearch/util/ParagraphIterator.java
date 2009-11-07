package com.plexobject.docusearch.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.text.BreakIterator;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import com.ibm.icu.text.RuleBasedBreakIterator;

public class ParagraphIterator implements Iterator<String> {
    private final RuleBasedBreakIterator breakIterator;
    private final String text;
    private int index;

    public ParagraphIterator(final File file) throws IOException {
        this(FileUtils.readFileToString(file));
    }

    public ParagraphIterator(final Reader reader) throws IOException {
        this(IOUtils.toString(reader));
    }

    public ParagraphIterator(final String text) throws IOException {
        this.text = text;
        final InputStream in = getClass().getClassLoader().getResourceAsStream(
                "paragraph_break_rules.txt");
        if (in == null) {
            throw new IOException("Failed to find paragraph_break_rules.txt");
        }
        this.breakIterator = new RuleBasedBreakIterator(IOUtils.toString(in));
        this.breakIterator.setText(text);
    }

    @Override
    public boolean hasNext() {
        return index < text.length();
    }

    @Override
    public String next() {
        int end = breakIterator.next();
        if (end == BreakIterator.DONE) {
            return null;
        }
        String sentence = text.substring(index, end);
        index = end;
        return sentence;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}

package com.plexobject.docusearch.util;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.text.BreakIterator;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

public class SentenceIterator implements Iterator<String> {
    private final BreakIterator breakIterator;
    private final String text;
    private int index;

    public SentenceIterator(final File file) throws IOException {
        this(FileUtils.readFileToString(file));
    }

    public SentenceIterator(final Reader reader) throws IOException {
        this(IOUtils.toString(reader));
    }

    public SentenceIterator(final String text) throws IOException {
        this.text = text;
        this.breakIterator = BreakIterator.getSentenceInstance();
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

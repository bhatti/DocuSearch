package com.plexobject.docusearch.util;

import java.io.IOException;

import com.plexobject.docusearch.domain.Tuple;

public class StringUtils {
    private static final int MIN_PARAGRAPH_LENGTH = 512;
    private static final int MIN_SENTENCE_LENGTH = 128;

    public static Tuple splitSentenceParagraphAndRest(final String allContents)
            throws IOException {
        final SentenceIterator sentenceIterator = new SentenceIterator(
                allContents);

        final StringBuilder firstParagraph = new StringBuilder();
        final StringBuilder firstSentence = new StringBuilder();

        while (firstParagraph.length() < MIN_PARAGRAPH_LENGTH) {
            if (firstSentence.length() < MIN_SENTENCE_LENGTH) {
                firstSentence.append(sentenceIterator.next() + "\n");
            }
            firstParagraph.append(sentenceIterator.next() + "\n");
        }

        final StringBuilder allButFirstParagraph = new StringBuilder();
        while (sentenceIterator.hasNext()) {
            allButFirstParagraph.append(sentenceIterator.next());
        }
        return new Tuple(firstSentence.toString(), firstParagraph.toString(),
                allButFirstParagraph.toString());
    }

    public static Tuple splitSentenceAndRest(final String allContents)
            throws IOException {
        final SentenceIterator sentenceIterator = new SentenceIterator(
                allContents);

        final StringBuilder firstSentence = new StringBuilder();

        while (firstSentence.length() < MIN_SENTENCE_LENGTH) {
            firstSentence.append(sentenceIterator.next() + "\n");
        }

        final StringBuilder allButFirstSentence = new StringBuilder();
        while (sentenceIterator.hasNext()) {
            allButFirstSentence.append(sentenceIterator.next());
        }
        return new Tuple(firstSentence.toString(), allButFirstSentence
                .toString());
    }

}

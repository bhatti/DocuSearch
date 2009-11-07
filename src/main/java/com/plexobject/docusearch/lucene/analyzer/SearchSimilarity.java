package com.plexobject.docusearch.lucene.analyzer;

import org.apache.lucene.search.Similarity;

public class SearchSimilarity extends Similarity {
    private static final long serialVersionUID = 1L;

    public SearchSimilarity() {
    }

    public float lengthNorm(String fieldName, int numTerms) {
        return (float) (1.0 / Math.sqrt(numTerms));
        // return (1.0f); // ignore length normalization
    }

    public float queryNorm(float sumOfSquaredWeights) {
        return (float) (1.0 / Math.sqrt(sumOfSquaredWeights));
    }

    public float tf(float freq) {
        return (float) Math.sqrt(freq);
    }

    public float sloppyFreq(int distance) {
        return 1.0f / (distance + 1);
    }

    public float idf(int docFreq, int numDocs) {
        return (float) (Math.log(numDocs / (double) (docFreq + 1)) + 1.0);
    }

    public float coord(int overlap, int maxOverlap) {
        return overlap / (float) maxOverlap;
    }

}

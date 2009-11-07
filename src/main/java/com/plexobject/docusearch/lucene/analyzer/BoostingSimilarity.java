package com.plexobject.docusearch.lucene.analyzer;

import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.analysis.payloads.PayloadHelper;

public class BoostingSimilarity extends DefaultSimilarity {
    private static final long serialVersionUID = 1L;

    public float scorePayload(String fieldName, byte[] payload, int offset,
            int length) {
        if (payload != null) {
            return PayloadHelper.decodeFloat(payload, offset);
        } else {
            return 1.0F;
        }
    }
}

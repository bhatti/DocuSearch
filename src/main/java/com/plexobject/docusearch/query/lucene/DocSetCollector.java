package com.plexobject.docusearch.query.lucene;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.index.IndexReader;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SortedIntDocSet;

import java.io.IOException;

class DocSetCollector extends Collector {
    private final int maxDoc;
    private final int smallSetSize;
    private int base;
    private int numHits;
    private OpenBitSet bits;
    private float topscore = Float.NEGATIVE_INFINITY;
    private Scorer scorer;

    // in case there aren't that many hits, we may not want a very sparse
    // bit array. Optimistically collect the first few docs in an array
    // in case there are only a few.
    final int[] scratch;

    DocSetCollector(final int maxDoc) {
        this(maxDoc >> 6, maxDoc);
    }

    DocSetCollector(final int smallSetSize, final int maxDoc) {
        this.smallSetSize = smallSetSize;
        this.maxDoc = maxDoc;
        this.scratch = new int[smallSetSize];
    }

    public void collect(int doc) throws IOException {
        doc += base;

        if (numHits < scratch.length) {
            scratch[numHits] = doc;
        } else {
            // this conditional could be removed if BitSet was preallocated, but
            // that
            // would take up more memory, and add more GC time...
            if (bits == null) {
                bits = new OpenBitSet(maxDoc);
            }
            bits.fastSet(doc);
        }
        topscore = Math.max(scorer.score(), topscore);

        numHits++;
    }

    public float getTopScore() {
        return topscore;
    }

    public int getNumHits() {
        return numHits;
    }

    public DocSet getDocSet() {
        if (numHits <= scratch.length) {
            // assumes docs were collected in sorted order!
            return new SortedIntDocSet(scratch, numHits);
        } else {
            // set the bits for ids that were collected in the array
            for (int i = 0; i < scratch.length; i++)
                bits.fastSet(scratch[i]);
            return new BitDocSet(bits, numHits);
        }
    }

    public void setScorer(final Scorer scorer) throws IOException {
        this.scorer = scorer;
    }

    public Scorer getScorer() {
        return scorer;
    }

    public void setNextReader(IndexReader reader, int docBase)
            throws IOException {
        this.base = docBase;
    }

    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    public int getSmallSetSize() {
        return smallSetSize;
    }
}

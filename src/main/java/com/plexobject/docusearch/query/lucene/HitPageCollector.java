package com.plexobject.docusearch.query.lucene;

import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.PriorityQueue;

/**
 * 
 * @author Shahzad Bhatti
 * 
 */
@SuppressWarnings("deprecation")
public class HitPageCollector extends HitCollector {
	public static class HitQueue extends PriorityQueue {
		public HitQueue(int size) {
			initialize(size);
		}

		public final boolean lessThan(Object a, Object b) {
			ScoreDoc hitA = (ScoreDoc) a;
			ScoreDoc hitB = (ScoreDoc) b;
			if (hitA.score == hitB.score) {
				return hitA.doc > hitB.doc;
			} else {
				return hitA.score < hitB.score;
			}
		}
	}
	private final int nDocs;
	private final PriorityQueue hq;
	private float minScore;
	private int totalHits;;
	private int totalInThisPage;
	private final int start;
	private final int maxNumHits;

	public HitPageCollector(int start, int maxNumHits) {
		this.nDocs = start + maxNumHits;
		this.start = start;
		this.maxNumHits = maxNumHits;
		this.hq = new HitQueue(nDocs);
	}

	public void collect(int doc, float score) {
		totalHits++;
		if ((hq.size() < nDocs) || (score >= minScore)) {
			ScoreDoc scoreDoc = new ScoreDoc(doc, score);
			hq.insertWithOverflow(scoreDoc); // update hit queue
			minScore = ((ScoreDoc) hq.top()).score; // reset minScore
		}
		totalInThisPage = hq.size();
	}

	public ScoreDoc[] getScores() {
		if (start <= 0) {
			throw new IllegalArgumentException("Invalid start :" + start
					+ " - start should be >=1");
		}
		int numReturned = Math.min(maxNumHits, (hq.size() - (start - 1)));
		if (numReturned <= 0) {
			return new ScoreDoc[0];
		}
		ScoreDoc[] scoreDocs = new ScoreDoc[numReturned];
		ScoreDoc scoreDoc;
		for (int i = hq.size() - 1; i >= 0; i--) { // put docs in array, working
			scoreDoc = (ScoreDoc) hq.pop();
			if (i < (start - 1)) {
				break; // off the beginning of the results array
			}
			if (i < (scoreDocs.length + (start - 1))) {
				scoreDocs[i - (start - 1)] = scoreDoc; // within scope of
			}
		}
		return scoreDocs;
	}

	public int getTotalAvailable() {
		return totalHits;
	}

	public int getStart() {
		return start;
	}

	public int getEnd() {
		return start + totalInThisPage - 1;
	}
}

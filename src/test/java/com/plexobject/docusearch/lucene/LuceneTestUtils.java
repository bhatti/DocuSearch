package com.plexobject.docusearch.lucene;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.junit.Assert;

public class LuceneTestUtils {

	@SuppressWarnings("deprecation")
	public static void assertAnalyzesTo(Analyzer analyzer, String input,
			String[] output) throws Exception {
		TokenStream stream = analyzer.tokenStream("field", new StringReader(
				input));

		if (true) {
			TermAttribute termAttr = (TermAttribute) stream
					.addAttribute(TermAttribute.class);
			for (int i = 0; i < output.length; i++) {
				Assert.assertTrue(stream.incrementToken());
				Assert.assertEquals(output[i], termAttr.term());
			}
			Assert.assertFalse(stream.incrementToken());
		} else {
			Token reusableToken = new Token();
			for (int i = 0; i < output.length; i++) {
				Token t = stream.next(reusableToken);
				Assert.assertTrue(t != null);
				Assert.assertEquals(output[i], t.term());
			}
			Assert.assertTrue(stream.next(reusableToken) == null);
		}
		stream.close();
	}

	public static int hitCount(IndexSearcher searcher, Query query,
			Filter filter) throws IOException {
		return searcher.search(query, filter, 1).totalHits;
	}

	public static int hitCount(IndexSearcher searcher, Query query)
			throws IOException {
		return searcher.search(query, 1).totalHits;
	}

	/***
	 * Return a list of tokens according to a test string format: a b c =>
	 * returns List<Token> [a,b,c] a/b => tokens a and b share the same spot
	 * (b.positionIncrement=0) a,3/b/c => a,b,c all share same position
	 * (a.positionIncrement=3, b.positionIncrement=0, c.positionIncrement=0)
	 * a,1,10,11 => "a" with positionIncrement=1, startOffset=10, endOffset=11
	 */
	public static List<Token> tokens(String str) {
		String[] arr = str.split(" ");
		List<Token> result = new ArrayList<Token>();
		for (int i = 0; i < arr.length; i++) {
			String[] toks = arr[i].split("/");
			String[] params = toks[0].split(",");

			int posInc;
			int start;
			int end;

			if (params.length > 1) {
				posInc = Integer.parseInt(params[1]);
			} else {
				posInc = 1;
			}

			if (params.length > 2) {
				start = Integer.parseInt(params[2]);
			} else {
				start = 0;
			}

			if (params.length > 3) {
				end = Integer.parseInt(params[3]);
			} else {
				end = start + params[0].length();
			}

			Token t = new Token(params[0].toCharArray(), 0, params[0].length(),
					start, end);
			t.setType("TEST");
			t.setPositionIncrement(posInc);

			result.add(t);
			for (int j = 1; j < toks.length; j++) {
				t = new Token(toks[j].toCharArray(), 0, toks[j].length(), 0, 0);
				t.setType("TEST");
				t.setPositionIncrement(0);
				result.add(t);
			}
		}
		return result;
	}

}

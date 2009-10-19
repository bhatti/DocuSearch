package com.plexobject.docusearch.lucene.analyzer;

import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;

/**
 * Returns the stems of tokens using the Porter stemmer
 * 
 * @author bhatti@plexobject.com
 */
public class PorterAnalyzer extends Analyzer {
	public TokenStream tokenStream(String fieldName, Reader reader) {
		return (new PorterStemFilter(new StopFilter(true,
				new LowerCaseTokenizer(reader),
				StopAnalyzer.ENGLISH_STOP_WORDS_SET)));
	}

}

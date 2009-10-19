package com.plexobject.docusearch.lucene.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;
import java.io.Reader;

import org.apache.solr.analysis.SynonymMap;
import org.apache.solr.analysis.SynonymFilter;

public class SynonymAnalyzer extends Analyzer {
	private final SynonymMap synonymMap;

	public SynonymAnalyzer(final SynonymMap synonymMap) {
		if (synonymMap == null) {
			throw new NullPointerException("synonymMap is not specified");
		}
		this.synonymMap = synonymMap;
	}

	public TokenStream tokenStream(String fieldName, Reader reader) {
		TokenStream result = new SynonymFilter(new StopFilter(true,
				new LowerCaseFilter(new StandardFilter(new StandardTokenizer(
						reader))), StandardAnalyzer.STOP_WORDS_SET), synonymMap);
		return result;
	}
}

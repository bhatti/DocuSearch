package com.plexobject.docusearch.lucene.analyzer;

import java.io.Reader;
import org.apache.lucene.analysis.ASCIIFoldingFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

/**
 * Filters the diacritic characters
 * 
 */
public class DiacriticAnalyzer extends StandardAnalyzer {
	public DiacriticAnalyzer() {
		super(Version.LUCENE_CURRENT);
	}

	public TokenStream tokenStream(String fieldName, Reader reader) {
		return new ASCIIFoldingFilter(super.tokenStream(fieldName, reader));
	}

}

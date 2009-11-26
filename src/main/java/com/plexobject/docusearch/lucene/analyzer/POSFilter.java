package com.plexobject.docusearch.lucene.analyzer;

import java.util.Arrays;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;

/**
 * Part of speech filter for Lucene POS Analyzer
 */
public class POSFilter extends TokenFilter {

	private Token[] tokens = null;
	private int currentToken = 0;

	public POSFilter(final TokenStream in, final Token[] tokens) {
		super(in);
		this.tokens = Arrays.copyOf(tokens, tokens.length);
	}

	public final Token next() {
		return (currentToken < tokens.length) ? tokens[currentToken++] : null;
	}

	/**
	 * @see java.lang.Object#equals(Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof POSFilter)) {
			return false;
		}
		if (!super.equals(object)) {
			return false;
		}
		POSFilter rhs = (POSFilter) object;
		return new EqualsBuilder().append(this.tokens, rhs.tokens).append(
				this.currentToken, rhs.currentToken).isEquals();
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return new HashCodeBuilder(786529047, 1924536713).append(this)
				.toHashCode();
	}
}

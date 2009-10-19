package com.plexobject.docusearch.lucene.analyzer;

import java.io.IOException;

import org.apache.commons.codec.language.Metaphone;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

public class MetaphoneReplacementFilter extends TokenFilter {
	public static final String METAPHONE = "METAPHONE";

	private Metaphone metaphoner = new Metaphone(); // #1
	private TermAttribute termAttr;
	private TypeAttribute typeAttr;

	public MetaphoneReplacementFilter(TokenStream input) {
		super(input);
		termAttr = (TermAttribute) addAttribute(TermAttribute.class);
		typeAttr = (TypeAttribute) addAttribute(TypeAttribute.class);
	}

	public boolean incrementToken() throws IOException {
		if (!input.incrementToken()) // Advance to next token
			return false; // When false, end has been reached

		String encoded;
		encoded = metaphoner.encode(termAttr.term()); // Convert term text to
		// Metaphone encoding
		termAttr.setTermBuffer(encoded); // Overwrite term text with encoded
		// text
		typeAttr.setType(METAPHONE); // Set token type
		return true;
	}

	/**
	 * @see java.lang.Object#equals(Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof MetaphoneReplacementFilter)) {
			return false;
		}
		if (!super.equals(object)) {
			return false;
		}
		MetaphoneReplacementFilter rhs = (MetaphoneReplacementFilter) object;
		return new EqualsBuilder().append(this.metaphoner, rhs.metaphoner)
				.append(this.termAttr, rhs.termAttr).append(this.typeAttr,
						rhs.typeAttr).isEquals();
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return new HashCodeBuilder(786529047, 1924536713).append(metaphoner)
				.toHashCode();
	}
}

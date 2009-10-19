package com.plexobject.docusearch.query;

import java.util.Map;

import com.plexobject.docusearch.domain.Document;

/**
 * This class represents document stored in the document-based database.
 * 
 * @author bhatti@plexobject.com
 */
public class SearchDoc extends Document {
	public static final String SCORE = "score";
	public static final String DOC = "doc";

	public SearchDoc(final Map<String, Object> properties) {
		super(properties);
	}

	/**
	 * @return - score of the document
	 */
	public double getScore() {
		return getDouble(SCORE, 0);
	}

	/**
	 * @return - hit document number
	 */
	public int getHitDocumentNumber() {
		return getInteger(DOC, 0);
	}

}

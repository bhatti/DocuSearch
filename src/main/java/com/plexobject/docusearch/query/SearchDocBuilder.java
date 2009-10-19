package com.plexobject.docusearch.query;

import java.util.Map;

import com.plexobject.docusearch.domain.DocumentBuilder;

public class SearchDocBuilder extends DocumentBuilder {

	public SearchDocBuilder(String database) {
		super(database);
	}

	public SearchDocBuilder() {
	}

	public SearchDocBuilder setScore(final float score) {
		put(SearchDoc.SCORE, String.valueOf(score));
		return this;
	}

	public SearchDocBuilder seHitDocumentNumber(final int doc) {
		put(SearchDoc.DOC, String.valueOf(doc));
		return this;
	}

	@Override
	public SearchDocBuilder put(final String name, final Object value) {
		super.put(name, value);
		return this;
	}

	@Override
	public SearchDocBuilder putAll(
			Map<? extends String, ? extends Object> properties) {
		super.putAll(properties);
		return this;
	}

	@Override
	public SearchDoc build() {
		return new SearchDoc(properties);
	}
}
